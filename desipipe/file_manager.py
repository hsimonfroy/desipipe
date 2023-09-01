import os
import glob
import re
import copy
import shutil
import itertools
import tempfile

import yaml

from . import utils
from .utils import BaseClass
from .environment import get_environ
from .io import get_filetype


class YamlLoader(yaml.SafeLoader):
    """
    *yaml* loader that correctly parses numbers.
    Taken from https://stackoverflow.com/questions/30458977/yaml-loads-5e-6-as-string-and-not-a-number.
    """


# https://stackoverflow.com/questions/30458977/yaml-loads-5e-6-as-string-and-not-a-number
YamlLoader.add_implicit_resolver(u'tag:yaml.org,2002:float',
                                 re.compile(u'''^(?:
                                 [-+]?(?:[0-9][0-9_]*)\\.[0-9_]*(?:[eE][-+]?[0-9]+)?
                                 |[-+]?(?:[0-9][0-9_]*)(?:[eE][-+]?[0-9]+)
                                 |\\.[0-9_]+(?:[eE][-+][0-9]+)?
                                 |[-+]?[0-9][0-9_]*(?::[0-5]?[0-9])+\\.[0-9_]*
                                 |[-+]?\\.(?:inf|Inf|INF)
                                 |\\.(?:nan|NaN|NAN))$''', re.X),
                                 list(u'-+0123456789.'))

YamlLoader.add_implicit_resolver('!none', re.compile('None$'), first='None')


def none_constructor(loader, node):
    return None


YamlLoader.add_constructor('!none', none_constructor)


def yaml_parser(string):
    """Parse string in *yaml* format."""
    return list(yaml.load_all(string, Loader=YamlLoader))


class BaseMutableClass(BaseClass):
    """Base mutable class, merely used to factorize code of :class:`FileEntry` and :class:`BaseFile`."""
    _defaults = dict()

    def __init__(self, *args, **kwargs):
        """
        Initialize :class:`BaseMutableClass`.

        Parameters
        ----------
        *args : dict, :class:`BaseMutableClass`
            Dictionary of (some) attributes (see above) or :class:`BaseMutableClass` instance.

        **kwargs : dict
            Optionally, more attributes.
        """
        if len(args) > 1:
            raise ValueError('Cannot take several args')
        if len(args):
            if isinstance(args[0], self.__class__):
                self.__dict__.update(args[0].__dict__)
                return
            kwargs = {**args[0], **kwargs}
        for name, value in self._defaults.items():
            setattr(self, name, copy.copy(value))
        self.update(**kwargs)

    def clone(self, **kwargs):
        """Return an updated copy."""
        new = self.copy()
        new.update(**kwargs)
        return new

    def update(self, **kwargs):
        """Update input attributes."""
        for name, value in kwargs.items():
            if name in self._defaults:
                setattr(self, name, type(self._defaults[name])(value))
            else:
                raise ValueError('Unknown argument {}; supports {}'.format(name, list(self._defaults)))

    def to_dict(self):
        """View as a dictionary (of attributes)."""
        return {name: getattr(self, name) for name in self._defaults}


def _make_list(values):
    """Turn input ``values`` (single value, list, iterator, etc.) into a list."""
    if isinstance(values, range):
        return values
    if not hasattr(values, '__iter__') or isinstance(values, str):
        values = [values]
    return list(values)


def _make_list_options(values):
    if values is Ellipsis:
        return values
    if not isinstance(values, (list, range)):
        return [values]
    return values


def in_options(values, options, return_index=False):
    """Return input values that are in options."""
    if not options:
        return []
    if values is Ellipsis:
        values = options
    if options is Ellipsis:
        toret = values
        if get_ndim(toret) == 0:
            toret = [toret]
        if return_index:
            return toret, Ellipsis
        return toret

    def get_ndim(opt):
        if hasattr(opt, '__iter__') and not isinstance(opt, str):
            for opt in opt:
                return 1 + get_ndim(opt)
        return 0

    ndim_options = get_ndim(options)
    ndim_values = get_ndim(values)
    if ndim_values == ndim_options - 1:
        values = [values]
    elif ndim_values != ndim_options:
        raise ValueError('Cannot match values {} with options {}'.format(values, options))
    toret, index = [], []
    options = list(options)
    for value in values:
        if type(value) is not type(options[0]):
            value = type(options[0])(value)
        if value in options:
            ii = options.index(value)
            toret.append(options[ii])
            index.append(ii)
    if return_index:
        return toret, index
    return toret


def iter_options(options):
    for values in itertools.product(*options.values()):
        yield {name: [values[iname]] for iname, name in enumerate(options)}


class FileEntry(BaseMutableClass):
    """
    Class describing a file entry.

    Attributes
    ----------
    filetype : str, default=''
        File type, e.g. 'catalog', 'power', etc.

    path : str, default=''
        Path to file(s). May contain placeholders, e.g. 'data_{tracer}_{region}.fits',
        with the list of values that ``tracer``, ``region`` may take specified in ``options`` (see below).

    id : str, default=''
        File (unique) identifier.

    author : str, default=''
        Who produced this file.

    options : dict, default=dict()
        Dictionary matching placeholders in ``path`` to the list of values they can take, e.g. {'region': ['NGC', 'SGC']}

    description : str, default=''
        Plain text describing the file(s).
    """

    _defaults = dict(filetype='', path='', id='', author='', options=dict(), foptions=dict(), description='')

    def update(self, **kwargs):
        """Update input attributes (options values are turned into lists)."""
        super(FileEntry, self).update(**kwargs)

        if 'options' in kwargs:
            options, foptions = {}, {}
            for name, values in kwargs['options'].items():
                if values is None or values is Ellipsis:
                    options[name] = Ellipsis
                elif isinstance(values, dict):
                    foptions[name] = list(values.keys())
                    values = list(values.values())
                elif isinstance(values, str) and re.match(r'range\((.*)\)$', values):
                    values = eval(values)
                options[name] = values = _make_list_options(values)
            self.options, self.foptions = options, foptions
        if 'foptions' in kwargs:
            self.foptions = dict(kwargs['foptions'])
        for name, values in self.options.items():
            self.foptions.setdefault(name, values)

    def select(self, **kwargs):
        """
        Restrict to input options, e.g.

        >>> entry.select(region=['NGC'])

        returns a new entry, with option 'region' taking values in ``['NGC']``.
        """
        def eq(test, ref):
            if type(test) is not type(ref):
                if hasattr(test, '__iter__') and hasattr(ref, '__iter__'):
                    return test == type(test)(ref)
            return test == ref

        options, foptions = self.options.copy(), self.foptions.copy()
        for name, values in kwargs.items():
            if name in options:
                options[name], indices = in_options(values, options[name], return_index=True)
                if indices is Ellipsis:  # Ellipsis
                    foptions[name] = options[name]
                else:
                    foptions[name] = [foptions[name][index] for index in indices]
            else:
                raise ValueError('Unknown option {}, select from {}'.format(name, self.options))
        return self.clone(options=options, foptions=foptions)

    def get(self, *args, **kwargs):
        """
        Return the :class:`File` instance that matches input arguments, see :meth:`select`.
        If :meth:`select` returns several file entries, and / or file entries with multiples files,
        a :class:`ValueError` is raised.
        """
        new = self.select(*args, **kwargs)
        if len(new) == 1:
            for fi in new:
                return fi  # File instance
        if len(new) == 0:
            raise ValueError('"get" is not applicable as there are no matching entries')
        else:
            raise ValueError('"get" is not applicable as there are  with multiple options:\n{}'.format(new))

    def __len__(self):
        """Length, i.e. number of individual files (looping over all options) described by this file entry."""
        size = 1
        for values in self.options.values():
            size *= len(values)
        return size

    def __iter__(self):
        """Iterate over all files (looping over all options) described by this file entry."""
        for ivalues in itertools.product(*(range(len(values)) for values in self.options.values())):
            options, foptions = {}, {}
            for iname, name in enumerate(self.options):
                ivalue = ivalues[iname]
                options[name], foptions[name] = self.options[name][ivalue], self.foptions[name][ivalue]
            fi = BaseFile()
            fi.__dict__.update(self.__dict__)
            fi.options, fi.foptions = options, foptions
            yield fi

    @property
    def filepath(self):
        return self.get().filepath

    def read(self, *args, **kwargs):
        return self.get().read(*args, **kwargs)

    def write(self, *args, **kwargs):
        return self.get().write(*args, **kwargs)

    def __repr__(self):
        """String representation: class name and attributes."""
        di = self.to_dict()
        di.pop('foptions', None)
        #return '{}({})'.format(self.__class__.__name__, ', '.join(['{}={}'.format(name, value) for name, value in di.items()]))
        return '{}(\n{}\n)'.format(self.__class__.__name__, ',\n'.join(['{}: {}'.format(name, value) for name, value in di.items()]))


class BaseFile(BaseMutableClass):
    """
    Class describing a single file (single :attr:`options` values).

    Attributes
    ----------
    filetype : str, default=''
        File type, e.g. 'catalog', 'power', etc.

    path : str, default=''
        Path to file(s). May contain placeholders, e.g. 'data_{tracer}_{region}.fits',
        with the list of values that ``tracer``, ``region`` may take specified in ``options`` (see below).

    id : str, default=''
        File (unique) identifier.

    author : str, default=''
        Who produced this file.

    options : dict, default=dict()
        Dictionary matching placeholders in ``path`` to the list of values they can take, e.g. {'region': ['NGC', 'SGC']}

    description : str, default=''
        Plain text describing the file(s).
    """
    _defaults = dict(filetype='', path='', id='', author='', options=dict(), foptions=dict(), description='')

    @property
    def filepath(self):
        """Real path i.e. replacing placeholders in :attr:`path` by their value."""
        path = self.path
        environ = getattr(self, 'environ', {})
        placeholders = re.finditer(r'\$\{.*?\}', path)
        for placeholder in placeholders:
            placeholder = placeholder.group()
            placeholder_nobrackets = placeholder[2:-1]
            if placeholder_nobrackets in environ:
                path = path.replace(placeholder, environ[placeholder_nobrackets])
        return path.format(**self.foptions)

    def read(self, *args, **kwargs):
        """Read file from disk."""
        return get_filetype(filetype=self.filetype, path=self.filepath).read(*args, **kwargs)

    def write(self, *args, **kwargs):
        """
        Write file to disk. First written in a temporary directory, then moved to its final destination.
        To write additional files, a method :attr:`write_attrs`, that should take the path to the directory as input,
        can be added to the current instance.
        """
        write_attrs = getattr(self, 'write_attrs', None)
        filepath = self.filepath
        dirname = os.path.dirname(filepath)
        utils.mkdir(dirname)
        if write_attrs is not None:
            with tempfile.TemporaryDirectory() as tmp_dir:
                new_dir = write_attrs(tmp_dir) or dirname
                shutil.copytree(tmp_dir, new_dir, dirs_exist_ok=True)
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = os.path.join(tmp_dir, os.path.basename(filepath))
            toret = get_filetype(filetype=self.filetype, path=path).write(*args, **kwargs)
            shutil.copytree(tmp_dir, dirname, dirs_exist_ok=True)
            return toret

    def __repr__(self):
        """String representation: class name and attributes."""
        di = self.to_dict()
        di.pop('path')
        di['filepath'] = self.filepath
        di.pop('foptions')
        #return '{}({})'.format(self.__class__.__name__, ', '.join(['{}={}'.format(name, value) for name, value in di.items()]))
        return '{}(\n{}\n)'.format(self.__class__.__name__, ',\n'.join(['{}: {}'.format(name, value) for name, value in di.items()]))


def prod(iterator):
    toret, count = 1, 0
    for item in iterator:
        toret *= item
        count += 1
    if count:
        return toret
    return 0


class FileEntryCollection(BaseClass):

    """A collection of file entries."""

    def __init__(self, data=None, string=None, parser=None, environ=None, **kwargs):
        """
        Initialize :class:`FileEntryCollection`.

        Parameters
        ----------
        data : dict, string, default=None
            Dictionary or path to a data base *yaml* file.

        string : str, default=None
            If not ``None``, *yaml* format string to decode.
            Added on top of ``data``.

        parser : callable, default=None
            Function that parses *yaml* string into a dictionary.
            Used when ``data`` is string, or ``string`` is not ``None``.

        **kwargs : dict
            Arguments for :func:`parser`.
        """
        if isinstance(data, self.__class__):
            self.__dict__.update(data.copy().__dict__)

        self.parser = parser
        if parser is None:
            self.parser = yaml_parser

        self.environ = dict(environ or {})
        self.data = []

        if utils.is_path(data):
            if string is None: string = ''
            with open(data, 'r') as file:
                string += file.read()
        elif data is not None:
            if isinstance(data, (FileEntryCollection, FileManager)):
                data = data.data
            for dd in data:
                self.append(dd)

        if string is not None:
            for dd in self.parser(string, **kwargs):
                self.append(dd)

    def index(self, id=None, filetype=None, keywords=None, **kwargs):
        """
        Return indices for input identifiers, keywords, or options, e.g.

        >>> db.index(keywords=['power cutsky', 'fiber'], options={'tracer': 'ELG'})

        selects the index of data base entries whose description contains 'power' and 'cutsky' or 'fiber',
        and option 'tracer' is 'ELG'.

        Parameters
        ----------
        id : list, str, default=None
            List of file entry identifiers.
            Defaults to all identifiers (no selection).

        filetype : list, str, default=None
            List of file types.
            Defaults to all file types (no selection).

        keywords : list, str, default=None
            List of keywords to search for in the file entry descriptions.
            If a string contains several words, all of them must be in the description
            for the corresponding file entry to be selected.
            If a list of strings is provided, any of the strings must be in the description
            for the corresponding file entry to be selected.
            e.g. ``['power cutsky', 'fiber']`` selects the data base entries whose description contains 'power' and 'cutsky' or 'fiber'.

        **kwargs : dict
            Restrict to these options, see :meth:`FileEntry.select`.

        Returns
        -------
        index : list
            List of indices.
        """
        if id is not None:
            id = _make_list_options(id)
            id = [iid.lower() for iid in id]
        if filetype is not None:
            filetype = _make_list_options(filetype)
        if keywords is not None:
            keywords = _make_list_options(keywords)
            keywords = [keyword.lower().split() for keyword in keywords]
        index = []
        for ientry, entry in enumerate(self.data):
            if id is not None and entry.id.lower() not in id:
                continue
            if filetype is not None and entry.filetype.lower() not in filetype:
                continue
            if keywords is not None:
                description = entry.description.lower()
                if not any(all(kw in description for kw in keyword) for keyword in keywords):
                    continue
            try:
                entry = entry.select(**kwargs)
            except ValueError:
                continue
            if not entry:
                continue
            index.append(ientry)
        return index

    def select(self, id=None, filetype=None, keywords=None, **kwargs):
        """
        Restrict to input identifiers, keywords, or options, e.g.

        >>> db.select(keywords=['power cutsky', 'fiber'], options={'tracer': 'ELG'})

        selects the data base entries whose description contains 'power' and 'cutsky' or 'fiber',
        and option 'tracer' is 'ELG'.

        Parameters
        ----------
        id : list, str, default=None
            List of file entry identifiers.
            Defaults to all identifiers (no selection).

        filetype : list, str, default=None
            List of file types.
            Defaults to all file types (no selection).

        keywords : list, str, default=None
            List of keywords to search for in the file entry descriptions.
            If a string contains several words, all of them must be in the description
            for the corresponding file entry to be selected.
            If a list of strings is provided, any of the strings must be in the description
            for the corresponding file entry to be selected.
            e.g. ``['power cutsky', 'fiber']`` selects the data base entries whose description contains 'power' and 'cutsky' or 'fiber'.

        **kwargs : dict
            Restrict to these options, see :meth:`FileEntry.select`.

        Returns
        -------
        new : FileEntryCollection
            Selected data base.
        """
        new = self[self.index(id=id, filetype=filetype, keywords=keywords, **kwargs)]
        for ientry, entry in enumerate(new.data):
            new.data[ientry] = entry.select(**kwargs)  # select options
        return new

    def get(self, *args, **kwargs):
        """
        Return the :class:`File` instance that matches input arguments, see :meth:`select`.
        If :meth:`select` returns several file entries, and / or file entries with multiples files,
        a :class:`ValueError` is raised.
        """
        new = self.select(*args, **kwargs)
        if len(new) == 1 and len(new[0]) == 1:
            for fi in new[0]:
                return fi  # File instance
        if prod(len(entry) for entry in new.data) == 0:
            raise ValueError('"get" is not applicable as there are no matching entries')
        else:
            raise ValueError('"get" is not applicable as there are {} entries with multiple options:\n{}'.format(len(new.data), '\n'.join([repr(entry) for entry in new.data])))

    def __getitem__(self, index):
        """Return file entry(ies) at the input index(ices) in the list."""
        if utils.is_sequence(index):
            data = [self.data.__getitem__(ii) for ii in index]
        else:
            data = self.data.__getitem__(index)
        if isinstance(data, list):
            return self.clone(data=data)
        return data

    def __delitem__(self, index):
        """Delete file entry(ies) at the input index(ices) in the list."""
        if not utils.is_sequence(index):
            index = [index]
        for ii in index:
            self.data.__delitem__(ii)

    def __iter__(self):
        """Iterate over file entries."""
        return iter(self.data)

    def __len__(self):
        """Length, i.e. number of file entries."""
        return len(self.data)

    def insert(self, index, entry):
        """
        Insert a new file entry, at input index.
        ``entry`` may be e.g. a dictionary, or a :class:`FileEntry` instance,
        in which case a shallow copy is made.
        """
        entry = FileEntry(entry)
        entry.environ = self.environ
        self.data.insert(index, entry)

    def append(self, entry):
        """Append an input file entry, which may be e.g. a dictionary, or a :class:`FileEntry` instance."""
        entry = FileEntry(entry)
        entry.environ = self.environ
        self.data.append(entry)

    def write(self, fn):
        """Write data base to *yaml* file ``fn``."""
        utils.mkdir(os.path.dirname(fn))

        with open(fn, 'w') as file:

            def list_rep(dumper, data):
                return dumper.represent_sequence(u'tag:yaml.org,2002:seq', data, flow_style=True)

            yaml.add_representer(list, list_rep)

            yaml.dump_all([utils.dict_to_yaml(entry.to_dict()) for entry in self.data], file, default_flow_style=False)

    def __copy__(self):
        """Return a shallow copy (data list is copied)."""
        new = super(FileEntryCollection, self).__copy__()
        new.data = self.data.copy()
        return new

    def update(self, **kwargs):
        """Update :attr:`data` (list of :class:`FileEntry`) or :attr:`environ` (dict)."""
        if 'data' in kwargs:
            self.data = []
            for entry in kwargs.pop('data'): self.append(entry)
        if 'environ' in kwargs:
            environ = kwargs.pop('environ')
            for entry in self.data:
                entry.environ = environ
        if kwargs:
            raise ValueError('Unrecognized arguments {}'.format(kwargs))

    def clone(self, *args, **kwargs):
        """Return an updated copy."""
        new = self.copy()
        new.update(*args, **kwargs)
        return new

    def __add__(self, other):
        """Sum of `self`` + ``other``."""
        new = self.copy()
        for entry in other.data:
            new.append(entry)
        return new

    def __radd__(self, other):
        if other == 0: return self.copy()
        return self.__add__(other)

    def __iadd__(self, other):
        if other == 0: return self.copy()
        return self.__add__(other)

    @property
    def filepaths(self):
        """All file paths in file data base."""
        toret = []
        for entry in self.data:
            toret += [ff.filepath for ff in entry]
        return toret

    def __repr__(self):
        return '{}(\n{}\n)'.format(self.__class__.__name__, ',\n'.join([repr(entry) for entry in self.data]))


class FileManager(FileEntryCollection):

    """File manager, main class to be used to get paths to / read / write files."""

    def __init__(self, database=(), environ=None):
        environ = get_environ(environ).to_dict(all=True)
        dbc = FileEntryCollection(environ=environ)
        for db in _make_list(database):
            dbc += FileEntryCollection(db)
        self.__dict__.update(dbc.__dict__)

    def update(self, **kwargs):
        """Update :attr:`data` (list of :class:`FileEntry`) or :attr:`environ` (dict)."""
        if 'environ' in kwargs:
            kwargs['environ'] = get_environ(kwargs['environ']).to_dict(all=True)
        super(FileManager, self).update(**kwargs)

    @property
    def options(self):
        """Return intersection of all options, i.e. options that are common to all file entries."""
        if not self.data:
            return {}

        options = dict(self.data[0].options)

        def _intersect(options1, options2):
            options = {}
            for name, values1 in options1.items():
                if name in options2:
                    options[name] = in_options(values1, options2[name])
            return options

        for entry in self.data:
            options = _intersect(options, entry.options)
        return options

    def __iter__(self):
        """Loop over options that are common to all file entries (:attr:`options`), and yield the (selected) :class:`FileEntryCollection` instances."""
        if not self.data:
            return
        for options in iter_options(self.options):
            database = self.clone(data=[])
            for entry in self.data:
                entry = entry.select(**{**entry.options, **options})
                database.append(entry)
            yield database

    @classmethod
    def databases(cls, keywords=None):
        if keywords is not None:
            keywords = _make_list(keywords)
            keywords = [keyword.lower().split() for keyword in keywords]
        from .config import Config
        file = Config().get('file', {})
        filenames = _make_list(file.get('filename', []))
        toret = []
        for filename in filenames:
            for fn in glob.glob(filename):
                if any(all(kw in filename for kw in keyword) for keyword in keywords):
                    toret.append(fn)
        return toret
