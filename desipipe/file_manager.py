import os
import re
import copy
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


class BaseFile(BaseClass):

    _defaults = dict(filetype='', path='', id='', author='', options=dict(), description='')

    def __init__(self, *args, **kwargs):
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
        new = self.copy()
        new.update(**kwargs)
        return new

    def update(self, **kwargs):
        for name, value in kwargs.items():
            if name in self._defaults:
                setattr(self, name, type(self._defaults[name])(value))
            else:
                raise ValueError('Unknown argument {}; supports {}'.format(name, list(self._defaults)))

    def as_dict(self):
        return {name: getattr(self, name) for name in self._defaults}

    def __repr__(self):
        return '{}({})'.format(self.__class__.__name__, ', '.join(['{}={}'.format(name, value) for name, value in self.as_dict()]))


def _make_list(values):
    if isinstance(values, range):
        return values
    if not hasattr(values, '__iter__') or isinstance(values, str):
        values = [values]
    return list(values)


class FileEntry(BaseFile):

    """Class describing a file entry."""

    def update(self, **kwargs):
        super(FileEntry, self).update(**kwargs)
        if 'options' in kwargs:
            options = {}
            for name, values in self.options.items():
                if isinstance(values, str) and re.match(r'range\((.*)\)$', values):
                    values = eval(values)
                options[name] = _make_list(values)
            self.options = options

    def select(self, **kwargs):
        options = self.options.copy()
        for name, values in kwargs.items():
            if name in options:
                options[name] = values
            else:
                raise ValueError('Unknown option {}, select from {}'.format(name, list(self['options'].keys())))
        return self.clone(options=options)

    def __len__(self):
        size = 1
        for values in self.options.values():
            size *= len(values)
        return size

    def __iter__(self):
        for values in itertools.product(*self.options.values()):
            options = {name: values[iname] for iname, name in enumerate(self.options)}
            fi = File()
            fi.__dict__.update(self.__dict__)
            fi.options = options
            yield fi


class File(BaseFile):

    """Class describing one file."""

    @property
    def rpath(self):
        path = self.path
        environ = getattr(self, 'environ', {})
        placeholders = re.finditer(r'\$\{.*?\}', path)
        for placeholder in placeholders:
            placeholder = placeholder.group()
            placeholder_nobrackets = placeholder[2:-1]
            if placeholder_nobrackets in environ:
                path = path.replace(placeholder, environ[placeholder_nobrackets])
        return path.format(**self.options)

    def read(self, *args, **kwargs):
        return get_filetype(filetype=self.filetype, path=self.rpath).read(*args, **kwargs)

    def write(self, *args, **kwargs):
        save_attrs = getattr(self, 'save_attrs', None)
        rpath = self.rpath
        dirname = os.path.dirname(rpath)
        utils.mkdir(dirname)
        with tempfile.TemporaryDirectory() as tmp_dir:
            if save_attrs is not None:
                fns = save_attrs(tmp_dir)
                for fn in fns:
                    os.rename(fn, os.path.join(dirname, os.path.relpath(fn, tmp_dir)))
            path = os.path.join(tmp_dir, 'tmp')
            toret = get_filetype(filetype=self.filetype, path=path).write(*args, **kwargs)
            os.rename(path, rpath)
            return toret


class FileDataBase(BaseClass):

    """A collection of file descriptors."""

    def __init__(self, data=None, string=None, parser=None, **kwargs):
        """
        Initialize :class:`FileDataBase`.

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

        kwargs : dict
            Arguments for :func:`parser`.
        """
        if isinstance(data, self.__class__):
            self.__dict__.update(data.copy().__dict__)

        self.parser = parser
        if parser is None:
            self.parser = yaml_parser

        datad = []

        if utils.is_path(data):
            if string is None: string = ''
            with open(data, 'r') as file:
                string += file.read()
        elif data is not None:
            datad += [FileEntry(dd) for dd in data]

        if string is not None:
            datad += [FileEntry(dd) for dd in self.parser(string, **kwargs)]

        self.data = datad

    def select(self, ids=None, keywords=None, prune=False, **kwargs):
        new = self.__class__()
        if ids is not None:
            ids = _make_list(ids)
            ids = [iid.lower() for iid in ids]
        if keywords is not None:
            keywords = _make_list(keywords)
            keywords = [keyword.split() for keyword in keywords]
        for entry in self.data:
            if ids is not None and entry.id.lower() not in ids:
                continue
            if keywords is not None:
                description = entry.description.lower()
                if not any(all(kw in description for kw in keyword) for keyword in keywords):
                    continue
            entry = entry.select(**kwargs)
            if prune and not entry:
                continue
            new.data.append(entry)
        return new

    def get(self, *args, **kwargs):
        new = self.select(*args, prune=True, **kwargs)
        if len(new) == 1 and len(new[0]):
            for fi in new[0]:
                return fi  # File instance
        raise ValueError('There are {} entries with {} files'.format(len(new), [len(entry) for entry in new]))

    def __getitem__(self, index):
        data = self.data.__getitem__(index)
        if isinstance(data, list):
            return self.__class__(data)
        return data

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def insert(self, index, entry):
        self.data.insert(index, FileEntry(entry))

    def append(self, entry):
        self.data.append(FileEntry(entry))

    def write(self, fn):

        utils.mkdir(os.path.dirname(fn))

        with open(fn, 'w') as file:

            def list_rep(dumper, data):
                return dumper.represent_sequence(u'tag:yaml.org,2002:seq', data, flow_style=True)

            yaml.add_representer(list, list_rep)

            yaml.dump_all([utils.dict_to_yaml(entry.as_dict()) for entry in self], file, default_flow_style=False)


    def __copy__(self):
        new = super(FileDataBase, self).__copy__()
        new.data = self.data.copy()
        return new

    def __add__(self, other):
        """Sum of `self`` + ``other``."""
        new = self.__class__()
        new.data += self.data
        new.data += other.data
        return new

    def __radd__(self, other):
        if other == 0: return self.copy()
        return self.__add__(other)

    def __iadd__(self, other):
        if other == 0: return self.copy()
        return self.__add__(other)


class FileManager(BaseClass):

    def __init__(self, database=(), environ=None):
        self.db = sum(FileDataBase(database) for database in _make_list(database))
        self.environ = get_environ(environ)
        for entry in self.db:
            entry.environ = self.environ.as_dict(all=True)

    def select(self, *args, **kwargs):
        new = self.copy()
        new.db = self.db.select(*args, **kwargs)
        return new

    def __len__(self):
        return len(self.db)

    def __iter__(self):
        if not self.db:
            return []

        options = dict(self.db[0].options)

        def _intersect(options1, options2):
            options = {}
            for name, values1 in options1.items():
                if name in options2:
                    values2 = options2[name]
                    values = [value for value in values1 if value in values2]
                    options[name] = values
            return options

        for entry in self.db:
            options = _intersect(options, entry.options)
        for values in itertools.product(*options.values()):
            opt = {name: values[iname] for iname, name in enumerate(options)}
            database = FileDataBase()
            for entry in self.db:
                fi = entry.clone(options={**entry.options, **opt})
                fi.environ = self.environ.as_dict(all=True)
                database.append(fi)
            yield database
