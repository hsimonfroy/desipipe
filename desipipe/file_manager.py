import os
import re
import copy
import itertools

import yaml

from . import utils
from .utils import BaseClass, BaseDict
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
    return yaml.load_all(string, Loader=YamlLoader)


class BaseFile(BaseDict):

    _defaults = dict(description='', id=None, filetype=None, path='', options=dict())

    def __init__(self, *args, **kwargs):
        super(BaseFile, self).__init__(*args, **kwargs)
        for name, value in self._defaults.items():
            self.setdefault(name, copy.copy(value))


def _make_property(name):

    def getter(self):
        return self[name]

    return getter


for name in BaseFile._defaults:
    setattr(BaseFile, name, _make_property(name))


def _make_list(values):
    if not hasattr(values, '__iter__') or isinstance(values, str):
        values = [values]
    return list(values)


class FileEntry(BaseFile):

    """Class describing a file entry."""
    _defaults = dict(description='', author='', id=None, filetype=None, path='', options=dict())

    def __init__(self, *args, **kwargs):
        super(FileEntry, self).__init__(*args, **kwargs)
        options = {}
        for name, values in self['options'].items():
            if isinstance(values, str) and re.match(values, 'range(*)'):
                values = eval(values)
            options[name] = _make_list(values)
        self['options'] = options

    def options(self):
        for values in itertools.product(*self['options'].values()):
            return {name: values[iname] for iname, name in enumerate(self['options'])}

    def select(self, **kwargs):
        options = self['options'].copy()
        for name, values in kwargs.items():
            if name in options:
                options[name] = values
            else:
                raise ValueError('Unknown option {}, select from {}'.format(name, list(self['options'].keys())))
        return self.clone(options=options)

    def __len__(self):
        size = 1
        for values in self['options'].values():
            size *= len(values)
        return size

    def __iter__(self):
        for options in self.options():
            fi = File({**self.data, 'options': options})
            fi.environ = getattr(self, 'environ', {})
            return fi


class File(BaseFile):

    """Class describing one file."""
    @property
    def path(self):
        path = self['path']
        environ = getattr(self, 'environ', {})
        placeholders = re.finditer(r'\$\{.*?\}', path)
        for placeholder in placeholders:
            placeholder = placeholder.group()
            placeholder_nobrackets = placeholder[2:-1]
            if placeholder_nobrackets in environ:
                path = path.replace(placeholder, environ[placeholder_nobrackets])
        return path.replace(**self.options)

    def read(self, *args, **kwargs):
        return get_filetype(filetype=self.filetype, path=self.path).read(*args, **kwargs)

    def write(self, *args, **kwargs):
        return get_filetype(filetype=self.filetype, path=self.path).write(*args, **kwargs)


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

    def __iter__(self):
        return iter(self.data)

    def __len__(self):
        return len(self.data)

    def insert(self, index, entry):
        self.data.insert(index, FileEntry(entry))

    def append(self, entry):
        self.data.append(FileEntry(entry))

    def write(self, fn):

        with open(fn, 'w') as file:

            def list_rep(dumper, data):
                return dumper.represent_sequence(u'tag:yaml.org,2002:seq', data, flow_style=True)

            yaml.add_representer(list, list_rep)

            with open(fn, 'w') as file:
                for entry in self:
                    file.write(yaml.dumps(utils.dict_to_yaml(entry), default_flow_style=False) + '\n---\n')


class FileManager(BaseClass):

    def __init__(self, database=(), environ=None):
        self.database = sum(FileDataBase(database) for database in _make_list(database))
        self.environ = get_environ(environ)
        for entry in self.database:
            entry.environ = self.environ

    def select(self, *args, **kwargs):
        new = self.copy()
        new.database = self.database.select(*args, **kwargs)
        return new

    def __iter__(self):
        if not self.database:
            return []
        options = dict(self.database[0])

        def _intersect(options1, options2):
            options = {}
            for name, values1 in options1.items():
                if name in options2:
                    values2 = options2[name]
                    values = [value for value in values1 if value in values2]
                    options[name] = values
            return options

        for entry in self.database:
            options = _intersect(options, entry.options)
        for values in itertools.product(*options.values()):
            opt = {name: values[iname] for iname, name in enumerate(options)}
            database = FileDataBase()
            for entry in self.database:
                fi = entry.clone(options={**entry['options'], **opt})
                fi.environ = self.environ
                database.append(fi)
            yield database
