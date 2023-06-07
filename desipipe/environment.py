import os
import sys
import copy
import contextlib

from .utils import BaseDict


def _insert_first(li, first):
    while True:
        try:
            li.remove(first)
        except ValueError:
            break
    li.insert(0, first)
    return li


def bash_env(command):
    import subprocess
    environ = {}
    result = subprocess.run(['bash', '-c', '{} && env'.format(command)], capture_output=True, text=True)
    for line in result.stdout.split('\n'):
        try:
            key, value = line.split('=')
            environ[key] = value
        except ValueError:
            pass
    return environ


class RegisteredEnvironment(type(BaseDict)):

    """Metaclass registering :class:`BaseEnvironment`-derived classes."""

    _registry = {}

    def __new__(meta, name, bases, class_dict):
        cls = super().__new__(meta, name, bases, class_dict)
        meta._registry[cls.name] = cls
        return cls


class BaseEnvironment(BaseDict, metaclass=RegisteredEnvironment):

    name = 'base'
    _defaults = dict()

    def __init__(self, data=None, command=None):
        for name, value in self._defaults.items():
            self.setdefault(name, copy.copy(value))
        self.command = command
        super(BaseEnvironment, self).__init__(**(data or {}))

    def all(self):  # including command
        new = self.copy()
        new.udpate(bash_env(self.command))
        return new

    def as_script(self):
        toret = ''
        if self.command:
            toret += self.command + '\n'
        for name, value in self.items():
            toret += 'export {}={}\n'.format(name, value)
        return toret


def get_environ(environ=None, data=None, **kwargs):
    if isinstance(environ, BaseEnvironment):
        return environ
    if environ is None:
        from .config import Config
        config = Config().get('environ', {})
        if isinstance(config, str):
            environ = config
        else:
            environ = config.pop('name', 'base')
            kwargs = {**config.pop(environ, {}), **(data or {})}
    return BaseEnvironment._registry[environ](data=data, **kwargs)


Environment = get_environ


@contextlib.contextmanager
def change_environ(environ):
    """
    Temporarily set the process environment variables.

    >>> with set_env(PLUGINS_DIR='test/plugins'):
    ...   "PLUGINS_DIR" in os.environ
    True

    >>> "PLUGINS_DIR" in os.environ
    False

    :type environ: dict[str, unicode]
    :param environ: Environment variables to set
    """
    if environ is None:
        yield
    environ_bak = os.environ.copy()
    syspath_bak = sys.path.copy()
    os.environ.clear()
    os.environ.update(environ)
    for key, value in environ.items():
        if key == 'PYTHONPATH':
            for path in value.split(':')[::-1]: _insert_first(sys.path, path)
        else:
            os.environ[key] = value
    try:
        yield
    finally:
        os.environ.clear()
        os.environ.update(environ_bak)
        sys.path.clear()
        sys.path += syspath_bak


class BaseNERSCEnvironment(BaseEnvironment):

    name = 'nersc-base'
    _defaults = dict(DESICFS='/global/cfs/cdirs/desi')


class CosmodesiNERSCEnvironment(BaseNERSCEnvironment):

    name = 'nersc-cosmodesi'
    _defaults = dict(DESICFS='/global/cfs/cdirs/desi')
    _command = 'source /global/cfs/cdirs/desi/users/adematti/cosmodesi_environment.sh main'