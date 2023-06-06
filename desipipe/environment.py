import os
import sys
import copy

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
    _command = None

    def __init__(self, *args, **kwargs):
        if self._command is not None:
            self.update(bash_env(self._command))
        for name, value in self._defaults.items():
            self.setdefault(name, copy.copy(value))
        super(BaseEnvironment, self).__init__(*args, **kwargs)
        for key, value in self.items():
            if key == 'PYTHONPATH':
                for path in value.split(':')[::-1]: _insert_first(sys.path, path)
            else:
                os.environ[key] = value


def get_environ(environ, *args, **kwargs):
    if isinstance(environ, BaseEnvironment):
        return environ
    if environ is None:
        return BaseEnvironment()
    return BaseEnvironment._registry[environ](*args, **kwargs)


Environment = get_environ


class BaseNERSCEnvironment(BaseEnvironment):

    name = 'nersc-base'
    _defaults = dict(DESICFS='/global/cfs/cdirs/desi')


class CosmodesiNERSCEnvironment(BaseNERSCEnvironment):

    name = 'nersc-cosmodesi'
    _command = 'source /global/cfs/cdirs/desi/users/adematti/cosmodesi_environment.sh main'