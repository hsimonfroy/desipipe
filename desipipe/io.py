from . import utils
from .utils import BaseClass


class RegisteredFile(type(BaseClass)):

    """Metaclass registering :class:`BaseFile`-derived classes."""

    _registry = {}

    def __new__(meta, name, bases, class_dict):
        cls = super().__new__(meta, name, bases, class_dict)
        meta._registry[cls.name] = cls
        return cls


class BaseFile(BaseClass, metaclass=RegisteredFile):

    name = 'base'

    def __init__(self, path):
        self.path = path

    def read(self):
        raise NotImplementedError('Implement read method in {}'.format(self.__class__.__name__))

    def write(self):
        raise NotImplementedError('Implement write method in {}'.format(self.__class__.__name__))


def get_filetype(filetype, path, *args, **kwargs):
    if isinstance(filetype, BaseFile):
        return filetype
    return BaseFile._registry[filetype](path, *args, **kwargs)


class CatalogFile(BaseFile):

    name = 'catalog'

    def read(self, *args, **kwargs):
        from mpytools import Catalog
        return Catalog.read(self.path, *args, **kwargs)

    def write(self, catalog, *args, **kwargs):
        return catalog.write(self.path, *args, **kwargs)


class PowerSpectrumFile(BaseFile):

    name = 'power'

    def read(self):
        from pypower import MeshFFTPower, PowerSpectrumMultipoles
        with utils.LoggingContext(level='warning'):
            toret = MeshFFTPower.load(self.path)
            if hasattr(toret, 'poles'):
                toret = toret.poles
            else:
                toret = PowerSpectrumMultipoles.load(self.path)
        return toret

    def write(self, power):
        return power.save(self.path)