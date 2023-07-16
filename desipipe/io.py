"""To implement a new file format, just subclass :class:`BaseFile`."""

import os

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

    """Base class to handle a file, with path saved as :attr:`path`, and :meth:`read` and :meth:`write` methods."""

    name = 'base'

    def __init__(self, path):
        self.path = path

    def read(self):
        raise NotImplementedError('Implement read method in {}'.format(self.__class__.__name__))

    def write(self):
        raise NotImplementedError('Implement write method in {}'.format(self.__class__.__name__))


def get_filetype(filetype, path, *args, **kwargs):
    """
    Convenient function that returns a :class:`BaseFile` instance.

    Parameters
    ----------
    filetype : str, :class:`BaseFile`
        Name of :class:`BaseFile`, or :class:`BaseFile` instance.

    path : str
        Path to file.

    *args : tuple
        Other arguments for :class:`BaseFile`.

    **kwargs : dict
        Other optional arguments for :class:`BaseFile`.

    Returns
    -------
    file : BaseFile
    """
    if isinstance(filetype, BaseFile):
        return filetype
    return BaseFile._registry[filetype](path, *args, **kwargs)


class TextFile(BaseFile):

    """Text file."""
    name = 'text'

    def read(self):
        """Read file."""
        with open(self.path, 'r') as file:
            return file.read()

    def write(self, txt):
        """Write file."""
        utils.mkdir(os.path.dirname(txt))
        with open(self.path, 'w') as file:
            file.write(txt)


class CatalogFile(BaseFile):

    """Catalog file."""
    name = 'catalog'

    def read(self, **kwargs):
        """Read catalog."""
        from mpytools import Catalog
        return Catalog.read(self.path, **kwargs)

    def write(self, catalog, **kwargs):
        """Write catalog."""
        return catalog.write(self.path, **kwargs)


class PowerSpectrumFile(BaseFile):

    """Power spectrum file."""
    name = 'power'

    def read(self, mode='poles'):
        """Read power spectrum."""
        from pypower import MeshFFTPower, PowerSpectrumStatistics
        with utils.LoggingContext(level='warning'):
            toret = MeshFFTPower.load(self.path)
            try:
                toret = getattr(toret, mode)
            except AttributeError:
                toret = PowerSpectrumStatistics.load(self.path)
        return toret

    def write(self, power):
        """Write power spectrum."""
        return power.save(self.path)


class CorrelationFunctionFile(BaseFile):

    """Correlation function file."""
    name = 'correlation'

    def read(self):
        """Read correlation function."""
        from pycorr import TwoPointCorrelationFunction
        with utils.LoggingContext(level='warning'):
            toret = TwoPointCorrelationFunction.load(self.path)
        return toret

    def write(self, corr):
        """Write correlation function."""
        return corr.save(self.path)


class BaseMatrixFile(BaseFile):

    """Power spectrum file."""
    name = 'wmatrix'

    def read(self, mode='poles'):
        """Read matrix."""
        from pypower import MeshFFTWindow, BaseMatrix
        toret = MeshFFTWindow.load(self.path)
        try:
            toret = getattr(toret, mode)
        except AttributeError:
            toret =  BaseMatrix.load(self.path)
        return toret

    def write(self, matrix):
        """Write matrix."""
        return matrix.save(self.path)


class GenericFile(BaseFile):

    """Generic file."""
    name = 'generic'

    def read(self, read, **kwargs):
        """Read."""
        return read(self.path, **kwargs)

    def write(self, write, **kwargs):
        """Write."""
        write(self.path, **kwargs)