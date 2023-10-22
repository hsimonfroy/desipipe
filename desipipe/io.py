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

    """Base class to handle a file, with path saved as :attr:`path`, and :meth:`load` and :meth:`save` methods."""

    name = 'base'

    def __init__(self, path):
        self.path = path

    def load(self):
        raise NotImplementedError('Implement load method in {}'.format(self.__class__.__name__))

    def save(self):
        raise NotImplementedError('Implement save method in {}'.format(self.__class__.__name__))


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

    def load(self):
        """Load file."""
        with open(self.path, 'r') as file:
            return file.read()

    def save(self, txt):
        """Save file."""
        utils.mkdir(os.path.dirname(self.path))
        with open(self.path, 'w') as file:
            file.write(txt)


class CatalogFile(BaseFile):

    """Catalog file."""
    name = 'catalog'

    def load(self, **kwargs):
        """Load catalog."""
        from mpytools import Catalog
        return Catalog.read(self.path, **kwargs)

    def save(self, catalog, **kwargs):
        """Save catalog."""
        return catalog.write(self.path, **kwargs)


class PowerSpectrumFile(BaseFile):

    """Power spectrum file."""
    name = 'power'

    def load(self, *select, mode='poles'):
        """Load power spectrum."""
        from pypower import MeshFFTPower, PowerSpectrumStatistics
        with utils.LoggingContext(level='warning'):
            toret = MeshFFTPower.load(self.path)
            try:
                toret = getattr(toret, mode)
            except AttributeError:
                toret = PowerSpectrumStatistics.load(self.path)
        if select:
            toret = toret.select(*select)
        return toret

    def save(self, power):
        """Save power spectrum."""
        return power.save(self.path)


class CorrelationFunctionFile(BaseFile):

    """Correlation function file."""
    name = 'correlation'

    def load(self, *select):
        """Load correlation function."""
        from pycorr import TwoPointCorrelationFunction
        with utils.LoggingContext(level='warning'):
            toret = TwoPointCorrelationFunction.load(self.path)
        if select:
            toret = toret.select(*select)
        return toret

    def save(self, corr):
        """Save correlation function."""
        return corr.save(self.path)


class BaseMatrixFile(BaseFile):

    """Power spectrum file."""
    name = 'wmatrix'

    def load(self, mode='poles'):
        """Load matrix."""
        from pypower import MeshFFTWindow, BaseMatrix
        toret = MeshFFTWindow.load(self.path)
        try:
            toret = getattr(toret, mode)
        except AttributeError:
            toret =  BaseMatrix.load(self.path)
        return toret

    def save(self, matrix):
        """Save matrix."""
        return matrix.save(self.path)


class ChainFile(BaseFile):

    """Chain file."""

    def load(self):
        """Load chain."""
        from desilike.samples import Chain
        return Chain.load(self.path)

    def save(self, chain):
        """Save chain."""
        return chain.save(self.path)


class ProfilesFile(BaseFile):

    """Profiles file."""

    def load(self):
        """Load profiles."""
        from desilike.samples import Profiles
        return Profiles.load(self.path)

    def save(self, profiles):
        """Save profiles."""
        return profiles.save(self.path)


class GenericFile(BaseFile):

    """Generic file."""
    name = 'generic'

    def load(self, load, **kwargs):
        """Load file."""
        for name in ['load', 'read']:
            func = getattr(load, name, None)
            if callable(func):
                load = func
                break
        return load(self.path, **kwargs)

    def save(self, save, **kwargs):
        """Save file."""
        for name in ['save', 'write']:
            func = getattr(save, name, None)
            if callable(func):
                save = func
                break
        save(self.path, **kwargs)