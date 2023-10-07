"""A few utilities."""

import os
import sys
import time
import logging
import traceback
from collections import UserDict
from pathlib import Path


def exception_handler(exc_type, exc_value, exc_traceback, mpicomm=None):
    """Print exception with a logger."""
    # Do not print traceback if the exception has been handled and logged
    _logger_name = 'Exception'
    log = logging.getLogger(_logger_name)
    line = '=' * 100
    # log.critical(line[len(_logger_name) + 5:] + '\n' + ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + line)
    log.critical('\n' + line + '\n' + ''.join(traceback.format_exception(exc_type, exc_value, exc_traceback)) + line)
    if exc_type is KeyboardInterrupt:
        log.critical('Interrupted by the user.')
    else:
        log.critical('An error occured.')


def mkdir(dirname, **kwargs):
    """Try to create ``dirname`` and catch :class:`OSError`."""
    try:
        os.makedirs(dirname, **kwargs)  # MPI...
    except OSError:
        return


def setup_logging(level=logging.INFO, stream=None, filename=None, filemode='w', **kwargs):
    """
    Set up logging.

    Note
    ----
    You may find it useful to have different logging level depending on the process; e.g.
    ``setup_logging(level=(logging.INFO if mpicomm.rank == 0 else logging.ERROR))``
    will set INFO level on rank 0, and ERROR level on all other ranks of the MPI communicator ``mpicomm``.

    Parameters
    ----------
    level : string, int, default=logging.INFO
        Logging level.

    stream : _io.TextIOWrapper, default=sys.stdout
        Where to stream.

    filename : string, default=None
        If not ``None`` stream to file name.

    filemode : string, default='w'
        Mode to open file, only used if filename is not ``None``.

    kwargs : dict
        Other arguments for :func:`logging.basicConfig`.
    """
    # Cannot provide stream and filename kwargs at the same time to logging.basicConfig, so handle different cases
    # Thanks to https://stackoverflow.com/questions/30861524/logging-basicconfig-not-creating-log-file-when-i-run-in-pycharm
    if isinstance(level, str):
        level = {'info': logging.INFO, 'debug': logging.DEBUG, 'warning': logging.WARNING}[level.lower()]
    for handler in logging.root.handlers:
        logging.root.removeHandler(handler)
    if stream is None:
        stream = sys.stdout

    t0 = time.time()

    class MyFormatter(logging.Formatter):

        def format(self, record):
            self._style._fmt = '[%09.2f] ' % (time.time() - t0) + ' %(asctime)s %(name)-25s %(levelname)-8s %(message)s'
            return super(MyFormatter, self).format(record)

    fmt = MyFormatter(datefmt='%m-%d %H:%M ')
    if filename is not None:
        mkdir(os.path.dirname(filename))
        handler = logging.FileHandler(filename, mode=filemode)
    else:
        handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(fmt)
    logging.basicConfig(level=level, handlers=[handler], **kwargs)
    sys.excepthook = exception_handler


class LoggingContext(object):
    """
    Class to locally update logging level:

    >>> with LoggingContext('warning') as mem:
            ...
            # Logging level is warning
            logger = logging.getLogger('Logger')
            logger.info('This should not be printed')  # not logged
            logger.warning('This should be printed')  # logged
            ...
    """
    def __init__(self, level=None):
        self._level = logging.root.level
        if level is not None:
            if isinstance(level, str):
                level = {'info': logging.INFO, 'debug': logging.DEBUG, 'warning': logging.WARNING}[level.lower()]
            logging.root.level = level
            for handler in logging.root.handlers:
                handler.setLevel(level)

    def __enter__(self):
        """Enter context."""
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        """Exit context."""
        logging.root.level = level = self._level
        for handler in logging.root.handlers:
            handler.setLevel(level)


class BaseMetaClass(type):

    """Metaclass to add logging attributes to :class:`BaseClass` derived classes."""

    def __new__(meta, name, bases, class_dict):
        cls = type.__new__(meta, name, bases, class_dict)
        cls.set_logger()
        return cls

    def set_logger(cls):
        """
        Add attributes for logging:

        - logger
        - methods log_debug, log_info, log_warning, log_error, log_critical
        """
        cls.logger = logging.getLogger(cls.__name__)

        def make_logger(level):

            @classmethod
            def logger(cls, *args, **kwargs):
                getattr(cls.logger, level)(*args, **kwargs)

            return logger

        for level in ['debug', 'info', 'warning', 'error', 'critical']:
            setattr(cls, 'log_{}'.format(level), make_logger(level))


class BaseClass(object, metaclass=BaseMetaClass):
    """
    Base class that implements :meth:`copy`.
    To be used throughout this package.
    """
    def __copy__(self, *args, **kwargs):
        new = self.__class__.__new__(self.__class__)
        new.__dict__.update(self.__dict__)
        return new

    def copy(self, *args, **kwargs):
        return self.__copy__(*args, **kwargs)


def is_sequence(item):
    """Whether input item is a tuple or list."""
    return isinstance(item, (list, tuple, set, frozenset))


def is_path(item):
    """Whether input item is a path."""
    return isinstance(item, (str, Path))


class DictMetaClass(type(BaseClass), type(UserDict)):

    pass


class BaseDict(UserDict, BaseClass, metaclass=DictMetaClass):

    def clone(self, *args, **kwargs):
        new = self.copy()
        new.update(*args, **kwargs)
        new.__init__()
        return new


def dict_to_yaml(d):
    """
    (Recursively) cast objects of input dictionary ``d`` to Python base types,
    such that they can be understood by the base yaml.
    """
    import numbers
    toret = {}
    for k, v in d.items():
        if isinstance(v, dict):
            v = dict_to_yaml(v)
        elif is_sequence(v):
            v = dict_to_yaml({i: vv for i, vv in enumerate(v)})
            v = [v[i] for i in range(len(v))]
        #elif isinstance(v, np.ndarray):
        #    if v.size == 1:
        #        v = v.item()
        #    else:
        #        v = v.tolist()
        #elif isinstance(v, np.floating):
        #    v = float(v)
        #elif isinstance(v, np.integer):
        #    v = int(v)
        elif not isinstance(v, (bool, numbers.Number)):
            v = str(v)
        toret[k] = v
    return toret