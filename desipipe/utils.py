"""A few utilities."""

import os
import sys
import time
import logging
import traceback
import shutil
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


def _copytree(entries, src, dst, symlinks, ignore, copy_function,
              ignore_dangling_symlinks, dirs_exist_ok=False, dirs_copystat=True):
    """
    Slight modification of `shutil._copytree`.
    Added `dirs_copystat` flag: if false, `shutil.copystat` is not applied to directories.
    See `copytree` for more details.
    """
    if ignore is not None:
        ignored_names = ignore(os.fspath(src), [x.name for x in entries])
    else:
        ignored_names = ()

    os.makedirs(dst, exist_ok=dirs_exist_ok)
    errors = []
    use_srcentry = copy_function is shutil.copy2 or copy_function is shutil.copy

    for srcentry in entries:
        if srcentry.name in ignored_names:
            continue
        srcname = os.path.join(src, srcentry.name)
        dstname = os.path.join(dst, srcentry.name)
        srcobj = srcentry if use_srcentry else srcname
        try:
            is_symlink = srcentry.is_symlink()
            if is_symlink and os.name == 'nt':
                # Special check for directory junctions, which appear as
                # symlinks but we want to recurse.
                lstat = srcentry.stat(follow_symlinks=False)
                if lstat.st_reparse_tag == stat.IO_REPARSE_TAG_MOUNT_POINT:
                    is_symlink = False
            if is_symlink:
                linkto = os.readlink(srcname)
                if symlinks:
                    # We can't just leave it to `copy_function` because legacy
                    # code with a custom `copy_function` may rely on copytree
                    # doing the right thing.
                    os.symlink(linkto, dstname)
                    shutil.copystat(srcobj, dstname, follow_symlinks=not symlinks)
                else:
                    # ignore dangling symlink if the flag is on
                    if not os.path.exists(linkto) and ignore_dangling_symlinks:
                        continue
                    # otherwise let the copy occur. copy2 will raise an error
                    if srcentry.is_dir():
                        copytree(srcobj, dstname, symlinks, ignore,
                                copy_function, ignore_dangling_symlinks,
                                dirs_exist_ok, dirs_copystat)
                    else:
                        copy_function(srcobj, dstname)
            elif srcentry.is_dir():
                copytree(srcobj, dstname, symlinks, ignore, copy_function,
                        ignore_dangling_symlinks, dirs_exist_ok, dirs_copystat)
            else:
                # Will raise a SpecialFileError for unsupported file types
                copy_function(srcobj, dstname)
        # catch the Error from the recursive copytree so that we can
        # continue with other files
        except shutil.Error as err:
            errors.extend(err.args[0])
        except OSError as why:
            errors.append((srcname, dstname, str(why)))
    if dirs_copystat:
        try:
            shutil.copystat(src, dst)
        except OSError as why:
            # Copying file access times may fail on Windows
            if getattr(why, 'winerror', None) is None:
                errors.append((src, dst, str(why)))
    if errors:
        raise shutil.Error(errors)
    return dst


def copytree(src, dst, symlinks=False, ignore=None, copy_function=shutil.copy2,
             ignore_dangling_symlinks=False, dirs_exist_ok=False, dirs_copystat=True):
    """
    Slight modification of `shutil.copytree`.
    Added `dirs_copystat` flag: if false, `shutil.copystat` is not applied to directories.
    Motivation: `shutil.copystat` can fail if the owner of the destination directory or file
    is different from the current user. `shutil.copy2` involves `shutil.copystat` as well.
    Thus to avoid this operation completely, use `copy_function=shutil.copyfile`
    and `dirs_copystat=False`.

    Recursively copy a directory tree and return the destination directory.

    If exception(s) occur, an Error is raised with a list of reasons.

    If the optional symlinks flag is true, symbolic links in the
    source tree result in symbolic links in the destination tree; if
    it is false, the contents of the files pointed to by symbolic
    links are copied. If the file pointed by the symlink doesn't
    exist, an exception will be added in the list of errors raised in
    an Error exception at the end of the copy process.

    You can set the optional ignore_dangling_symlinks flag to true if you
    want to silence this exception. Notice that this has no effect on
    platforms that don't support os.symlink.

    The optional ignore argument is a callable. If given, it
    is called with the `src` parameter, which is the directory
    being visited by copytree(), and `names` which is the list of
    `src` contents, as returned by os.listdir():

        callable(src, names) -> ignored_names

    Since copytree() is called recursively, the callable will be
    called once for each directory that is copied. It returns a
    list of names relative to the `src` directory that should
    not be copied.

    The optional copy_function argument is a callable that will be used
    to copy each file. It will be called with the source path and the
    destination path as arguments. By default, copy2() is used, but any
    function that supports the same signature (like copy()) can be used.

    If dirs_exist_ok is false (the default) and `dst` already exists, a
    `FileExistsError` is raised. If `dirs_exist_ok` is true, the copying
    operation will continue if it encounters existing directories, and files
    within the `dst` tree will be overwritten by corresponding files from the
    `src` tree.
    """
    sys.audit("shutil.copytree", src, dst)
    with os.scandir(src) as itr:
        entries = list(itr)
    return _copytree(entries=entries, src=src, dst=dst, symlinks=symlinks,
                     ignore=ignore, copy_function=copy_function,
                     ignore_dangling_symlinks=ignore_dangling_symlinks,
                     dirs_exist_ok=dirs_exist_ok,
                     dirs_copystat=dirs_copystat)


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