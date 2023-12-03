import copy
from .utils import BaseClass


class RegisteredScheduler(type(BaseClass)):

    """Metaclass registering :class:`BaseScheduler`-derived classes."""

    _registry = {}

    def __new__(meta, name, bases, class_dict):
        cls = super().__new__(meta, name, bases, class_dict)
        meta._registry[cls.name] = cls
        return cls


class BaseScheduler(BaseClass, metaclass=RegisteredScheduler):

    """Base scheduler class, which invokes computing resource providers."""

    name = 'base'
    _defaults = dict()

    def __init__(self, provider=None, **kwargs):
        """
        Initialize :class:`BaseScheduler`.

        Parameters
        ----------
        provider : BaseProvider
            Provider instance, that takes care of executing a command.

        **kwargs : dict
            Other attributes, to replace values in :attr:`_defaults`.
        """
        for name, value in self._defaults.items():
            setattr(self, name, copy.copy(value))
        self.update(**{'provider': provider, **kwargs})

    def update(self, **kwargs):
        """Update scheduler with input attributes."""
        if 'provider' in kwargs:
            self.provider = kwargs.pop('provider', None)
        for name, value in kwargs.items():
            if name in self._defaults:
                vt = type(self._defaults[name])
                try: value = vt(value)
                except TypeError: pass
                setattr(self, name, value)
            else:
                raise ValueError('Unrecognized argument {}; supports {}'.format(name, list(self._defaults)))

    def __call__(self, cmd, ntasks=None):
        """Schedule input command ``cmd``."""
        raise NotImplementedError


def get_scheduler(scheduler=None, **kwargs):
    """
    Convenient function that returns the scheduler.

    Parameters
    ----------
    scheduler : BaseScheduler, str, dict, default=None
        A :class:`BaseScheduler` instance, which is then returned directly,
        a string specifying the name of the scheduler (e.g. 'simple')
        or a dictionary of scheduler attributes.
        If not specified, the default scheduler in desipipe's configuration
        (see :class:`Config`) is used if provided, else 'simple'.

    **kwargs : dict
        Optionally, additional scheduler attributes.

    Returns
    -------
    scheduler : BaseScheduler
    """
    if isinstance(scheduler, BaseScheduler):
        return scheduler
    if isinstance(scheduler, dict):
        scheduler, kwargs = scheduler.pop('scheduler', None), {**scheduler, **kwargs}
    if scheduler is None:
        from .config import Config
        scheduler = Config().get('scheduler', 'simple')
    return BaseScheduler._registry[scheduler](**kwargs)


Scheduler = get_scheduler


class SimpleScheduler(BaseScheduler):
    """
    Simple scheduler that chooses the optimal number of workers to minimize their :meth:`cost`,
    while maintaining the current number of workers below :attr:`max_workers`.

    Parameters
    ----------
    max_workers : int, default=1
        Maximum number of workers.
    """
    name = 'simple'
    _defaults = dict(max_workers=1)

    def __call__(self, cmd, ntasks=None):
        if ntasks is None: ntasks = self.max_workers
        ntasks = ntasks - self.provider.nworkers(state='PENDING')
        if ntasks == 0:
            return 0
        # Too many jobs launched, let's kill some
        if ntasks < 0:
            nkill = 0
            tokill = []
            for jobid, nworkers in self.provider.jobids(state='PENDING', return_nworkers=True):
                if nkill + nworkers <= abs(ntasks) and jobid is not None:
                    tokill.append(jobid)
                    nkill += nworkers
            self.provider.kill(*tokill)
            return -nkill

        max_workers = min(ntasks, self.max_workers)
        best_workers, best_cost = 0, float('inf')
        for workers in range(1, max_workers + 1):
            # print(workers, self.provider.nworkers(), self.max_workers)
            if (workers + self.provider.nworkers()) > self.max_workers:
                break
            cost = self.provider.cost(workers=workers)
            if cost <= best_cost:
                best_workers, best_cost = workers, cost
        if best_workers:
            self.provider(cmd, workers=best_workers)
        return best_workers