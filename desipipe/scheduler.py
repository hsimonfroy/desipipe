from .utils import BaseClass


class RegisteredScheduler(type(BaseClass)):

    """Metaclass registering :class:`BaseScheduler`-derived classes."""

    _registry = {}

    def __new__(meta, name, bases, class_dict):
        cls = super().__new__(meta, name, bases, class_dict)
        meta._registry[cls.name] = cls
        return cls


class BaseScheduler(BaseClass, metaclass=RegisteredScheduler):

    name = 'base'
    _defaults = dict()

    def __init__(self, provider=None, **kwargs):
        self.update(**{'provider': provider, **self._defaults, **kwargs})

    def update(self, **kwargs):
        if 'provider' in kwargs:
            self.provider = kwargs.pop('provider', None)
        for name, value in kwargs.items():
            if name in self._defaults:
                setattr(self, name, type(self._defaults[name])(value))
            else:
                raise ValueError('Unknown argument {}; supports {}'.format(name, list(self._defaults)))


def get_scheduler(scheduler=None, **kwargs):
    if scheduler is None:
        from .config import Config
        scheduler = Config().get('scheduler', 'simple')
    if isinstance(scheduler, BaseScheduler):
        return scheduler
    return BaseScheduler._registry[scheduler](**kwargs)


Scheduler = get_scheduler


class SimpleScheduler(BaseClass):

    name = 'simple'
    _defaults = dict(max_workers=1)

    def __call__(self, cmd, ntasks=None):
        if ntasks is None:
            ntasks = 1
        max_workers = min(ntasks, self.max_workers - self.provider.nrunning())
        best_workers, best_cost = 0, float('inf')
        for workers in range(1, max_workers + 1):
            cost = self.provider.cost(workers=workers)
            if cost < best_cost:
                best_workers, best_cost = workers, cost
        if best_workers:
            self.provider(cmd, workers=best_workers)