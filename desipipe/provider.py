import os
import time
import copy
import random
import subprocess

from .utils import BaseClass


class RegisteredProvider(type(BaseClass)):

    """Metaclass registering :class:`BaseProvider`-derived classes."""

    _registry = {}

    def __new__(meta, name, bases, class_dict):
        cls = super().__new__(meta, name, bases, class_dict)
        meta._registry[cls.name] = cls
        return cls


class BaseProvider(BaseClass, metaclass=RegisteredProvider):

    """Base computing resource provider class, that runs commands on the specific computer / cluster."""

    name = 'base'
    _defaults = dict()

    def __init__(self, environ=None, **kwargs):
        """
        Initialize provider.

        Parameters
        ----------
        environ : BaseEnvironment, str, dict, default=None
            Environment, see :func:`get_environ`.

        **kwargs : dict
            Other attributes, to replace values in :attr:`_defaults`.
        """
        for name, value in self._defaults.items():
            setattr(self, name, copy.copy(value))
        self.update(**{'environ': environ, **kwargs})
        self.processes = []

    @classmethod
    def jobid(cls):
        """Return job ID."""
        return 0

    @classmethod
    def kill(cls, *jobids):
        """Kill input job IDs."""

    def update(self, **kwargs):
        """Update provider with input attributes."""
        for name, value in kwargs.items():
            if name == 'environ':
                from .environment import get_environ
                self.environ = get_environ(value)
            elif name in self._defaults:
                vt = type(self._defaults[name])
                try: value = vt(value)
                except TypeError: pass
                setattr(self, name, value)
            else:
                raise ValueError('Unknown argument {}; supports {}'.format(name, list(self._defaults)))

    def jobids(self):
        """Current job IDs."""
        return []

    def nrunning(self):
        """Number of running workers."""
        return 0

    def cost(self, workers=1):
        """
        Compute cost associated to the input number of workers (in addition to running ones).
        Only cost variations matter (not the absolute cost):
        constant cost triggers more workers, increasing cost penalizes more workers.
        """
        return 0

    def __call__(self, cmd, workers=1):
        """Submit input command ``cmd`` on ``workers`` workers."""
        raise NotImplementedError


def get_provider(provider=None, **kwargs):
    """
    Convenient function that returns the provider.

    Parameters
    ----------
    provider : BaseProvider, str, dict, default=None
        A :class:`BaseProvider` instance, which is then returned directly,
        a string specifying the name of the provider (e.g. 'local')
        or a dictionary of provider attributes.
        If not specified, the default provider in desipipe's configuration
        (see :class:`Config`) is used if provided, else 'local'.

    **kwargs : dict
        Optionally, additional provider attributes.

    Returns
    -------
    provider : BaseProvider
    """
    if isinstance(provider, BaseProvider):
        return provider
    if isinstance(provider, dict):
        provider, kwargs = provider.pop('provider', None), {**provider, **kwargs}
    if provider is None:
        from .config import Config
        provider = Config().get('provider', 'local')
    return BaseProvider._registry[provider](**kwargs)


Provider = get_provider


class LocalProvider(BaseProvider):
    """
    Local provider: input commands are executed as subprocesses.

    Parameters
    ----------
    mpiprocs_per_worker : int, default=1
        Number of MPI processes per worker.

    mpiexec : str, default='mpiexec -np {mpiprocs:d}'
        Template to run a command with MPI.
    """
    name = 'local'
    _defaults = dict(mpiprocs_per_worker=1, mpiexec='mpiexec -np {mpiprocs:d} {cmd}')

    @classmethod
    def jobid(cls):
        """Return job ID."""
        return os.getpid()

    @classmethod
    def kill(cls, *jobids):
        """Kill input job IDs."""
        import signal
        for jobid in jobids:
            try:
                os.kill(int(jobid), signal.SIGTERM)
            except ProcessLookupError:
                pass

    def __call__(self, cmd, workers=1):
        """Submit input command ``cmd`` on ``workers`` workers."""
        environ = {**os.environ, **self.environ.to_dict(all=True)}
        for worker in range(workers):
            tmp = cmd
            if self.mpiprocs_per_worker > 1:
                tmp = self.mpiexec.format(mpiprocs=self.mpiprocs_per_worker, cmd=tmp)
            # self.processes.append(subprocess.Popen(tmp.split(' ')))
            self.processes.append(subprocess.Popen(tmp.split(' '), start_new_session=True, env=environ))
            time.sleep(random.uniform(0.8, 1.2))

    def nrunning(self):
        """Number of running workers."""
        return sum(process.poll() is None for process in self.processes)

    def cost(self, workers=1):
        """
        Compute cost associated to the input number of workers (in addition to running ones).
        Cost is constant, then increases steeply when
        the total number of processes (workers times MPI) reaches the number of CPU counts.
        """
        nprocs = workers * self.mpiprocs_per_worker + self.nrunning()
        ncpus = os.cpu_count()
        if nprocs < ncpus:
            return 0.
        if nprocs == ncpus:
            return 5.
        return 10.


class SlurmProvider(BaseProvider):
    """
    Slurm provider: input commands are submitted as Slurm jobs.

    Parameters
    ----------
    account : str, default='desi'
        Account.

    constraint : str, default='cpu'
        Run on CPU ('cpu') or GPU ('gpu') nodes.

    queue : str, default='regular'
        Name of job queue.

    time : str, default='01:00:00'
        Time to be allocated for a particuler worker.
        Best is to take the estimated time for the task (with some margin),
        or an integer times this time (in case of short tasks).

    nodes_per_workers : float, default=1.
        Number of nodes to reserve for each worker; must be > 0.
        If a float, workers are piled up as MPI processes on the number of nodes :meth:`nodes`.

    mpiprocs_per_worker : int, default=1
        Number of MPI processes per worker.

    mpiexec : str, default='srun -N {nodes:d} -n {mpiprocs:d} --cpu-bind=cores {cmd}'
        Template to run a command with MPI.
    """
    name = 'slurm'
    _defaults = dict(account='desi', constraint='cpu', qos='regular', time='01:00:00', nodes_per_worker=1., mpiprocs_per_worker=1,
                     output='/dev/null', error='/dev/null', mpiexec='srun --unbuffered -N {nodes:d} -n {mpiprocs:d} {cmd}', signal='SIGTERM@30',
                     killed_at_timeout=None, kwargs=dict())

    @classmethod
    def jobid(cls):
        """Return job ID."""
        return os.environ.get('SLURM_JOB_ID', '')

    @classmethod
    def kill(cls, *jobids):
        """Kill input job IDs."""
        for jobid in jobids:
            subprocess.run(['scancel', str(jobid)])

    def __call__(self, cmd, workers=1):
        """Submit input command ``cmd`` on ``workers`` workers."""
        if self.nodes_per_worker <= 0.:
            raise ValueError('Cannot set nodes_per_worker <= 0.')
        nodes = self.nodes(workers=workers)
        if int(self.nodes_per_worker) != self.nodes_per_worker:  # stack jobs
            cmd = self.mpiexec.format(nodes=nodes, mpiprocs=self.mpiprocs_per_worker * workers, cmd=cmd) + ' --mpisplits {:d}'.format(workers)
        else:
            cmd = [self.mpiexec.format(nodes=int(self.nodes_per_worker), mpiprocs=self.mpiprocs_per_worker, cmd=cmd)] * workers
            cmd = ' & '.join(cmd)
            if workers: cmd += ' & wait'
        cmd = self.environ.to_script(sep=' ; ') + ' ; ' + cmd
        kwargs = []
        for name, value in self.kwargs.items(): kwargs += ['--{}'.format(name), str(value)]
        # -- parsable to get jobid (optionally, cluster name)
        # -- wrap to pass the job
        cmd = ['sbatch', '--output', self.output, '--error', self.error, '--account', str(self.account), '--constraint', str(self.constraint), '--qos', str(self.qos), '--time', str(self.time), '--nodes', str(nodes), '--signal', str(self.signal), '--parsable'] + kwargs + ['--wrap', cmd]
        proc = subprocess.run(cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE, text=True)
        self.processes.append((proc.stdout.split(',')[0].strip(), nodes, workers))  # jobid, workers

    def jobids(self):
        """Current job IDs."""
        return [process[0] for process in self.processes]

    def nrunning(self, of='workers'):
        """Number of running workers."""
        allowed_of = ['workers', 'nodes']
        if of not in allowed_of:
            raise ValueError('of must be one of {}, found {}'.format(allowed_of, of))
        try:
            sqs = subprocess.run(['sqs'], check=True, stdout=subprocess.PIPE, text=True).stdout.split('\n')
        except subprocess.CalledProcessError:
            jobids = getattr(self, '_jobids', [])
        else:
            istate = sqs[0].index('ST')
            jobids = []
            for line in sqs[1:]:
                if line:
                    state = line[istate:]
                    if not state.startswith('CG'):
                        jobids.append(line.split()[0].strip())
            self._jobids = jobids
        # print(jobids, self.processes)
        if of == 'workers':
            return sum(workers * (jobid in jobids) for jobid, nodes, workers in self.processes)
        return sum(nodes * (jobid in jobids) for jobid, nodes, workers in self.processes)

    def nodes(self, workers=1):
        """
        Number of nodes required for input number of workers,
        computed as the nearest greater integer of :attr:`nodes_per_worker` and ``workers``.
        """
        import math
        return math.ceil(self.nodes_per_worker * workers)

    def cost(self, workers=1):
        """Cost required for input number of workers (in addition to running ones)."""
        return self.nodes(workers=workers) + self.nrunning(of='nodes')


class NERSCProvider(SlurmProvider):
    """
    Slurm provider on NERSC: same as :class:`SlurmProvider`,
    but taking into account NERSC specificities:

    - maximum number of MPI processes on a node of 128 (which sets a minimum value on :attr:`nodes_per_worker` given :attr:`mpiprocs_per_worker`)
    - cost increases linearly after :attr:`threshold_nodes` are allocated.

    Parameters
    ----------
    threshold_nodes : int, default=10
        Below this number of allocated nodes, cost is constant.
        Beyond this number of allocated nodes, cost increases linearly.
    """
    name = 'nersc'
    _defaults = {**SlurmProvider._defaults, 'threshold_nodes': 1}  # threshold_nodes = 1: favors 1-node jobs

    def __init__(self, *args, **kwargs):
        super(NERSCProvider, self).__init__(*args, **kwargs)
        self.max_mpiprocs_per_node = 128
        self.nodes_per_worker = max(self.nodes_per_worker, self.mpiprocs_per_worker * 1. / self.max_mpiprocs_per_node)
        if self.constraint == 'gpu':
            if not self.account.endswith('_g'): self.account += '_g'
            if 'gpus-per-node' not in self.kwargs: self.kwargs['gpus-per-node'] = 4

    def cost(self, workers=1):
        """Cost required for input number of workers (in addition to running ones)."""
        nodes = self.nodes(workers=workers) + self.nrunning(of='nodes')
        if nodes < self.threshold_nodes:
            return 0
        return nodes - self.threshold_nodes