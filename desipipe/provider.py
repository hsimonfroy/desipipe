import os
import time
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

    name = 'base'
    _defaults = dict()

    def __init__(self, **kwargs):
        self.__dict__.update(self._defaults)
        self.update(**kwargs)
        self.processes = []

    def update(self, **kwargs):
        for name, value in kwargs.items():
            if name in self._defaults:
                setattr(self, name, type(self._defaults[name])(value))
            else:
                raise ValueError('Unknown argument {}; supports {}'.format(name, list(self._defaults)))

    def nrunning(self):
        return 0

    def cost(self, workers=1):
        return 0


def get_provider(provider=None, **kwargs):
    if provider is None:
        from .config import Config
        provider = Config().get('provider', 'local')
    if isinstance(provider, BaseProvider):
        return provider
    return BaseProvider._registry[provider](**kwargs)


Provider = get_provider


class LocalProvider(BaseProvider):

    name = 'local'
    _defaults = dict(mpiprocs_per_worker=1, mpiexec='mpiexec -np {mpiprocs_per_worker:d} {cmd}')

    def __call__(self, cmd, workers=1):
        for worker in range(workers):
            self.processes.append(subprocess.Popen(self.mpiexec.format(mpiprocs_per_worker=self.mpiprocs_per_worker, cmd=cmd), start_new_session=True))
            time.sleep(random.uniform(0.8, 1.2))

    def nrunning(self):
        return sum(process.poll() is None for process in self.processes)

    def cost(self, workers=1):
        nprocs = (workers + self.nrunning()) * self.mpiprocs_per_worker
        ncpus = os.cpu_count()
        if nprocs < ncpus:
            return 0.
        if nprocs == ncpus:
            return 5.
        return 10.


class SlurmProvider(BaseProvider):

    name = 'slurm'
    _defaults = dict(account='desi', constraint='cpu', queue='regular', time='01:00:00', workers=1, min_nodes_per_worker=1, mpiprocs_per_worker=1,
                     mpiexec='srun -N {nodes_per_worker:d} -n {mpiprocs_per_worker:d} --cpu-bind=cores {cmd}', out='out_%x_%j.txt', err='err_%x_%j.txt')

    def __call__(self, cmd, workers=1):
        nodes = self.min_nodes_per_worker * workers
        nodes_per_worker = self.min_nodes_per_worker
        cmd = [self.mpiexec.format(nodes_per_worker=nodes_per_worker, mpiprocs_per_worker=self.mpiprocs_per_worker, cmd=cmd)] * workers
        cmd = ' & '.join(cmd) + '& wait'
        # --parsable to get jobid (optionally, cluster name)
        # -- wrap to pass the job
        cmd = f'sbatch --account {self.account} --constraint {self.contraint} --queue {self.queue} --time {self.time} --nodes {nodes:d} --parsable --wrap "{cmd}"'
        proc = subprocess.Popen(cmd, shell=True)
        out, err = proc.communicate()
        self.processes.append(out.split(',')[0])  # jobid

    def nrunning(self):
        jobids = [line.split()[0] for line in subprocess.check_output(['sqs']).split('\n')]
        return sum(jobid in jobids for jobid in self.processes)

    def cost(self, workers=1):
        return self.min_nodes_per_worker * workers


class NERSCProvider(SlurmProvider):

    name = 'nersc'

    def __init__(self, *args, **kwargs):
        super(NERSCProvider, self).__init__(*args, **kwargs)
        self.max_mpiprocs_per_node = 128
        self.min_nodes_per_worker = max(self.min_nodes_per_worker, (self.mpiprocs_per_worker + self.max_mpiprocs_per_node - 1) // self.max_mpiprocs_per_node)
