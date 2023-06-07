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

    def __init__(self, environ=None, **kwargs):
        self.update(**{'environ': environ, **self._defaults, **kwargs})
        self.processes = []

    def update(self, **kwargs):
        for name, value in kwargs.items():
            if name == 'environ':
                from .environment import get_environ
                self.environ = get_environ(value)
            elif name in self._defaults:
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
    _defaults = dict(mpiprocs_per_worker=1, mpiexec='mpiexec -np {mpiprocs:d} {cmd}')

    def __call__(self, cmd, workers=1):
        for worker in range(workers):
            cmd = self.mpiexec.format(mpiprocs=self.mpiprocs_per_worker, cmd=cmd)
            self.processes.append(subprocess.Popen(cmd, start_new_session=True, env={**os.environ, **self.environ.all()}))
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
    _defaults = dict(account='desi', constraint='cpu', queue='regular', time='01:00:00', workers=1, nodes_per_worker=1., mpiprocs_per_worker=1,
                     mpiexec='srun -N {nodes:d} -n {mpiprocs:d} --cpu-bind=cores {cmd}', out='out_%x_%j.txt', err='err_%x_%j.txt')

    def __call__(self, cmd, workers=1):
        if self.nodes_per_worker <= 0.:
            raise ValueError('Cannot set nodes_per_worker <= 0.')
        nodes = self.nodes(workers=workers)
        if int(self.nodes_per_worker) != self.nodes_per_worker:  # stack jobs
            cmd = 'desipipe-mpispawn --nprocs {} {}'.format(' '.join([self.mpiprocs_per_worker] * workers), cmd)
            cmd = self.mpiexec.format(nodes_per_worker=1, mpiprocs_per_worker=1, cmd=cmd)
        else:
            cmd = [self.mpiexec.format(nodes=self.nodes_per_worker, mpiprocs=self.mpiprocs_per_worker, cmd=cmd)] * workers
            cmd = ' & '.join(cmd) + '& wait'
        cmd = self.environ.as_script() + cmd
        # --parsable to get jobid (optionally, cluster name)
        # -- wrap to pass the job
        cmd = f'sbatch --account {self.account} --constraint {self.contraint} --queue {self.queue} --time {self.time} --nodes {nodes:d} --parsable --wrap "{cmd}"'
        proc = subprocess.Popen(cmd, shell=True)
        out, err = proc.communicate()
        self.processes.append(out.split(',')[0])  # jobid

    def nrunning(self):
        jobids = [line.split()[0] for line in subprocess.check_output(['sqs']).split('\n')]
        return sum(jobid in jobids for jobid in self.processes)

    def nodes(self, workers=1):
        import math
        return math.ceil(self.nodes_per_worker * workers)

    def cost(self, workers=1):
        return self.nodes(workers=workers)


class NERSCProvider(SlurmProvider):

    name = 'nersc'

    def __init__(self, *args, **kwargs):
        super(NERSCProvider, self).__init__(*args, **kwargs)
        self.max_mpiprocs_per_node = 128
        self.nodes_per_worker = max(self.nodes_per_worker, self.mpiprocs_per_worker * 1. / self.max_mpiprocs_per_node)


import sys
import argparse

from mpi4py import MPI


def mpi_spawn(args=None):

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('-n', '--nprocs', type=int, nargs='+', required=True, help='Number of processes for each task')
    parser.add_argument('cmds', metavar='cmd', type=str, nargs='+', required=True, help='revisions')
    args = parser.parse_args(args=args)
    cmds = args.cmds
    if len(cmds) != args.nprocs:
        if len(cmds) == 1:
            cmds = cmds * args.nprocs
        else:
            raise ValueError('Provide as many commands as nprocs')
    for cmd, nprocs in zip(cmds, args.nprocs):
        MPI.COMM_SELF.Spawn(sys.executable, args=cmd, maxprocs=nprocs)