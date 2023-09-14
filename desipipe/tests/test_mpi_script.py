#!/usr/bin/env python
import time
from mpi4py import MPI

try:
    raise MPI.Exception
    mpicomm = MPI.Comm.Get_parent()
    mpicomm.size
except MPI.Exception:
    mpicomm = MPI.COMM_WORLD
    size = mpicomm.Get_size()

rank = mpicomm.Get_rank()
print('START')
time.sleep(3)
print(rank, mpicomm.size)