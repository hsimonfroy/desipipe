import uuid
import pickle
import hashlib


def func(a, b, c=2):
    toret = a * b * c
    return toret


def test_uid():

    uid = pickle.dumps((func, (1, 2), {'c': 3}))
    hex = hashlib.md5(uid).hexdigest()
    id = uuid.UUID(hex=hex)
    print(id)


def test_subprocess():
    import os
    import subprocess
    proc = subprocess.Popen('mpiexec -np 1 python test_mpi.py'.split(), env={**os.environ, 'JOBID': 'TEST'})
    out, err = proc.communicate()
    print(out)


if __name__ == '__main__':

    #test_uid()
    test_subprocess()