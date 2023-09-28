import os
import time

os.environ['OPENBLAS_NUM_THREADS'] = '1'
os.environ['MKL_NUM_THREADS'] = '1'
import numpy as np

t0 = time.time()
shape = (500, 500)
for i in range(200):
    np.linalg.inv(np.random.uniform(0., 1., shape))
print('TERMINATED', time.time() - t0)