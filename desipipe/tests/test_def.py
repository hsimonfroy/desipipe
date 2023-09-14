def common1(size):
    import time
    import numpy as np
    time.sleep(3)
    x, y = np.random.uniform(-1, 1, size), np.random.uniform(-1, 1, size)
    return np.sum((x**2 + y**2) < 1.) * 1. / size