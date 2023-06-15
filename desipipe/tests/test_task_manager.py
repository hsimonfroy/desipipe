import time

from desipipe import Queue, Environment, TaskManager, FileManager


def test_app():

    from desipipe.task_manager import PythonApp

    def func(a, b):
        return a * b

    app = PythonApp(func)
    print(app.run((1, 1), {})[2])


def test_queue():

    queue = Queue('test', base_dir='_tests', spawn=1)
    tm = TaskManager(queue, environ=Environment(), scheduler=dict(max_workers=10))

    @tm.python_app
    def fraction(size=10000):
        import time
        import numpy as np
        time.sleep(5)
        x, y = np.random.uniform(-1, 1, size), np.random.uniform(-1, 1, size)
        return np.sum((x**2 + y**2) < 1.) * 1. / size

    @tm.clone(scheduler=dict(max_workers=1)).python_app
    def average(fractions):
        import numpy as np
        return np.average(fractions) * 4.

    t0 = time.time()
    fractions = [fraction(size=1000 + i) for i in range(20)]
    print(average(fractions).result(), time.time() - t0)


def test_cmdline():

    import subprocess
    queue = "'./_tests/*'"
    queue_single = "./_tests/test.sqlite"
    subprocess.call(['desipipe', 'queues', '-q', queue])
    subprocess.call(['desipipe', 'tasks', '-q', queue_single, '--state', 'SUCCEEDED'])
    subprocess.call(['desipipe', 'delete', '-q', queue])
    subprocess.call(['desipipe', 'pause', '-q', queue])
    subprocess.call(['desipipe', 'resume', '-q', queue])
    subprocess.call(['desipipe', 'spawn', '-q', queue])
    subprocess.call(['desipipe', 'retry', '-q', queue, '--state', 'SUCCEEDED', '--spawn'])


if __name__ == '__main__':

    #test_app()
    test_queue()
    #test_cmdline()