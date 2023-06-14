from desipipe import Queue, Environment, TaskManager, FileManager


def test_app():

    from desipipe.task_manager import PythonApp

    def func(a, b):
        return a * b

    app = PythonApp(func)
    print(app.run((1, 1), {})[2])


def test_queue():

    queue = Queue('test', base_dir='_tests', spawn=True)
    tm = TaskManager(queue, environ=Environment(), scheduler=dict(max_workers=4))

    @tm.python_app
    def fraction(size=1000):
        import numpy as np
        x, y = np.random.uniform(-1, 1, size), np.random.uniform(-1, 1, size)
        return np.sum((x**2 + y**2) < 1.) * 1. / size

    @tm.clone(scheduler=dict(max_workers=1)).python_app
    def average(*fractions):
        import numpy as np
        return np.average(fractions) * 4.

    fractions = [fraction(size=1000 + i) for i in range(40)]
    print(average(*fractions).result())


if __name__ == '__main__':

    #test_app()
    test_queue()