from desipipe import Queue, Environment, TaskManager, spawn, setup_logging

setup_logging()

queue = Queue('test')
queue.clear()

environ = Environment()
tm = TaskManager(queue=queue, environ=environ)
tm_test = tm.clone(scheduler=dict(max_workers=1))


@tm_test.bash_app
def test(i):
    return ['python', 'test_mpi_script.py', i]


if __name__ == '__main__':

    setup_logging()
    for i in range(10):
        test(i)