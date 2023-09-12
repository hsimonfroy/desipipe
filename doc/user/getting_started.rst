.. _user-getting-started:


Getting started
===============

In this page we will describe **desipipe**'s basics' with a practical example,
but further examples can be found in the provided `notebooks <https://github.com/cosmodesi/desipipe/blob/main/nb>`_.

**desipipe** provides a framework to organize the processing of your analysis.

Write tasks
-----------
The point of **desipipe** is to write all your tasks and resource requests within one Python script.
Let's consider a simple example: the Monte-Carlo estimation of :math:`\pi`.

.. code-block:: python

  def draw_random_numbers(size):
      import numpy as np
      return np.random.uniform(-1, 1, size)

  def fraction(seed=42, size=10000, draw_random_numbers=draw_random_numbers):
      # All definitions, except input parameters, must be in the function itself, or in its arguments
      # and this, recursively:
      # draw_random_numbers is defined above and all definitions, except input parameters, are in the function itself
      # This is required for the tasks to be pickelable (~ can be converted to bytes)
      import time
      import numpy as np
      time.sleep(5)  # wait 5 seconds, just to show jobs are indeed run in parallel
      x, y = draw_random_numbers(size), draw_random_numbers(size)
      return np.sum((x**2 + y**2) < 1.) * 1. / size  # fraction of points in the inner circle of radius 1

  def average(fractions):
      import numpy as np
      return np.average(fractions) * 4.


This is pretty close to what one would have written in Python, except we must take care of including all definitions in the functions to be run.
Typically, modules should be imported in the function, and if there is dependence in other functions, they should be included in the signature (input arguments),
this is the case of ``draw_random_numbers`` here.

Now, to get a reliable estimate of :math:`\pi`, we want to repeate the simulation ``fraction`` many times, and if possible, in parallel.
Let's specify this! We create a queue, that will contain all our tasks (here one task = one ``fraction`` call).

.. code-block:: python

  from desipipe import Queue, Environment, TaskManager, FileManager

  # Let's instantiate a Queue, which records all tasks to be performed
  queue = Queue('test', base_dir='_tests')
  # Pool of 4 workers
  # Any environment variable can be passed to Environment: it will be set when running the tasks below
  tm = TaskManager(queue, environ=Environment(), scheduler=dict(max_workers=4))

  # Left untouched, this is a helper function, not a standalone task
  def draw_random_numbers(size):
      import numpy as np
      return np.random.uniform(-1, 1, size)

  # We decorate the function (task) with tm.python_app
  @tm.python_app
  def fraction(seed=42, size=10000, draw_random_numbers=draw_random_numbers):
      # All definitions, except input parameters, must be in the function itself, or in its arguments
      # and this, recursively:
      # draw_random_numbers is defined above and all definitions, except input parameters, are in the function itself
      # This is required for the tasks to be pickelable (~ can be converted to bytes)
      import time
      time.sleep(5)  # wait 5 seconds, just to show jobs are indeed run in parallel
      x, y = draw_random_numbers(size), draw_random_numbers(size)
      return np.sum((x**2 + y**2) < 1.) * 1. / size  # fraction of points in the inner circle of radius 1

  # Here we use another task manager, with only 1 worker
  tm2 = tm.clone(scheduler=dict(max_workers=1))
  @tm2.python_app
  def average(fractions):
      import numpy as np
      return np.average(fractions) * 4.

  # Let's add another task, to be run with bash
  @tm2.bash_app
  def echo(avg):
      return ['echo', '-n', 'bash app says pi is ~ {:.4f}'.format(avg)]

  # The following line stacks all the tasks in the queue
  fractions = [fraction(seed=i) for i in range(20)]
  # fractions is a list of Future instances
  # We can pass them to other tasks, which creates a dependency graph
  avg = average(fractions)
  ech = echo(avg)

The script above stacks all tasks in the queue. ``fraction`` tasks will be 'PENDING' (waiting to be run),
while ``average`` tasks will be 'WAITING' for the former to complete. ``echo`` also depends on ``average``.
Running the script above will write a :class:`desipipe.Queue` on disk, with name 'test', in the directory ``_tests``
(by default, it is ``${HOME}/.desipipe/queues/${USERNAME}/``).

Now, we can spawn a manager process that will run the above tasks, following the specifications of the task managers.


.. code-block:: python

  spawn(queue)


One can interact with ``queue`` from python directly, e.g.: :meth:`Queue.tasks` to list tasks,
:meth:`Queue.pause` to pause the queue, :meth:`Queue.resume` to resume the queue, etc.
Usually though, one will use the command line: see below.

Cheat list
----------

Spawn a manager process
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

  desipipe spawn -q my_queue

The process will distribute the tasks among workers, using the scheduler and provider defined above.

Print queues
~~~~~~~~~~~~

.. code-block:: bash

  desipipe queues -q '*/*'

Print tasks in queue
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash

  desipipe tasks -q my_queue

Task state can be:
- 'WAITING':  Waiting for requirements (other tasks) to finish
- 'PENDING':  Eligible to be selected and run
- 'RUNNING':  Running right now
- 'SUCCEEDED':  Finished with errno = 0
- 'FAILED':  Finished with errno != 0

Pause a queue
~~~~~~~~~~~~~

.. code-block:: bash

  desipipe pause -q my_queue

When pausing a queue, all processes running tasks from this queue will stop (after they finish their current task).

Resume a queue
~~~~~~~~~~~~~~

.. code-block:: bash

  desipipe resume -q my_queue   # pass --spawn to spawn a manager process that will distribute the tasks among workers

When resuming a queue, tasks can be processed.

Retry tasks
~~~~~~~~~~~

.. code-block:: bash

  desipipe retry -q my_queue

Task state is changed to 'PENDING', i.e. they will be processed again.

Kill tasks
~~~~~~~~~~

.. code-block:: bash

  desipipe kill -q my_queue

Kills running tasks.

Delete queue(s)
~~~~~~~~~~~~~~~

.. code-block:: bash

  desipipe delete -q 'my_*'  # pass --force to actually delete the queue