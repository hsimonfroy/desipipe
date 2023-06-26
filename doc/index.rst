.. title:: desipipe docs

**************************************
Welcome to desipipe's documentation!
**************************************

.. toctree::
  :maxdepth: 1
  :caption: User documentation

  user/building
  api/api

.. toctree::
  :maxdepth: 1
  :caption: Developer documentation

  developer/documentation
  developer/tests
  developer/contributing
  developer/changes

.. toctree::
  :hidden:

************
Introduction
************

**desipipe** is an attempt to provide a common framework for the processing and file management of DESI clustering analyses.

In terms of capabilities, **desipipe** includes:

* a file management system
* a task management system that takes plain Python functions and handles dependencies
* bookkeeping of script and module versions

Example scripts and notebooks are provided in :root:`desipipe/examples` and :root:`desipipe/nb`.

The goal is to be able to write a full processing pipeline within one Python script / notebook,
which then serves as a pipeline documentation. Here is an example, estimating :math:`\pi`.

.. code-block:: python


  from desipipe import Queue, Environment, TaskManager, FileManager

  # Let's instantiate a Queue, which records all tasks to be performed
  # spawn=True means a manager process is spawned to distribute the tasks among workers
  # spawn=False only updates the queue, but no other process to run the tasks is spawned
  # That can be updated afterwards, with e.g. the command line:
  # desipipe spawn -q ./_tests/test --spawn
  queue = Queue('test', base_dir='_tests', spawn=True)
  # Pool of 4 workers
  # Any environment variable can be passed to Environment: it will be set when running the tasks below
  tm = TaskManager(queue, environ=Environment(), scheduler=dict(max_workers=4))

  # We decorate the function (task) with tm.python_app
  @tm.python_app
  def fraction(seed=42, size=10000):
      import time
      import numpy as np
      time.sleep(5)  # wait 5 seconds, just to show jobs are indeed run in parallel
      x, y = np.random.uniform(-1, 1, size), np.random.uniform(-1, 1, size)
      return np.sum((x**2 + y**2) < 1.) * 1. / size

  # Here we use another task manager, with only 1 worker
  tm2 = tm.clone(scheduler=dict(max_workers=1))
  @tm2.python_app  # the two lines above can be on the same line in Python >= 3.9
  def average(fractions):
      import numpy as np
      return np.average(fractions) * 4.

  # Let's add another task, to be run in a shell
  @tm2.bash_app
  def echo(avg):
      return ['echo', '-n', 'bash app says pi is ~ {:.4f}'.format(avg)]

  # The following line stacks all the tasks in the queue
  fractions = [fraction(seed=i) for i in range(20)]
  # fractions is a list of Future instances
  # We can pass them to other tasks, which creates a dependency graph
  avg = average(fractions)
  ech = echo(avg)
  # At this point jobs are submitted
  print('Elapsed time: {:.4f}'.format(time.time() - t0))


**************
Code structure
**************

The code structure is the following:

  - config.py implements the **desipipe** configuration
  - environment.py implements the various computing environments
  - file_manager.py implements the file management
  - io.py implements file input/outputs
  - task_manager.py implements the task management
  - scheduler.py implements job schedulers to bused by the task management
  - provider.py implements computing resource providers to be used by the task management
  - utils.py implements some utilities

Changelog
=========

* :doc:`developer/changes`

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
