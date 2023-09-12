.. title:: desipipe docs

**************************************
Welcome to desipipe's documentation!
**************************************

.. toctree::
  :maxdepth: 1
  :caption: User documentation

  user/building
  user/getting_started
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
which then serves as a pipeline documentation.

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
