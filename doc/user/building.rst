.. _user-building:

Building
========

Requirements
------------
Only requirements are:

  - pyyaml
  - mpi4py

pip
---
To install **desipipe**, simply run::

  python -m pip install git+https://github.com/cosmodesi/desipipe

git
---

First::

  git clone https://github.com/cosmodesi/desipipe.git

To install the code::

  python setup.py install --user

Or in development mode (any change to Python code will take place immediately)::

  python setup.py develop --user