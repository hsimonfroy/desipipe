import os
import re
import io
import sys
import contextlib
import time
import random
import uuid
import signal
import pickle
import subprocess
import traceback
import logging
import argparse
import hashlib
import inspect
import glob
import textwrap
import copyreg
import shutil

import sqlite3

from . import utils
from .utils import BaseClass
from .config import Config
from .environment import get_environ, change_environ
from .scheduler import get_scheduler
from .provider import get_provider


task_states = ['WAITING',       # Waiting for requires to finish
               'PENDING',       # Eligible to be selected and run
               'RUNNING',       # Running right now
               'SUCCEEDED',     # Finished with err code = 0
               'FAILED',        # Finished with err code != 0
               'KILLED',        # Finished with SIGTERM (eg Slurm job timeout)
               'UNKNOWN']       # Something went wrong and we lost track


TaskState = type('TaskState', (), {**dict(zip(task_states, task_states)), 'ALL': task_states})



def reduce_app(self):
    """Special reduce method for :class:`BaseApp`, dropping :attr:`task_manager` and :attr:`file`."""
    state = self.__getstate__()
    state['task_manager'] = state['file'] = None
    return (self.__class__.__new__, (self.__class__,), state)


class QueuePickler(pickle.Pickler):

    """Special pickler for queue, handling :class:`BaseApp` and :class:`Future` instances."""

    def __init__(self, *args, **kwargs):
        """Initialize pickler and add the special reduce method for :class:`BaseApp` to the :attr:`dispatch_table`."""
        super(QueuePickler, self).__init__(*args, **kwargs)
        self.dispatch_table = copyreg.dispatch_table.copy()
        for cls in BaseApp.__subclasses__():
            self.dispatch_table[cls] = reduce_app

    def persistent_id(self, obj):
        # Instead of pickling obj as a regular class instance, we emit a persistent ID.
        if isinstance(obj, Future):
            # Here, our persistent ID is simply a tuple, containing a tag and a
            # key, which refers to a specific record in the database.
            ids = getattr(self, 'future_ids', [])
            ids.append(obj.id)
            setattr(self, 'future_ids', ids)
            return ('Future', obj.id)
        # If obj does not have a persistent ID, return None. This means obj
        # needs to be pickled as usual.
        return None

    @classmethod
    def dumps(cls, obj, *args, **kwargs):
        """
        Dump input ``obj`` to string.
        args and kwargs are passed to :meth:`__init__`.
        """
        f = io.BytesIO()
        cls(f, *args, **kwargs).dump(obj)
        return f.getvalue()


class QueueUnpickler(pickle.Unpickler):
    """
    Unpickler that corresponds to :class:`QueuePickler`,
    replacing :class:`Future` instances by the actual result of the computation.
    """
    def __init__(self, file, queue=None):
        super().__init__(file)
        self.queue = queue

    def persistent_load(self, pid):
        # This method is invoked whenever a persistent ID is encountered.
        # Here, pid is the tuple returned by QueueUnpickler.
        tag, id = pid
        if tag == 'Future':
            # Fetch the referenced record from the database and return it.
            return self.queue[id].result
        else:
            # Always raises an error if you cannot return the correct object.
            # Otherwise, the unpickler will think None is the object referenced
            # by the persistent ID.
            raise pickle.UnpicklingError('unsupported persistent object')

    @classmethod
    def loads(cls, s, *args, **kwargs):
        """
        Load input string ``s``.
        args and kwargs are passed to :meth:`__init__`.
        """
        f = io.BytesIO(s)
        return cls(f, *args, **kwargs).load()


class Task(BaseClass):
    """
    Class representing a task, i.e. a application (:attr:`app`) and the arguments to call it with (:attr:`args` and :attr:`kwargs`).

    Attributes
    ----------
    id : str
        Task unique identifier.
        It is built from the pickle representation of (:attr:`app`, :attr:`args`, :attr:`kwargs`),
        such that different tasks have different identifiers (with extremely high probability).

    app : BaseApp
        Application.

    args : tuple
        Tuple of arguments to be passed to :meth:`BaseApp.run`.

    kwargs : dict
        Dictionary of arguments to be passed to :meth:`BaseApp.run`.

    require_ids : list
        List of task IDs that are required for this task to be run,
        infered from :class:`Future` instances passed to :attr:`args` and :attr:`kwargs`.

    state : str
        Task state, one of :attr:`TaskState.ALL`, i.e. ('WAITING', 'PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'KILLED', 'UNKNOWN').

    jobid : str
        Job identifier (not used for now).

    errno : int
        0 if no error, else error number (defaults to 42).

    err : str
        Error message; empty if no error.

    out : str
        Standard output message.

    versions : dict
        Module versions.

    result : object
        Value returned by :class:`BaseApp.func`.

    dtime : float
        Running time of :class:`BaseApp.run`.
    """
    _attrs = ['id', 'app', 'args', 'kwargs', 'require_ids', 'state', 'jobid', 'errno', 'err', 'out', 'versions', 'result', 'dtime']

    def __init__(self, app, args=None, kwargs=None, state=None):
        """
        Initialize :class:`Task`.

        Parameters
        ----------
        app : BaseApp
            Application.

        args : tuple, default=None
            Tuple of arguments to be passed to :meth:`BaseApp.run`.

        kwargs : dict, default=None
            Dictionary of arguments to be passed to :meth:`BaseApp.run`.

        id : str, default=None
            Unique identifier for task.
            As a default, it is built from the pickle representation of (:attr:`app`, :attr:`args`, :attr:`kwargs`),
            such that different tasks have different identifiers (with extremely high probability).

        state : str, default=None
            Task state. Defaults to 'WAITING' if this task requires others to be run,
            else to 'PENDING'.
        """
        for name in ['jobid', 'err', 'out']:
            setattr(self, name, '')
        self.result = None
        self.dtime = None
        self.update(app=app, args=args, kwargs=kwargs, state=state)

    def update(self, **kwargs):
        """Update task with input attributes."""
        require_id = False
        if 'app' in kwargs:
            self.app = kwargs.pop('app')
            require_id = True
        if 'args' in kwargs:
            self.args = tuple(kwargs.pop('args'))
            require_id = True
        if 'kwargs' in kwargs:
            self.kwargs = dict(kwargs.pop('kwargs') or {})
            require_id = True

        if not hasattr(self, 'require_ids') or require_id:
            try:
                f = io.BytesIO()
                pickler = QueuePickler(f)
                pickler.dump((self.app, self.args, self.kwargs))
                uid = f.getvalue()
                self.require_ids = list(getattr(pickler, 'future_ids', []))
            except (AttributeError, pickle.PicklingError) as exc:
                raise ValueError('Make sure the task function, args and kwargs are picklable') from exc
        if 'state' in kwargs:
            self.state = kwargs.pop('state')
            if self.state is None:
                if self.require_ids:
                    self.state = TaskState.WAITING
                else:
                    self.state = TaskState.PENDING
        if require_id:
            hex = hashlib.md5(uid).hexdigest()
            self.id = str(uuid.UUID(hex=hex))  # unique ID, tied to the given app, args and kwargs
        for name in ['jobid', 'errno', 'err', 'out', 'result', 'dtime']:
            if name in kwargs:
                setattr(self, name, kwargs.pop(name))
        if kwargs:
            raise ValueError('Unrecognized arguments {}'.format(kwargs))

    def clone(self, *args, **kwargs):
        """Return an updated copy."""
        new = self.copy()
        new.update(*args, **kwargs)
        return new

    def run(self):
        """
        Run task:

        - call :class:`BaseApp.run`, saving main script where the task is defined and package versions in a folder '.desipipe' located in the directory where files are saved (if any).
        - set :attr:`errno`, :attr:`result`, :attr:`err`, :attr:`out`, :attr:`versions` and :attr:`dtime`.
        - set :attr:`state`: 'KILLED' if termination signal, 'FAILED' if :class:`BaseApp.run` raised an excpetion, else 'SUCCEEDED'.

        """
        from .file_manager import File
        t0 = time.time()

        def save_attrs(dirname):
            dirname = os.path.join(dirname, '.desipipe')
            script_fn = os.path.join(dirname, '{}.py'.format(self.app.name))
            input_fn = getattr(self.app, 'file', None)
            if input_fn is not None and os.path.isfile(input_fn):
                shutil.copyfile(input_fn, script_fn)
            with open(script_fn, 'w') as file:
                file.write(self.app.code)
            versions_fn = os.path.join(dirname, '{}.versions'.format(self.app.name))
            with open(versions_fn, 'w') as file:
                for name, version in self.app.versions().items():
                    file.write('{}={}\n'.format(name, version))

        File.save_attrs = save_attrs  # save main script and versions whenever a file is written to disk
        self.errno, self.result, self.err, self.out, self.versions = self.app.run(tuple(self.args), self.kwargs)
        File.save_attrs = None
        if self.errno:
            if self.errno == signal.SIGTERM:
                self.state = TaskState.KILLED
            else:
                self.state = TaskState.FAILED
        else:
            self.state = TaskState.SUCCEEDED
        self.dtime = time.time() - t0

    def __getstate__(self):
        """Return the task state (called by pickler)."""
        return {name: getattr(self, name) for name in self._attrs if hasattr(self, name)}


class Future(BaseClass):

    """Structure that keeps track of :class:`Task` result (searching in the :attr:`queue`, with given :attr:`id`)."""

    def __init__(self, queue, id):
        """
        Initialize :class:`Future`.

        Parameters
        ----------
        queue : Queue
            Queue where the associated :class:`Task` is saved.

        id : str
            :attr:`Task.id`.
        """
        self.queue = queue
        self.id = str(id)

    def result(self, timeout=1e4, timestep=1.):
        """
        Return task result.

        Parameters
        ----------
        timeout : float, default=1e4
            Return ``None`` after this delay (in seconds) if no success.

        timestep : float, default=1.
            Period (in seconds) at which the queue is queried.

        Returns
        -------
        result : object
            Value returned by :class:`BaseApp.func`.
        """
        t0 = time.time()
        try:
            return self._result
        except AttributeError:
            while True:
                if (time.time() - t0) < timeout:
                    if self.queue.tasks(id=self.id, property='state') not in (TaskState.WAITING, TaskState.PENDING):
                        # print(self.queue.tasks(self.id)[0].err)
                        self._result = self.queue.tasks(id=self.id).result
                        return self._result
                    time.sleep(timestep * random.uniform(0.8, 1.2))
                else:
                    self.log_error('time out while getting result')
                    return None


queue_states = ['ACTIVE', 'PAUSED']


QueueState = type('QueueState', (), {**dict(zip(queue_states, queue_states)), 'ALL': queue_states})


class Queue(BaseClass):

    """Queue keeping track of all tasks that have been run and to be run, with sqlite backend."""

    def __init__(self, name, base_dir=None, create=None, spawn=False):
        """
        Initialize queue.

        Parameters
        ----------
        name : str
            Name of queue; can contain alphanumeric characters, underscores and hyphens.
            'this/queue' saves the queue as ``base_dir/this/queue.sqlite``.
            '/this/queue' saves the queue as '/this/queue.sqlite' (starting from root).

        base_dir : str, default=None
            Base directory where to save queue.
            If ``None``, defaults to :attr:`Config.queue_dir`.

        create : bool, default=None
            If ``True``, create a new queue; a :class:`ValueError` is raised if the queue already exists.
            If ``False``, do not create the queue; a class:`ValueError` is raised if no queue exists.
            If ``None`` (default), create the queue if does not exist.

        spawn : bool, default=False
            If ``True``, spawn a manager process that will distribute the tasks among workers.
        """
        if isinstance(name, self.__class__):
            self.__dict__.update(name.__dict__)
            return

        if base_dir is None:
            base_dir = Config().queue_dir

        if re.match('^[a-zA-Z0-9_/.-]+$', name) is None:
            raise ValueError('Input queue name {} must be alphanumeric plus underscores and hyphens'.format(name))

        if not name.endswith('.sqlite'):
            name += '.sqlite'
        self.fn = os.path.abspath(os.path.join(base_dir, name))
        self.base_dir = os.path.dirname(self.fn)

        # Check if it already exists and/or if we are supposed to create it
        exists = os.path.exists(self.fn)
        if create is None:
            create = not exists
        elif create and exists:
            raise ValueError('Queue {} already exists'.format(name))
        elif (not create) and (not exists):
            raise ValueError('Queue {} does not exist'.format(name))

        # Create directory with rwx for user but no one else
        if create:
            utils.mkdir(self.base_dir, mode=0o700)
            self.db = sqlite3.Connection(self.fn)

            # Give rw access to user but no one else
            os.chmod(self.fn, 0o600)

            # Create tables
            script = """
            CREATE TABLE tasks (
                id       TEXT PRIMARY KEY,
                task     TEXT,
                state    TEXT,
                tmid     TEXT  -- task manager id
            );
            -- Dependencies table.  Multiple entries for multiple deps.
            CREATE TABLE requires (
                id      TEXT,     -- task.id foreign key
                require TEXT,     -- task.id that it depends upon
            -- Add foreign key constraints
                FOREIGN KEY(id) REFERENCES tasks(id),
                FOREIGN KEY(require) REFERENCES tasks(id)
            );
            -- Task manager table
            CREATE TABLE managers (
                tmid    TEXT PRIMARY KEY, -- task manager id foreign key
                manager TEXT,             -- task manager
            -- Add foreign key constraints
                FOREIGN KEY(tmid) REFERENCES tasks(tmid)
            );
            -- Metadata about this queue, e.g. active/paused
            CREATE TABLE metadata (
                key   TEXT,
                value TEXT
            );
            """
            self.db.executescript(script)
            # Initial queue state is active
            self.db.execute('INSERT INTO metadata VALUES (?, ?)', ('state', QueueState.ACTIVE))
            self.db.commit()
        else:
            self.db = sqlite3.Connection(self.fn, timeout=60)
        if spawn:
            subprocess.Popen(['desipipe', 'spawn', '--queue', self.fn], start_new_session=True, env=os.environ)

    def _query(self, query, timeout=120., timestep=1., many=False):
        """
        Perform a database query, retrying if needed.

        Parameters
        ----------
        query : str, list
            Query as a string, or a tuple or list (query, arguments),
            where arguments is a tuple, or list of tuples if ``many`` is ``True``.

        timeout : float, default=120
            After this delay (in seconds) without success, raise sqlite3.OperationalError.

        timestep : float, default=1
            Period (in seconds) at which the queue is queried.

        many : bool, default=False
            If ``True``, call the same query for the list of arguments (tuple).
            (in practice, call ``db.executemany(query, args)`` instead of ``db.execute(query)``).

        Returns
        -------
        result : str
            Result of query.
        """
        t0, ntries = time.time(), 1
        if isinstance(query, str):
            query = (query,)
        while True:
            try:
                if ntries > 1:
                    self.log_debug('Retrying: "{}"'.format(' '.join(query)))

                if many:
                    result = self.db.executemany(*query)
                else:
                    result = self.db.execute(*query)

                if ntries > 1:
                    self.log_debug('Succeeded after {} tries'.format(ntries))

                return result

            except (sqlite3.OperationalError, sqlite3.DatabaseError) as exc:
                # A few known errors that can occur when multiple clients
                # are hammering on the database. For these cases, wait
                # and try again a few times before giving up.
                known_excs = ['database is locked', 'database disk image is malformed']  # on NFS
                if getattr(exc, 'message', '').lower() in known_excs:
                    if (time.time() - t0) < timeout:
                        self.db.close()
                        time.sleep(timestep * random.uniform(0.8, 1.2))
                        self.db = sqlite3.Connection(self.fn)
                        ntries += 1
                    else:
                        self.log_error('tried {} times and still getting errors'.format(ntries))
                        raise exc
                else:
                    raise exc

    def _add_requires(self, id, requires):
        """
        Add requirements for a task.

        Parameters
        ----------
        id : str
            Task ID.

        requires : list
            List of IDs upon which this task depends.
        """
        query = 'INSERT OR REPLACE INTO requires (id, require) VALUES (?, ?)'
        if isinstance(requires, str):
            self._query([query, (id, requires)])
        else:
            args = [(id, x) for x in requires]
            self._query([query, args], many=True)
        self.db.commit()

    def _add_manager(self, manager):
        # """Add input :class:`TaskManager` to the data base (or replace if already there, as specified by :attr:`TaskManager.id`)."""
        query = 'INSERT OR REPLACE INTO managers (tmid, manager) VALUES (?, ?)'
        self._query([query, (manager.id, QueuePickler.dumps(manager))])
        self.db.commit()

    def add(self, tasks, replace=False):
        """
        Add input task(s) to the queue.

        Parameters
        ----------
        tasks : Task, list
            Task or list of tasks.

        replace : bool, default=False
            If ``True``, replace task(s) (as identified by their IDs) with the input ones.
            If ``False``, an error is raised if input tasks (as identified by their IDs) are already in the queue.
            If ``None``, do not add task if already in queue.

        Returns
        -------
        futures : list
            List of :class:`Future` corresponding to input tasks.
        """
        isscalar = isinstance(tasks, Task)
        if isscalar:
            tasks = [tasks]
        ids, requires, managers, states, tasks_serialized, futures = [], [], [], [], [], []
        for task in tasks:
            futures.append(Future(queue=self, id=task.id))
            if replace is None:
                row = self._query(['SELECT COUNT(id) FROM tasks WHERE id=?', (task.id,)]).fetchone()
                if row is not None and row[0]: continue
            ids.append(task.id)
            requires.append(task.require_ids)
            managers.append(task.app.task_manager)
            states.append(task.state)
            tasks_serialized.append(QueuePickler.dumps(task))
        query = 'INSERT'
        if replace: query = 'REPLACE'
        query += ' INTO tasks (id, task, state, tmid) VALUES (?,?,?,?)'
        self._get_lock()
        self._query([query, zip(ids, tasks_serialized, states, [tm.id for tm in managers])], many=True)
        if not replace:
            for id, requires in zip(ids, requires):
                self._add_requires(id, requires)
            for manager in managers:
                self._add_manager(manager)
        self.db.commit()
        self._release_lock()
        for id, state in zip(ids, states):
            if state in (TaskState.SUCCEEDED, TaskState.FAILED):
                self._update_waiting_tasks(id)
        if isscalar:
            return futures[0]
        return futures

    def _get_lock(self, timeout=10., timestep=1.):
        # """Get lock on the data base."""
        t0 = time.time()
        while True:
            try:
                self.db.execute('BEGIN IMMEDIATE')
                return True
            except sqlite3.OperationalError as exc:
                if (time.time() - t0) > timeout:
                    self.log_error('unable to get database lock')
                    return False
                time.sleep(timestep * random.uniform(0.8, 1.2))

    def _release_lock(self):
        # """Release lock on the data base."""
        self.db.commit()

    @property
    def state(self):
        """Get queue state ('ACTIVE' or 'PAUSED')."""
        return self._query('SELECT value FROM metadata WHERE key="state"').fetchone()[0]

    @state.setter
    def state(self, state):
        """Set queue state ('ACTIVE' or 'PAUSED')."""
        if state not in (QueueState.ACTIVE, QueueState.PAUSED):
            raise ValueError('Invalid queue state {}; should be {} or {}'.format(state, QueueState.ACTIVE, QueueState.PAUSED))
        self._query(['UPDATE metadata SET value=? where key="state"', (state,)])
        self.db.commit()

    def pause(self):
        """Pause queue, i.e. set state to 'PAUSED'."""
        self.state = QueueState.PAUSED

    @property
    def paused(self):
        """Is queue paused?"""
        return self.state == QueueState.PAUSED

    def resume(self):
        """Resume queue, i.e. set state to 'ACTIVE'."""
        self.state = QueueState.ACTIVE

    def set_task_state(self, id, state):
        """Set the state of task with input ID ``id`` to ``state``."""
        try:
            self._get_lock()
            query = 'UPDATE tasks SET state=? WHERE id=?'
            self._query([query, (state, id)])
            self.db.commit()

            if state in (TaskState.SUCCEEDED, TaskState.FAILED):
                self._update_waiting_tasks(id)

        finally:
            self._release_lock()

    def _update_waiting_task_state(self, id, force=False):
        """
        Check if all requirements of task of ID ``id`` have finished running.
        If so, set it into the task to 'PENDING' state.

        If ``force``, do the check no matter what. Otherwise, only proceed
        with check if the task is still in the 'WAITING' state.
        """
        if not self._get_lock():
            self.log_error('unable to get db lock; not updating waiting task state')
            return

        # Ensure that it is still waiting
        # (another process could have moved it into pending)
        if not force:
            q = 'SELECT state FROM tasks where tasks.id=?'
            row = self.db.execute(q, (id,)).fetchone()
            if row is None:
                self._release_lock()
                raise ValueError('Task ID {} not found'.format(id))
            if row[0] != TaskState.WAITING:
                self._release_lock()
                return

        # Count number of requires that are still pending or waiting
        query = 'SELECT COUNT(d.require) FROM requires d JOIN tasks t ON d.require = t.id WHERE d.id=? AND t.state IN (?, ?, ?)'
        row = self._query([query, (id, TaskState.PENDING, TaskState.WAITING, TaskState.RUNNING)]).fetchone()
        self._release_lock()
        if row is None:
            return
        if row[0] == 0:
            self.set_task_state(id, TaskState.PENDING)
        elif force:
            self.set_task_state(id, TaskState.WAITING)

    def _update_waiting_tasks(self, id):
        """Identify tasks that are waiting for task of ID ``id``, and call :meth:`_update_waiting_task_state` on them."""
        query = 'SELECT t.id FROM tasks t JOIN requires d ON d.id=t.id WHERE d.require=? AND t.state=?'
        waiting_tasks = self._query([query, (id, TaskState.WAITING)]).fetchall()
        for id in waiting_tasks:
            self._update_waiting_task_state(id[0])

    def delete(self):
        """Delete data base :attr:`db` from both this instance and the disk."""
        if hasattr(self, 'db'):
            self.db.close()
            del self.db
        import shutil
        shutil.rmtree(self.base_dir, ignore_errors=True)

    def tasks(self, id=None, tmid=None, state=None, one=None, property=None):
        """
        List tasks in queue.

        Parameters
        ----------
        id : str, default=None
            If not ``None``, select task with given ID.

        tmid : str, default=None
            If not ``None``, select tasks with given task manager ID.

        state : str, default=None
            If not ``None``, select tasks with given state.

        one : bool, default=None
            If ``True``, return only one task.
            If ``False``, return list.
            If ``None``, and ``id`` is not ``None``, return a single task; else a list of tasks.

        property : str, default=None
            If not ``None``, instead of returning task(s), return this property
            (one of 'id', 'state', 'task_manager').

        Returns
        -------
        tasks : Task, list
            Task or property or property or list of such objects.
        """
        # View as list
        select = []
        if id is not None:
            select.append('id="{}"'.format(id))
            if one is None: one = True
        if tmid is not None:
            select.append('tmid="{}"'.format(tmid))
        if state is not None:
            select.append('state="{}"'.format(state))
        query = 'SELECT task, id, state, tmid FROM tasks'
        if select: query += ' WHERE {}'.format(' AND '.join(select))
        tasks = self._query(query)
        if one:
            tasks = tasks.fetchone()
            if tasks is None: return None
            tasks = [tasks]
        else:
            tasks = tasks.fetchall()
            if tasks is None: return []
        toret = []
        for task in tasks:
            task, id, state, tmid = task
            if property == 'id':
                toret.append(id)
                continue
            if property == 'state':
                toret.append(state)
                continue
            task_manager = self.managers(tmid=tmid)
            if property == 'task_manager':
                toret.append(task_manager)
                continue
            task = QueueUnpickler.loads(task, queue=self)
            task.update(id=id, state=state)
            task.app.task_manager = task_manager
            toret.append(task)
        if one:
            return toret[0]
        return toret

    def pop(self, id=None, tmid=None):
        """
        Retrieve a task to be run (i.e. in 'PENDING' state).

        Parameters
        ----------
        id : str, default=None
            If not ``None``, pop the task with given ID.

        tmid : str, default=None
            If not ``None``, pop a task with given task manager ID.

        Returns
        -------
        task : Task
        """
        # First make sure we are not paused
        if self.state == QueueState.PAUSED:
            return None

        if self._get_lock():
            task = self.tasks(id=id, tmid=tmid, state=TaskState.PENDING, one=True)
        else:
            self.log_warning("There may be tasks left in queue but I couldn't get lock to see")
            return None
        self._release_lock()
        if task is None:
            return None
        self.set_task_state(task.id, TaskState.RUNNING)
        task.update(state=TaskState.RUNNING)
        return task

    def managers(self, tmid=None, property=None):
        """
        List managers.

        Parameters
        ----------
        tmid : str, default=None
            If not ``None``, return the task manager with given ID.

        property : str, default=None
            If not ``None``, instead of returning task manager(s), return this property
            (one of 'tmid').

        Returns
        -------
        tm : TaskManager, list
           Task manager or property or list of such objects.
        """
        query = 'SELECT tmid, manager FROM managers'
        one = tmid is not None
        if one:
            query += ' WHERE tmid="{}"'.format(tmid)
        managers = self._query(query).fetchall()
        if managers is None:
            return None
        toret = []
        for manager in managers:
            tmid, manager = manager
            if property in ('tmid', 'id'):
                toret.append(tmid)
                continue
            toret.append(QueueUnpickler.loads(manager, queue=self))
        if one:
            return toret[0]
        return toret

    def counts(self, tmid=None, state=None):
        """
        Count the number of tasks.

        Parameters
        ----------
        tmid : str, default=None
            If not ``None``, select tasks with given task manager ID.

        state : str, default=None
            If not ``None``, select tasks with given state.

        Returns
        -------
        counts : int
        """
        select = []
        if tmid is not None:
            select.append('tmid="{}"'.format(tmid))
        if state is not None:
            select.append('state="{}"'.format(state))
        query = 'SELECT count(state) FROM tasks'
        if select: query += ' WHERE {}'.format(' AND '.join(select))
        return self._query(query).fetchone()[0]

    def summary(self, tmid=None, return_type='dict'):
        """
        Return summary description of queue, i.e. number of tasks in all states :attr:`TaskState.ALL`.

        Parameters
        ----------
        tmid : str, default=None
            If not ``None``, select tasks with given task manager ID.

        return_type : str, default='dict'
            If 'dict', return a dictionary mapping task state to the task counts.
            If 'str', return a string.

        Returns
        -------
        summary : dict, str
        """
        counts = {state: self.counts(tmid=tmid, state=state) for state in TaskState.ALL}
        if return_type == 'dict':
            return counts
        if return_type == 'str':
            toret = ['{:10s}: {}'.format(state, count) for state, count in counts.items()]
            return '\n'.join(toret)
        raise ValueError('Unkown return_type {}'.format(return_type))

    def __repr__(self):
        """String representation of queue: size, state and file name."""
        return '{}(size={}, state={}, fn={})'.format(self.__class__.__name__, self.counts(), self.state, self.fn)

    def __str__(self):
        """String representation of queue: size, state and file name, and summary."""
        return self.__repr__() + '\n' + self.summary(return_type='str')

    def __getitem__(self, id):
        """Return task with input ID ``id``."""
        toret = self.tasks(id=id)
        if toret is None:
            raise KeyError('task {} not found'.format(id))
        return toret

    def __getstate__(self):
        """Return queue state: just its file name."""
        return {'fn': self.fn}

    def __setstate__(self, state):
        """Set queue state, from the file name."""
        self.__init__(state['fn'], base_dir='', create=False, spawn=False)


class BaseApp(BaseClass):
    """
    Base application class.

    Attributes
    ----------
    func : callable
        Function doing the job.

    name : str
        :attr:`func` name.

    code : str
        :attr:`func` code.

    file : str
        File path where :attr:`func` is defined (may be ``None``, e.g. in case of notebooks).

    task_manager : TaskManager
        Task manager to which the task has been added.
    """
    def __init__(self, func, task_manager=None):
        """
        Initialize application, called by :class:`TaskManager` decorators :meth:`TaskManager.bash_app` and :meth:`TaskManager.python_app`.

        Parameters
        ----------
        func : callable
            Function doing the job.

        task_manager : TaskManager, default=None
            Task manager to which the task has been added.
        """
        if isinstance(func, self.__class__):
            self.__dict__.update(func.__dict__)
            return
        self.update(func=func, task_manager=task_manager)

    def update(self, **kwargs):
        """Update app with input attributes."""
        if 'func' in kwargs:
            self.func = kwargs.pop('func')
            self.name = self.func.__name__
            self.code = textwrap.dedent(inspect.getsource(self.func)).split('\n')
            if self.code[0].startswith('@'):
                self.code = self.code[1:]
            self.code = '\n'.join(self.code)
            self.file = None
            try:
                self.file = inspect.getfile(self.func)
                if not os.path.isfile(self.file): self.file = None
            except:
                pass
            #self.imports = {}
            #for m in re.findall('[\n;\s]*from\s+([^\s.]*)\s+import', self.code) + re.findall('[\n;\s]*import\s+([^\s.]*)\s*', self.code):
            #    self.imports[m.__name__] = getattr(m, '__version__')
            if 'task_manager' in kwargs:
                self.task_manager = kwargs.pop('task_manager')
        if kwargs:
            raise ValueError('Unrecognized arguments {}'.format(kwargs))

    def clone(self, **kwargs):
        """Return an updated copy."""
        new = self.copy()
        new.update(**kwargs)
        return new

    def __call__(self, *args, **kwargs):
        """Call the decorator, i.e. add task to the queue."""
        return self.task_manager.add(Task(self, args, kwargs))

    def __getstate__(self):
        """Return app state."""
        state = {name: getattr(self, name) for name in ['name', 'code', 'file', 'task_manager']}
        return state

    def __setstate__(self, state):
        """Set app state."""
        self.__dict__.update(state)
        exec(self.code)
        self.func = locals()[self.name]

    def run(self, args, kwargs):
        """Run app with input ``args`` and ``kwargs``."""
        raise NotImplementedError

    def versions(self):
        """Return module versions (unknown by default)."""
        return {}


def select_modules(modules):
    """Select 'standard' top-level modules: those which do not start with an underscore."""
    return set(name.split('.', maxsplit=1)[0] for name in modules if not name.startswith('_'))


_modules = select_modules(sys.modules)


class PythonApp(BaseApp):
    """
    Python application, e.g.:

    .. code-block:: python

        @tm.python_app
        def test(n):
            print('hello' * n)

    """
    def run(self, args, kwargs):
        """Run app with input ``args`` and ``kwargs``."""
        errno, result = 0, None
        with io.StringIO() as out, io.StringIO() as err, contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            try:
                result = self.func(*args, **kwargs)
            except Exception as exc:
                errno = getattr(exc, 'errno', 42)
                traceback.print_exc(file=err)
                #raise exc
            versions = self.versions()
            return errno, result, err.getvalue(), out.getvalue(), versions

    def versions(self):
        """Return module versions."""
        versions = {}
        for name in select_modules(sys.modules) - _modules:
            try:
                versions[name] = sys.modules[name].__version__
            except (KeyError, AttributeError):
                pass


class BashApp(BaseApp):
    """
    Bash application, e.g.:

    .. code-block:: python

        @tm.bash_app
        def test(n):
            return "echo '{}'".format('hello' * n)

    """
    def run(self, args, kwargs):
        """Run app with input ``args`` and ``kwargs``."""
        errno, result, out, err = 0, None, '', ''
        cmd = self.func(*args, **kwargs)
        proc = subprocess.Popen(cmd.split(' '), stderr=subprocess.PIPE, stdout=subprocess.PIPE)
        out, err = proc.communicate()
        return errno, result, err, out, {}


class TaskManager(BaseClass):
    """
    Task manager, main class to be used in scripts, e.g.:

    .. code-block:: python

        queue = Queue('test', base_dir='_tests', spawn=True)
        tm = TaskManager(queue, environ=Environment(), scheduler=dict(max_workers=10))

        @tm.python_app
        def test(n):
            print('hello' * n)

    Attributes
    ----------
    queue : Queue
        Queue, see :class:`Queue`.

    id : str
        Task manager unique identifier.
        It is built from the pickle representation of (:attr:`environ`, :attr:`scheduler`, :attr:`provider`),
        such that different task managers have different identifiers (with extremely high probability).

    environ : BaseEnvironment
        Tasks are run within this environment.

    scheduler : BaseScheduler
        Tasks are distributed to workers with this scheduler.

    provider : BaseProvider
        Tasks are executed on the machine with this provider.
    """
    def __init__(self, queue, environ=None, scheduler=None, provider=None):
        """
        Initialize :class:`TaskManager`.

        Parameters
        ----------
        queue : str, Queue
            Queue, see :func:`get_queue`.

        environ : BaseEnvironment, str, dict, default=None
            Tasks are run within this environment. See :func:`get_environ`.

        scheduler : BaseScheduler, str, dict, default=None
            Tasks are distributed to workers with this scheduler. See :func:`get_scheduler`.

        provider : BaseProvider, str, dict, default=None
            Tasks are executed on the machine with this provider. See :func:`get_provider`.
        """
        self.update(queue=queue, environ=environ, scheduler=scheduler, provider=provider)

    def update(self, **kwargs):
        """Update task manager attributes."""
        require_id = False
        for name, func in zip(['queue', 'environ', 'scheduler', 'provider'],
                              [get_queue, get_environ, get_scheduler, get_provider]):
            if name in kwargs:
                setattr(self, name, func(kwargs.pop(name)))
                if name != 'queue': require_id = True
        if require_id:
            uid = QueuePickler.dumps((self.environ, self.scheduler, self.provider))
            hex = hashlib.md5(uid).hexdigest()
            self.id = str(uuid.UUID(hex=hex))  # unique ID, tied to the given environ, scheduler, provider
        if kwargs:
            raise ValueError('Unrecognized arguments {}'.format(kwargs))

    def clone(self, **kwargs):
        """Return an updated copy."""
        new = self.copy()
        new.update(**kwargs)
        return new

    def python_app(self, func):
        """Decorator for :class:`PythonApp`."""
        return PythonApp(func, task_manager=self)

    def bash_app(self, func):
        """Decorator for :class:`BashApp`."""
        return BashApp(func, task_manager=self)

    def add(self, task, replace=None):
        """Add task to the queue, see :meth:`Queue.add`."""
        task.app.task_manager = self
        return self.queue.add(task, replace=replace)

    def spawn(self, *args, **kwargs):
        """Distribute tasks to workers."""
        self.scheduler.update(provider=self.provider)
        self.scheduler(*args, **kwargs)


def work(queue, tmid=None, id=None, mpicomm=None):
    """
    Do the actual work: pop tasks from the input queue, and run them.

    Parameters
    ----------
    queue : Queue
        Queue to pop tasks from.

    tmid : str, default=None
        If not ``None``, take tasks with this task manager ID.

    id : str, default=None
        If not ``None``, take a task with this task ID.

    mpicomm : MPI communicator, default=mpi.COMM_WORLD
        The MPI communicator.
    """
    if mpicomm is None:
        from mpi4py import MPI
        mpicomm = MPI.COMM_WORLD
    while True:
        task = None
        # print(queue.summary(), queue.counts(state='PENDING'))
        if mpicomm.rank == 0:
            task = queue.pop(tmid=tmid, id=id)
        # print(task, queue.counts(state=TaskState.PENDING))
        task = mpicomm.bcast(task, root=0)
        if task is None:
            break
        task.run()
        # task.update(jobid=environ.get('DESIPIPE_JOBID', ''))
        # print(task.state, task.result)
        if mpicomm.rank == 0:
            queue.add(task, replace=True)


def spawn(queue, timeout=1e4, timestep=1.):
    """
    Distribute tasks to workers.
    If all queues are paused, the function terminates.

    Parameters
    ----------
    queue : Queue, list
        Queue or list of queues to process.

    timeout : float, default=1e4
        Time out after this delay (in seconds).

    timestep : float, default=1.
        Period (in seconds) at which the queue is queried for new tasks.
    """
    queues = [queue] if isinstance(queue, Queue) else queue
    t0 = time.time()
    stop = False
    managers = [queue.managers() for queue in queues]  # get once, to avoid unpickling manager for each task
    while True:
        if (time.time() - t0) > timeout:
            break
        if all(queue.paused for queue in queues) or stop:
            break
        stop = True
        for iq, queue in enumerate(queues):
            if queue.paused:
                continue
            for manager in managers[iq]:
                ntasks = queue.counts(tmid=manager.id, state=TaskState.PENDING)
                # for task in queue.tasks():
                #     print(task.state, getattr(task, 'err', None))
                if ntasks: stop = False
                # print('desipipe work --queue {} --tmid {}'.format(queue.fn, manager.id))
                # manager.spawn('desipipe work --queue {}'.format(queue.fn), ntasks=ntasks)
                # print('desipipe work --queue {} --tmid {}'.format(queue.fn, manager.id))
                manager.spawn('desipipe work --queue {} --tmid {}'.format(queue.fn, manager.id), ntasks=ntasks)
        time.sleep(timestep * random.uniform(0.8, 1.2))


def get_queue(queue, create=None, spawn=False):
    """
    Return queue.

    Parameters
    ----------
    queue : str, Queue, list
        Queue names, :class:`Queue`, or list of such objects.

    create : bool, default=None
        If ``True``, create a new queue; a :class:`ValueError` is raised if the queue already exists.
        If ``False``, do not create the queue; a class:`ValueError` is raised if no queue exists.
        If ``None`` (default), create the queue if does not exist.

    spawn : bool, default=False
        If ``True``, spawn a manager process that will distribute the tasks among workers.

    Returns
    -------
    queue : Queue, list
        Queue or list of queues.
    """
    if isinstance(queue, list):
        toret = []
        for queue in queue:
            tmp = get_queue(queue, create=create, spawn=spawn)
            if isinstance(tmp, Queue):
                toret.append(tmp)
            else:
                toret += tmp
        return toret
    if isinstance(queue, Queue):
        return queue
    if '/' in queue:
        if queue.startswith('.'):
            queue = os.path.abspath(queue)
        else:
            queue = os.path.join(Config()['base_queue_dir'], queue)
    else:
        queue = os.path.join(Config().queue_dir, queue)
    if '*' in queue:
        return [get_queue(queue, create=create, spawn=spawn) for queue in glob.glob(queue) if queue.endswith('.sqlite')]
    return Queue(queue, create=create, spawn=spawn)


def action_from_args(action='work', args=None):

    """Function called when using desipipe from the command line."""

    logger = logging.getLogger('desipipe')
    from .utils import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    if action == 'queues':

        parser.add_argument('-q', '--queue', type=str, required=False, default='*/*', help='Name of queue; user/queue to select user != {} and e.g. */* to select all queues of all users)'.format(Config.default_user))
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue, create=False)
        if isinstance(queues, Queue): queues = [queues]
        if not queues:
            logger.info('No matching queue')
            return
        logger.info('Matching queues:')
        for queue in queues:
            logger.info(str(queue))
        return

    if action == 'work':

        parser.add_argument('-q', '--queue', type=str, required=True, help='Name of queue; user/queue to select user != {}'.format(Config.default_user))
        parser.add_argument('--tmid', type=str, required=False, default=None, help='Task manager ID')
        parser.add_argument('--id', type=str, required=False, default=None, help='Task ID')
        args = parser.parse_args(args=args)
        if '*' in args.queue:
            raise ValueError('Provide single queue!')
        return work(get_queue(args.queue, create=False), tmid=args.tmid, id=args.id)

    if action == 'tasks':

        parser.add_argument('-q', '--queue', type=str, required=True, help='Name of queue; user/queue to select user != {}'.format(Config.default_user))
        parser.add_argument('--tmid', type=str, required=False, default=None, help='Task manager ID')
        parser.add_argument('--id', type=str, required=False, default=None, help='Task ID')
        parser.add_argument('--state', type=str, required=False, default=TaskState.FAILED, choices=TaskState.ALL, help='Task state')
        args = parser.parse_args(args=args)
        if '*' in args.queue:
            raise ValueError('Provide single queue!')
        logger.info('Tasks that are {}:'.format(args.state))
        for task in get_queue(args.queue, create=False).tasks(state=args.state, tmid=args.tmid, id=args.id):
            for name in ['jobid', 'errno', 'err', 'out']:
                logger.info('{}: {}'.format(name, getattr(task, name)))
            logger.info('=' * 20)
        return

    parser.add_argument('-q', '--queue', nargs='*', type=str, required=True, help='Name of queue; user/queue to select user != {} and e.g. */* to select all queues of all users)'.format(Config.default_user))

    if action == 'delete':

        parser.add_argument('--force', action='store_true', help='Pass this flag to force delete')
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue, create=False)
        if not queues:
            logger.info('No queue to delete.')
            return
        logger.info('I will delete these queues:')
        for queue in queues:
            logger.info(str(queue))
        if not args.force:
            logger.warning('--force is not set. To actually delete the queues, pass --force')
            return
        for queue in queues:
            queue.delete()
        return

    if action == 'pause':

        args = parser.parse_args(args=args)
        queues = get_queue(args.queue, create=False)
        for queue in queues:
            logger.info('Pausing queue {}'.format(repr(queue)))
            queue.pause()
        return

    if action == 'resume':

        parser.add_argument('--spawn', action='store_true', help='Spawn a new manager process')
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue, create=False)
        for queue in queues:
            logger.info('Resuming queue {}'.format(repr(queue)))
            queue.resume()
        if args.spawn:
            subprocess.Popen(['desipipe', 'spawn', '--queue', ' '.join(args.queue)], start_new_session=True, env=os.environ)
        return

    if action == 'spawn':

        parser.add_argument('--timeout', type=float, required=False, default=1e4, help='Stop after this time')
        parser.add_argument('--spawn', action='store_true', help='Spawn a new manager process and exit this one')
        args = parser.parse_args(args=args)
        if args.spawn:
            subprocess.Popen(['desipipe', 'spawn', '--queue', ' '.join(args.queue), '--timeout', str(args.timeout)], start_new_session=True, env=os.environ)
            return
        queues = get_queue(args.queue, create=False)
        return spawn(queues, timeout=args.timeout)

    if action == 'retry':

        parser.add_argument('--tmid', type=str, required=False, default=None, help='Task manager ID')
        parser.add_argument('--id', type=str, required=False, default=None, help='Task ID')
        parser.add_argument('--state', type=str, required=False, default=TaskState.KILLED, choices=TaskState.ALL, help='Task state')
        parser.add_argument('--spawn', action='store_true', help='Spawn a new manager process')
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue, create=False)
        for queue in queues:
            for id in queue.tasks(tmid=args.tmid, id=args.id, state=args.state, property='id'):
                queue.set_task_state(id, state=TaskState.PENDING)
        if args.spawn:
            subprocess.Popen(['desipipe', 'spawn', '--queue', ' '.join(args.queue)], start_new_session=True, env=os.environ)
        return

    raise ValueError('unknown action {}; pick from {}'.format(action, list(action_from_args.actions.keys())))


# Description for command line help
action_from_args.actions = {
    'spawn': 'Spawn a manager process to distribute tasks among workers (if queue is not paused)',
    'pause': 'Pause a queue: all workers and managers of provided queues (exclusively) stop after finishing their current task',
    'resume': 'Restart a queue: running manager processes (if any running) distribute again work among workers',
    'delete': 'Delete queue and data base',
    'queues': 'List all queues',
    'tasks': 'List all (failed) tasks of given queue',
    'retry': 'Move all (killed) tasks into PENDING state, so they are rerun'
}