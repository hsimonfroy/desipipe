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
    state = self.__getstate__()
    state['task_manager'] = state['file'] = None
    return (self.__class__.__new__, (self.__class__,), state)


class QueuePickler(pickle.Pickler):

    def __init__(self, *args, **kwargs):
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
        f = io.BytesIO()
        cls(f, *args, **kwargs).dump(obj)
        return f.getvalue()


class QueueUnpickler(pickle.Unpickler):

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
        f = io.BytesIO(s)
        return cls(f, *args, **kwargs).load()


class Task(BaseClass):

    _attrs = ['id', 'app', 'args', 'kwargs', 'require_ids', 'state', 'jobid', 'errno', 'err', 'out', 'versions', 'result', 'dtime']

    def __init__(self, app, args=None, kwargs=None, id=None, state=None):
        for name in ['jobid', 'err', 'out']:
            setattr(self, name, '')
        self.result = None
        self.dtime = None
        self.update(app=app, args=args, kwargs=kwargs, id=id, state=state)

    def update(self, **kwargs):
        if 'app' in kwargs:
            self.app = kwargs['app']
        if 'args' in kwargs:
            self.args = tuple(kwargs.pop('args'))
        if 'kwargs' in kwargs:
            self.kwargs = dict(kwargs.pop('kwargs') or {})
        require_id = 'id' in kwargs and kwargs['id'] is None
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
        if 'id' in kwargs:
            id = kwargs.pop('id')
            if require_id:
                hex = hashlib.md5(uid).hexdigest()
                id = uuid.UUID(hex=hex)  # unique ID, tied to the given app, args and kwargs
            self.id = str(id)
        for name in ['app', 'jobid', 'errno', 'err', 'out', 'result', 'dtime']:
            if name in kwargs:
                setattr(self, name, kwargs.pop(name))
        if kwargs:
            raise ValueError('Unrecognized arguments {}'.format(kwargs))

    def clone(self, *args, **kwargs):
        new = self.copy()
        new.update(*args, **kwargs)
        return new

    def run(self):
        from .file_manager import File
        t0 = time.time()

        def save_attrs(dirname):
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
            return (script_fn, versions_fn)

        File.save_attrs = save_attrs
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
        return {name: getattr(self, name) for name in self._attrs if hasattr(self, name)}


class Future(BaseClass):

    def __init__(self, queue, id):
        self.queue = queue
        self.id = str(id)

    def result(self, timeout=1e4, timestep=1.):
        t0 = time.time()
        try:
            return self._result
        except AttributeError:
            while True:
                if (time.time() - t0) < timeout:
                    if self.queue.tasks(id=self.id, property='state')[0] not in (TaskState.WAITING, TaskState.PENDING):
                        # print(self.queue.tasks(self.id)[0].err)
                        self._result = self.queue.tasks(self.id)[0].result
                        return self._result
                    time.sleep(timestep * random.uniform(0.8, 1.2))
                else:
                    self.log_error('time out while getting result')
                    return None


queue_states = ['ACTIVE', 'PAUSED']


QueueState = type('QueueState', (), {**dict(zip(queue_states, queue_states)), 'ALL': queue_states})


class Queue(BaseClass):

    def __init__(self, name, base_dir=None, create=None, spawn=False):
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
            self.db.execute('INSERT INTO metadata VALUES (?,?)', ('state', QueueState.ACTIVE))
            self.db.commit()
        else:
            self.db = sqlite3.Connection(self.fn, timeout=60)
        if spawn:
            subprocess.Popen(['desipipe', 'spawn', '--queue', self.fn], start_new_session=True, env=os.environ)

    def _query(self, query, timeout=120., timestep=2., many=False):
        """
        Perform a database query retrying if needed. If timeout seconds
        pass, then re-raise sqlite3.OperationalError from locked db.

        If ``many``, calls db.executemany(query, args) instead of
        db.execute(query).

        Returns result of query.
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
        Add requires for a task.

        id : string task id
        requires : list of ids upon which taskid depends
        """
        query = 'INSERT OR REPLACE INTO requires (id, require) VALUES (?, ?)'
        if isinstance(requires, str):
            self._query([query, (id, requires)])
        else:
            args = [(id, x) for x in requires]
            self._query([query, args], many=True)
        self.db.commit()

    def _add_manager(self, manager):
        query = 'INSERT OR REPLACE INTO managers (tmid, manager) VALUES (?, ?)'
        self._query([query, (manager.id, QueuePickler.dumps(manager))])
        self.db.commit()

    def add(self, tasks, task_manager=None, replace=False):
        isscalar = isinstance(tasks, Task)
        if isscalar:
            tasks = [tasks]
        ids, requires, managers, states, tasks_serialized, futures = [], [], [], [], [], []
        for task in tasks:
            futures.append(Future(queue=self, id=task.id))
            if replace is None:
                row = self._query(['SELECT COUNT(id) FROM tasks WHERE id=?', (task.id,)]).fetchone()
                if row is not None and row[0]: continue
            if task_manager is not None:
                task.app.task_manager = task_manager
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

    # Get and release locks on the data base
    def _get_lock(self, timeout=1., timestep=2.):
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
        self.db.commit()

    # Get / Set state of the queue
    @property
    def state(self):
        return self._query('SELECT value FROM metadata WHERE key="state"').fetchone()[0]

    @state.setter
    def state(self, state):
        if state not in (QueueState.ACTIVE, QueueState.PAUSED):
            raise ValueError('Invalid queue state {}; should be {} or {}'.format(state, QueueState.ACTIVE, QueueState.PAUSED))
        self._query(['UPDATE metadata SET value=? where key="state"', (state,)])
        self.db.commit()

    def set_task_state(self, id, state):
        try:
            self._get_lock()
            query = 'UPDATE tasks SET state=? WHERE id=?'
            self._query([query, (state, id)])
            self.db.commit()

            if state in (TaskState.SUCCEEDED, TaskState.FAILED):
                self._update_waiting_tasks(id)

        finally:
            self._release_lock()

    # functions to update states based on requires
    def _update_waiting_task_state(self, id, force=False):
        """
        Check if all requires of this task have finished running.
        If so, set it into the pending state.

        if force, do the check no matter what. Otherwise, only proceed
        with check if the task is still in the Waiting state.
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
        """
        Identify tasks that are waiting for id, and call
        :meth:`_update_waiting_task_state` on them.
        """
        query = 'SELECT t.id FROM tasks t JOIN requires d ON d.id=t.id WHERE d.require=? AND t.state=?'
        waiting_tasks = self._query([query, (id, TaskState.WAITING)]).fetchall()
        for id in waiting_tasks:
            self._update_waiting_task_state(id[0])

    def pause(self):
        self.state = QueueState.PAUSED

    @property
    def paused(self):
        return self.state == QueueState.PAUSED

    def resume(self):
        self.state = QueueState.ACTIVE

    def delete(self):
        if hasattr(self, 'db'):
            self.db.close()
            del self.db
        import shutil
        # ignore_errors = True is needed on NFS systems; this might
        # leave a dangling directory, but the sqlite db file itself
        # should be gone so it won't appear with qdo list.
        shutil.rmtree(self.base_dir, ignore_errors=True)

    # Get a task
    def pop(self, tmid=None, id=None):
        # First make sure we aren't paused
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

    def tasks(self, id=None, tmid=None, state=None, one=False, property=None):
        # View as list
        select = []
        if id is not None:
            select.append('id="{}"'.format(id))
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

    def managers(self, tmid=None, property=None):
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
        select = []
        if tmid is not None:
            select.append('tmid="{}"'.format(tmid))
        if state is not None:
            select.append('state="{}"'.format(state))
        query = 'SELECT count(state) FROM tasks'
        if select: query += ' WHERE {}'.format(' AND '.join(select))
        return self._query(query).fetchone()[0]

    def summary(self, tmid=None, return_type='dict'):
        counts = {state: self.counts(tmid=tmid, state=state) for state in TaskState.ALL}
        if return_type == 'dict':
            return counts
        if return_type == 'str':
            toret = ['{:10s}: {}'.format(state, count) for state, count in counts.items()]
            return '\n'.join(toret)
        raise ValueError('Unkown return_type {}'.format(return_type))

    def __repr__(self):
        return '{}(size={}, state={}, fn={})'.format(self.__class__.__name__, self.counts(), self.state, self.fn)

    def __str__(self):
        return self.__repr__() + '\n' + self.summary(return_type='str')

    def __getitem__(self, id):
        toret = self.tasks(id=id)
        if toret is None:
            raise KeyError('task {} not found'.format(id))
        return toret[0]

    def __getstate__(self):
        return {'fn': self.fn}

    def __setstate__(self, state):
        self.__init__(state['fn'], base_dir='', create=False, spawn=False)


class BaseApp(BaseClass):

    def __init__(self, func, task_manager=None):
        if isinstance(func, self.__class__):
            self.__dict__.update(func.__dict__)
            return
        self.update(func=func, task_manager=task_manager)

    def update(self, **kwargs):
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
        new = self.copy()
        new.update(**kwargs)
        return new

    def __call__(self, *args, **kwargs):
        return self.task_manager.add(Task(self, args, kwargs))

    def __getstate__(self):
        state = {name: getattr(self, name) for name in ['name', 'code', 'file', 'task_manager']}
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        exec(self.code)
        self.func = locals()[self.name]


def select_modules(modules):
    return set(name.split('.', maxsplit=1)[0] for name in modules if not name.startswith('_'))


_modules = select_modules(sys.modules)


class PythonApp(BaseApp):

    def run(self, args, kwargs):
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
        versions = {}
        for name in select_modules(sys.modules) - _modules:
            try:
                versions[name] = sys.modules[name].__version__
            except (KeyError, AttributeError):
                pass


class BashApp(BaseApp):

    def run(self, args, kwargs):
        errno, result, out, err = 0, None, '', ''
        cmd = self.func(*args, **kwargs)
        proc = subprocess.Popen(cmd.split(' '), shell=True)
        out, err = proc.communicate()
        return errno, result, err, out, {}


class TaskManager(BaseClass):

    def __init__(self, queue, id=None, environ=None, scheduler=None, provider=None):
        self.update(queue=queue, id=id, environ=environ, scheduler=scheduler, provider=provider)

    def update(self, **kwargs):
        for name, func in zip(['queue', 'environ', 'scheduler', 'provider'],
                              [Queue, get_environ, get_scheduler, get_provider]):
            if name in kwargs:
                setattr(self, name, func(kwargs.pop(name)))
        if 'id' in kwargs:
            id = kwargs.pop('id')
            if id is None:
                uid = QueuePickler.dumps((self.environ, self.scheduler, self.provider))
                hex = hashlib.md5(uid).hexdigest()
                id = uuid.UUID(hex=hex)  # unique ID, tied to the given environ, scheduler, provider
            self.id = str(id)
        if kwargs:
            raise ValueError('Unrecognized arguments {}'.format(kwargs))

    def clone(self, **kwargs):
        new = self.copy()
        new.update(**kwargs)
        return new

    def python_app(self, func):
        return PythonApp(func, task_manager=self)

    def bash_app(self, func):
        return BashApp(func, task_manager=self)

    def add(self, task, replace=None):
        return self.queue.add(task, task_manager=self, replace=replace)

    def spawn(self, *args, **kwargs):
        self.scheduler.update(provider=self.provider)
        self.scheduler(*args, **kwargs)


def work(queue, tmid=None, id=None, mpicomm=None):
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


def spawn(queue, timeout=1e4, timestep=2.):
    queues = [queue] if isinstance(queue, Queue) else queue
    t0 = time.time()
    stop = False
    while True:
        if (time.time() - t0) > timeout:
            break
        if all(queue.paused for queue in queues) or stop:
            break
        stop = True
        for queue in queues:
            if queue.paused:
                continue
            for manager in queue.managers():
                ntasks = queue.counts(tmid=manager.id, state=TaskState.PENDING)
                # for task in queue.tasks():
                #     print(task.state, getattr(task, 'err', None))
                if ntasks: stop = False
                # print('desipipe work --queue {} --tmid {}'.format(queue.fn, manager.id))
                # manager.spawn('desipipe work --queue {}'.format(queue.fn), ntasks=ntasks)
                manager.spawn('desipipe work --queue {} --tmid {}'.format(queue.fn, manager.id), ntasks=ntasks)
        time.sleep(timestep * random.uniform(0.8, 1.2))


def get_queue(queue, create=False, spawn=False):
    if isinstance(queue, list):
        toret = []
        for queue in queue:
            tmp = get_queue(queue, create=create, spawn=spawn)
            if isinstance(tmp, Queue):
                toret.append(tmp)
            else:
                toret += tmp
        return toret
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

    logger = logging.getLogger('desipipe')
    from .utils import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    if action == 'queues':

        parser.add_argument('-q', '--queue', type=str, required=False, default='*/*', help='Name of queue; user/queue to select user != {} and e.g. */* to select all queues of all users)'.format(Config.default_user))
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue)
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
        return work(get_queue(args.queue), tmid=args.tmid, id=args.id)

    if action == 'tasks':

        parser.add_argument('-q', '--queue', type=str, required=True, help='Name of queue; user/queue to select user != {}'.format(Config.default_user))
        parser.add_argument('--tmid', type=str, required=False, default=None, help='Task manager ID')
        parser.add_argument('--id', type=str, required=False, default=None, help='Task ID')
        parser.add_argument('--state', type=str, required=False, default=TaskState.FAILED, choices=TaskState.ALL, help='Task state')
        args = parser.parse_args(args=args)
        if '*' in args.queue:
            raise ValueError('Provide single queue!')
        logger.info('Tasks that are {}:'.format(args.state))
        for task in get_queue(args.queue).tasks(state=args.state, tmid=args.tmid, id=args.id):
            for name in ['jobid', 'errno', 'err', 'out']:
                logger.info('{}: {}'.format(name, getattr(task, name)))
            logger.info('=' * 20)
        return

    parser.add_argument('-q', '--queue', nargs='*', type=str, required=True, help='Name of queue; user/queue to select user != {} and e.g. */* to select all queues of all users)'.format(Config.default_user))

    if action == 'delete':

        parser.add_argument('--force', action='store_true', help='Pass this flag to force delete')
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue)
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

    if action == 'pause':

        args = parser.parse_args(args=args)
        queues = get_queue(args.queue)
        for queue in queues:
            logger.info('Pausing queue {}'.format(repr(queue)))
            queue.pause()
        return

    if action == 'resume':

        parser.add_argument('--spawn', action='store_true', help='Spawn a new manager process')
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue)
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
        queues = get_queue(args.queue)
        return spawn(queues, timeout=args.timeout)

    if action == 'retry':

        parser.add_argument('--tmid', type=str, required=False, default=None, help='Task manager ID')
        parser.add_argument('--id', type=str, required=False, default=None, help='Task ID')
        parser.add_argument('--state', type=str, required=False, default=TaskState.KILLED, choices=TaskState.ALL, help='Task state')
        parser.add_argument('--spawn', action='store_true', help='Spawn a new manager process')
        args = parser.parse_args(args=args)
        queues = get_queue(args.queue)
        for queue in queues:
            for id in queue.tasks(tmid=args.tmid, id=args.id, state=args.state, property='id'):
                queue.set_task_state(id, state=TaskState.PENDING)
        if args.spawn:
            subprocess.Popen(['desipipe', 'spawn', '--queue', ' '.join(args.queue)], start_new_session=True, env=os.environ)


action_from_args.actions = {
    'spawn': 'Spawn a manager process to distribute tasks among workers (if queue is not paused)',
    'pause': 'Pause a queue: all workers and managers of provided queues (exclusively) stop after finishing their current task',
    'resume': 'Restart a queue: running manager processes (if any running) distribute again work among workers',
    'delete': 'Delete queue and data base',
    'queues': 'List all queues',
    'tasks': 'List all (failed) tasks of given queue',
    'retry': 'Move all (killed) tasks into PENDING state, so they are rerun'
}