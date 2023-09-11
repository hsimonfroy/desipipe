import os
from getpass import getuser

from .utils import BaseDict
from .file_manager import yaml_parser


class Config(BaseDict):
    """
    desipipe configuration ('config.yaml') is saved
    under 'DESIPIPE_CONFIG_DIR' environment variable if defined, else '~/.desipipe'.
    """
    default_user = getuser()
    home_dir = os.path.expanduser('~')

    def __init__(self, user=None):
        """
        Read configuration.

        Parameters
        ----------
        user : str, default=None
            User. Defaults to :func`getuser`.
        """
        if user is None:
            user = self.default_user
        self.user = str(user)
        self.data = {}

        self.config_dir = os.getenv('DESIPIPE_CONFIG_DIR',  '')
        default_config_dir = os.path.join(self.home_dir, '.desipipe')
        if not self.config_dir:
            self.config_dir = default_config_dir

        if os.path.isfile(self.config_fn):
            with open(self.config_fn, 'r') as file:
                self.data = yaml_parser(file.read())[0]
            try:
                with open(self.config_fn, 'a'): pass
            except PermissionError:  # from now on, write to home
                self.config_dir = default_config_dir
        self.setdefault('base_queue_dir', os.getenv('DESIPIPE_QUEUE_DIR', os.path.join(default_config_dir, 'queues')))

    @property
    def config_fn(self):
        """Path to .yaml configuration file."""
        return os.path.join(self.config_dir, 'config.yaml')

    @property
    def queue_dir(self):
        """Queue directory."""
        return os.path.join(self['base_queue_dir'], self.user)