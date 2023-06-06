import os
from getpass import getuser

from .utils import BaseDict
from .file_manager import yaml_parser


class Config(BaseDict):

    default_user = getuser()
    home_dir = os.path.expanduser('~')

    def __init__(self, user=None):
        if user is None:
            user = self.default_user
        self.user = str(user)

        self.config_dir = os.getenv('DESIPIPE_CONFIG_DIR', None)
        default_config_dir = os.path.join(self.home_dir, '.desipipe')
        if not self.config_dir:
            self.config_dir = default_config_dir

        config_fn = {}
        if os.path.isfile(self.config_fn):
            config_fn = self.config_fn
            try:
                with open(self.config_fn, 'a'): pass
            except PermissionError:  # from now on, write to home
                self.config_dir = default_config_dir
        self.data = yaml_parser(config_fn)[0]
        self.setdefault('base_queue_dir', os.path.join(self.config_dir, 'queues'))

    @property
    def config_fn(self):
        """Path to .yaml configuration file."""
        return os.path.join(self.config_dir, 'config.yaml')

    @property
    def queue_dir(self):
        return os.path.join(self['base_queue_dir'], self.user)