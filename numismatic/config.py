from pathlib import Path
import os
from configparser import ConfigParser

from appdirs import user_config_dir

# Generally the config is only read at import time but we provide read_config(
# as a convenience so the user can reread the configuration at runtime.
def read_config():
    '''Reads the current configuration'''
    config = ConfigParser()
    for config_file in [Path(__file__).parent / 'numismatic.ini',
                        Path(user_config_dir()) / 'numismatic.ini']:
        if config_file.exists():
            config.read(config_file)
    return config


# we instantiate one copy of the config at import time to keep a consistent
# configuration state across the application.
config = read_config()


def get_config_item(item, default=None, section='DEFAULT', config=config):
    return config[section][item]

def config_item_getter(section, item, default=None, config=config):
    def _get_config_item():
        return get_config_item(item=item, default=default, section=section,
                               config=config)
    return _get_config_item


class ConfigMixin:

    @classmethod
    def get_config(cls, config=config):
        return config[cls.__name__]

    @classmethod
    def get_config_item(cls, item, default=None, config=config):
        return get_config_item(item=item, default=default,
                               section=cls.__name__, config=config)
