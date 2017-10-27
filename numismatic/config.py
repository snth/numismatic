from pathlib import Path
import os
from configparser import ConfigParser

from appdirs import user_config_dir

def read_config():
    '''Reads the current configuration'''
    config = ConfigParser()
    for config_file in [Path(__file__).parent / 'numismatic.ini',
                        Path(user_config_dir()) / 'numismatic.ini']:
        if config_file.exists():
            config.read(config_file)
    return config


# Generally the config is only read at import time but we provide read_config(
# as a convenience so the user can reread the configuration at runtime.
config = read_config()
