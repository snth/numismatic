from pathlib import Path
import os
from configparser import ConfigParser

def get_all_config():
    """Returns entire dictionary config"""
    for config_file in [Path(os.environ['HOME']) / '.coinrc']:
      config = ConfigParser()
      if config_file.exists():
          config.read(config_file)
          return config

      return None

def get_config(entity_filter=None):
    """Returns config dictionary with key equal to entity_filer"""
    config = get_all_config()
    if (entity_filter is None):
        return config

    return (config[entity_filter] if entity_filter in config else {})