from pathlib import Path
import os
from configparser import ConfigParser

def get_config():
    for config_file in [Path(os.environ['HOME']) / '.coinrc']:
      config = ConfigParser()
      if config_file.exists():
          config.read(config_file)
          return config

      return None
