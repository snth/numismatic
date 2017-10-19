from pathlib import Path
import os
from configparser import ConfigParser

def get_config():
    cwd = os.getcwd()
    for config_file in [Path(cwd) / '.coinrc']:
      config = ConfigParser()
      if config_file.exists():
          config.read(config_file)
          return config

      return None