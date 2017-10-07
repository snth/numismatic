#!/usr/bin/env python

from pathlib import Path
from setuptools import setup

README = Path('README.rst').open('rt').read()
REQUIREMENTS = Path('requirements.txt').open('rt').read().strip().split('\n')
print(REQUIREMENTS)
VERSION = open(Path('VERSION')).read().strip()
print(VERSION)

setup(name='numismatic',
      version=VERSION,
      description='Coin Collector for digital assets ',
      url='http://github.com/snth/numismatic/',
      maintainer='Tobias Brandt',
      maintainer_email='Tobias.Brandt@gmail.com',
      license='MIT',
      keywords='bitcoin,ethereum,cryptocurrencies',
      packages=['numismatic'],
      long_description=README,
      install_requires=REQUIREMENTS,
      python_requires='>=3.6',
      zip_safe=True,
      entry_points='''
          [console_scripts]
          coin=numismatic.cli:coin
          '''
      )
