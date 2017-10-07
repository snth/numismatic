#!/usr/bin/env python

from pathlib import Path
from setuptools import setup

#README = Path('README.markdown').read()
README=''
REQUIREMENTS = list(open(Path('requirements.txt')).read().strip().split('\n'))
print(REQUIREMENTS)
VERSION = open(Path('VERSION')).read().strip()
print(VERSION)

setup(name='numismatic',
      version=VERSION,
      description='Coin Collector for digital currencies ',
      url='http://gitlab.com/crypto-currencies/coin/',
      maintainer='Tobias Brandt',
      maintainer_email='Tobias.Brandt@gmail.com',
      license='Apache',
      keywords='bitcoin,ethereum,cryptocurrencies',
      packages=['numismatic'],
      long_description=README,
      install_requires=REQUIREMENTS,
      python_requires='>=3.6',
      zip_safe=False,
      entry_points='''
          [console_scripts]
          coin=numismatic.cli:numisma
          '''
      )
