#!/usr/bin/env python

from pathlib import Path
from setuptools import setup, find_packages

README = Path('README.rst').open('rt').read()
REQUIREMENTS = Path('requirements.txt').open('rt').read().strip().split('\n')
VERSION = open(Path('VERSION.txt')).read().strip()

setup(name='numismatic',
      version=VERSION,
      description='Coin Collector for digital assets ',
      url='http://github.com/snth/numismatic/',
      maintainer='Tobias Brandt',
      maintainer_email='Tobias.Brandt@gmail.com',
      license='MIT',
      keywords='bitcoin,ethereum,cryptocurrencies',
      packages=find_packages(),
      package_data={
          '': ['*.txt', '*.md', '*.rst'],
          },
      long_description=README,
      install_requires=REQUIREMENTS,
      python_requires='~=3.6',
      extras_require={
        'SQL': ['sqlalchemy'],
        },
      zip_safe=False,
      entry_points='''
          [console_scripts]
          coin=numismatic.cli:coin
          ''',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'License :: OSI Approved :: MIT License',
          'Programming Language :: Python',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: Implementation :: CPython',
          ],
      )
