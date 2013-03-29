#!/usr/bin/env python
"""
Setup file for DBWrapper.

"""
from distutils.core import setup

setup(
      name='DBWrapper',
      version='1.1',
      description='Thread-safe wrapper for Python sqlite3 bindings.',
      author='Sven Festersen',
      author_email='sven@sven-festersen.de',
      url='https://github.com/SvenFestersen/DBWrapper',
      packages=['dbwrapper'],
     )
