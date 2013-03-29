#!/usr/bin/env python
"""
Setup file for DBWrapper.

"""
import os
from distutils.core import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
      name='DBWrapper',
      version='1.1',
      description='Thread-safe wrapper for Python sqlite3 bindings.',
      author='Sven Festersen',
      author_email='sven@sven-festersen.de',
      url='https://github.com/SvenFestersen/DBWrapper',
      packages=['dbwrapper'],
      license='GPL',
      keywords='database sqlite threading',
      long_description=read('README'),
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: Developers',
          'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
          'Operating System :: OS Independent',
          'Topic :: Software Development'
      ],
     )
