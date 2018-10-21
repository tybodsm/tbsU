#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='tbsU',
      version='0.1',
      description='Package of utilities designed and heavily used by Tyler Bodine-Smith',
      url='tylerbs.com',
      author='Tyler Bodine-Smith',
      author_email='tbs@tylerbs.com',
      license='MIT',
      packages=find_packages(),
      install_requires=[
          'tabulate',
          'pandas',
          'numpy',
          'sqlalchemy',
      ],
      )
