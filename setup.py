#!/usr/bin/env python
from setuptools import setup, find_packages

setup(name='tbsU',
      version='0.1',
      description='Package of utilities designed and heavily used by Tyler Bodine-Smith',
      url='tylerbs.com',
      author='Tyler Bodine-Smith',
      author_email='tbs@tylerbs.com',
      license='AGPLv3',
      packages=find_packages(),
      install_requires=[
        'tabulate',
        'pandas',
        'numpy',
        'sqlalchemy',
      ],
      classifiers=[
        "Programming Language :: Python :: 3.6",
        "License :: OSI Approved :: MIT License",
      ]
      )
