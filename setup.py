#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

if sys.argv[-1] == 'publish':
    os.system('python setup.py sdist upload')
    sys.exit()

readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

setup(
    name='esit',
    version='0.1.1',
    description='ElasticSearch Index Tools. Tools for duplicating, migrating, and experimenting with changes to ElasticSearch indexes and mappings.',
    long_description=readme + '\n\n' + history,
    author='Paul Bonser',
    author_email='paul@marketvibe.com',
    url='https://github.com/MarketVibe/esit',
    packages=[
        'esit', 'esit.commands',
    ],
    package_dir={'esit': 'esit'},
    include_package_data=True,
    install_requires=[
        'docopt',
        'pyelasticsearch',
        'progress',
        'jinja2',
    ],
    license="BSD",
    zip_safe=False,
    keywords='esit',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
    ],
    entry_points={
        'console_scripts': ['esit = esit.commands.main:run']
    },
    test_suite='tests',
)
