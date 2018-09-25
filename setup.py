#!/usr/bin/env python
#  -*- coding: utf-8 -*-
from __future__ import print_function

import codecs
from glob import glob

from setuptools import find_packages, setup

import pyathena

with codecs.open('README.rst', 'rb', 'utf-8') as readme:
    long_description = readme.read()


setup(
    name='PyAthena',
    version=pyathena.__version__,
    description='Python DB API 2.0 (PEP 249) compliant client for Amazon Athena',
    long_description=long_description,
    url='https://github.com/laughingman7743/PyAthena/',
    author='laughingman7743',
    author_email='laughingman7743@gmail.com',
    license='MIT License',
    packages=find_packages(),
    package_data={
        '': ['LICENSE', '*.rst', 'Pipfile*'],
    },
    include_package_data=True,
    data_files=[
        ('', ['LICENSE'] + glob('*.rst') + glob('Pipfile*')),
    ],
    install_requires=[
        'future',
        'futures;python_version=="2.7"',
        'botocore>=1.5.52',
        'boto3>=1.4.4',
        'tenacity>=4.1.0',
    ],
    extras_require={
        'Pandas': ['pandas>=0.19.0'],
        'SQLAlchemy': ['SQLAlchemy>=1.0.0'],
    },
    tests_require=[
        'SQLAlchemy>=1.0.0',
        'pytest>=3.5',
        'pytest-cov',
        'pytest-flake8>=1.0.1',
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'awsathena.rest = pyathena.sqlalchemy_athena:AthenaDialect',
        ],
    },
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Topic :: Database :: Front-Ends',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],
)
