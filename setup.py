#!/usr/bin/env python
#  -*- coding: utf-8 -*-
from __future__ import print_function
import codecs

from setuptools import setup

import pyathena


_PACKAGE_NAME = 'PyAthena'


with codecs.open('README.rst', 'rb', 'utf-8') as readme:
    long_description = readme.read()


setup(
    name=_PACKAGE_NAME,
    version=pyathena.__version__,
    description='Python DB API 2.0 (PEP 249) compliant client for Amazon Athena',
    long_description=long_description,
    url='https://github.com/laughingman7743/PyAthena/',
    author='laughingman7743',
    author_email='laughingman7743@gmail.com',
    license='MIT License',
    packages=[_PACKAGE_NAME.lower()],
    package_data={
        '': ['*.rst'],
    },
    install_requires=[
        'future',
        'botocore>=1.5.52',
        'boto3>=1.4.4',
        'tenacity>=4.1.0',
    ],
    extras_require={
        'Pandas': ['pandas>=0.19.0'],
        'SQLAlchemy': ['SQLAlchemy>=1.0.0'],
    },
    tests_require=[
        'futures',
        'SQLAlchemy>=1.0.0',
        'pytest',
        'pytest-cov',
        'pytest-flake8',
        'pytest-catchlog',
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
