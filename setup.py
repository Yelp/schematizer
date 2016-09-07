#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from setuptools import find_packages
from setuptools import setup

setup(
    name='schematizer',
    author='BAM',
    author_email='bam@yelp.com',
    license='Copyright Yelp 2015',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'uwsgi',
        'pyramid',
        'pyramid_yelp_conn',
        'yelp_pyramid',
        'yelp_servlib',
        'yelp_conn',
        'yelp_lib',
        'yelp-profiling',
    ],
    extras_require={
        'internal': [
            'pyramid_uwsgi_metrics'
        ]
    }
)
