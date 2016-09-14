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
        'yelp_servlib',
        'yelp_lib',
        'yelp-profiling',
    ],
    extras_require={
        'internal': [
            'pyramid-yelp-conn',
            'pyramid-uwsgi-metrics',
            'yelp-conn',
            'yelp_pyramid'
        ]
    }
)
