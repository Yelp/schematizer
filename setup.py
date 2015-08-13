#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

setup(
    name='schematizer',
    author='BAM',
    author_email='bam@yelp.com',
    license='Copyright Yelp 2015',
    packages=find_packages(exclude=['tests*']),
    install_requires=[
        'uwsgi',
        'pyramid',
        'pyramid_uwsgi_metrics',
        'pyramid_yelp_conn',
        'yelp_pyramid',
        'yelp_servlib',
        'yelp_conn',
        'yelp_lib',
    ]
)
