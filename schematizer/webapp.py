# -*- coding: utf-8 -*-

import os

import pyramid_uwsgi_metrics
import uwsgi_metrics
import yelp_pyramid
from yelp_servlib import logging_util
from yelp_servlib import config_util
from pyramid.config import Configurator

import schematizer.config

SERVICE_CONFIG_PATH = os.environ.get('SERVICE_CONFIG_PATH')
SERVICE_ENV_CONFIG_PATH = os.environ.get('SERVICE_ENV_CONFIG_PATH')

uwsgi_metrics.initialize()


def _create_application():
    """Create the WSGI application, post-fork."""

    # Create a basic pyramid Configurator.
    config = Configurator(settings={
        'service_name': 'schematizer',
        'pyramid_swagger.skip_validation': [
            '/(static)\\b',
            '/(api-docs)\\b',
            '/(status)\\b'
        ],
        'pyramid_yelp_conn.reload_clusters': [
            ('schematizer', 'master'),
            ('schematizer', 'slave'),
        ],
    })

    config_util.load_default_config(
        SERVICE_CONFIG_PATH,
        SERVICE_ENV_CONFIG_PATH)

    # Add the service's custom configuration, routes, etc.
    config.include(schematizer.config.routes)

    # Include the yelp_pyramid library default configuration after our
    # configuration so that the yelp_pyramid configuration can base decisions
    # on the service's configuration.
    config.include(yelp_pyramid)
    config.include('pyramid_yelp_conn')
    config.set_yelp_conn_session(schematizer.database.session)

    config.include('pyramid_swagger')

    # Display metrics on the '/status/metrics' endpoint
    config.include(pyramid_uwsgi_metrics)

    # Scan the service package to attach any decorated views.
    config.scan('schematizer')

    return config.make_wsgi_app()


def create_application():
    with logging_util.log_create_application('schematizer_uwsgi'):
        return _create_application()
