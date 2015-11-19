# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import pyramid_uwsgi_metrics
import uwsgi_metrics
import yelp_pyramid
import yelp_pyramid.healthcheck
from pyramid.config import Configurator
from yelp_lib.decorators import memoized
from yelp_servlib import config_util
from yelp_servlib import logging_util

import schematizer.config
import schematizer.models.database
from schematizer import healthchecks

SERVICE_CONFIG_PATH = os.environ.get('SERVICE_CONFIG_PATH')
SERVICE_ENV_CONFIG_PATH = os.environ.get('SERVICE_ENV_CONFIG_PATH')

uwsgi_metrics.initialize()


@memoized
def initialize_application():
    """Initialize required configuration variables. Note that it is important
    for this to be `@memoized` as it has caused problems when it happens
    repeatedly (such as during the healthcheck, see DATAPIPE-360)
    """
    config_util.load_default_config(
        SERVICE_CONFIG_PATH,
        SERVICE_ENV_CONFIG_PATH
    )


yelp_pyramid.healthcheck.install_healthcheck(
    'mysql',
    healthchecks.mysql_healthcheck,
    unhealthy_threshold=5,
    healthy_threshold=2,
    init=initialize_application
)


def _create_application():
    """Create the WSGI application, post-fork."""

    # Create a basic pyramid Configurator.
    config = Configurator(settings={
        'service_name': 'schematizer',
        'pyramid_swagger.skip_validation': [
            '/(static)\\b',
            '/(api-docs)\\b',
            '/(status)\\b',
            '/(web)\\b'
        ],
        'pyramid_yelp_conn.reload_clusters': [
            ('schematizer', 'master'),
            ('schematizer', 'slave'),
        ],
    })

    initialize_application()

    # Add the service's custom configuration, routes, etc.
    config.include(schematizer.config.routes)

    # Include the yelp_pyramid library default configuration after our
    # configuration so that the yelp_pyramid configuration can base decisions
    # on the service's configuration.
    config.include(yelp_pyramid)
    config.include('pyramid_yelp_conn')
    config.set_yelp_conn_session(schematizer.models.database.session)

    # Include pyramid_swagger for REST endpoints (see ../api-docs/)
    config.include('pyramid_swagger')

    # Include pyramid_mako for template rendering
    config.include('pyramid_mako')

    # Display metrics on the '/status/metrics' endpoint
    config.include(pyramid_uwsgi_metrics)

    # Scan the service package to attach any decorated views.
    config.scan('schematizer')

    return config.make_wsgi_app()


def create_application():
    with logging_util.log_create_application('schematizer_uwsgi'):
        return _create_application()
