# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import os

import uwsgi_metrics
from pyramid.config import Configurator
from pyramid.tweens import EXCVIEW
from yelp_servlib import config_util
from yelp_servlib import logging_util

import schematizer.config
import schematizer.models.database
from schematizer import healthchecks
from schematizer.config import get_config
from schematizer.helpers.decorators import memoized

SERVICE_CONFIG_PATH = os.environ.get('SERVICE_CONFIG_PATH')
SERVICE_ENV_CONFIG_PATH = os.environ.get('SERVICE_ENV_CONFIG_PATH')


CLUSTERS = [
    ('schematizer', 'master'),
    ('schematizer', 'slave'),
    ('schematizer', 'reporting'),
]


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


try:
    # TODO(DATAPIPE-1506|abrar): Currently we have
    # force_avoid_internal_packages as a means of simulating an absence
    # of a yelp's internal package. And all references
    # of force_avoid_internal_packages have to be removed from
    # schematizer after we have completely ready for open source.
    if get_config().force_avoid_internal_packages:
        raise ImportError
    import yelp_pyramid.healthcheck
    yelp_pyramid.healthcheck.install_healthcheck(
        'mysql',
        healthchecks.MysqlHealthCheck(CLUSTERS),
        unhealthy_threshold=5,
        healthy_threshold=2,
        init=initialize_application
    )
except ImportError:
    pass


def _create_application():
    """Create the WSGI application, post-fork."""

    # Create a basic pyramid Configurator.
    config = Configurator(settings={
        'service_name': 'schematizer',
        'zipkin.tracing_percent': 100,
        'pyramid_swagger.swagger_versions': ['1.2', '2.0'],
        'pyramid_swagger.skip_validation': [
            '/(static)\\b',
            '/(api-docs)\\b',
            '/(status)\\b',
            '/(swagger.json)\\b'
        ],
        'pyramid_yelp_conn.reload_clusters': CLUSTERS
    })

    initialize_application()

    # Add the service's custom configuration, routes, etc.
    config.include(schematizer.config.routes)

    try:
        # TODO(DATAPIPE-1506|abrar): Currently we have
        # force_avoid_internal_packages as a means of simulating an absence
        # of a yelp's internal package. And all references
        # of force_avoid_internal_packages have to be removed from
        # schematizer after we have completely ready for open source.
        if get_config().force_avoid_internal_packages:
            raise ImportError

        import yelp_pyramid
        # Include the yelp_pyramid library default configuration after our
        # configuration so that the yelp_pyramid configuration can base
        # decisions on the service's configuration.
        config.include(yelp_pyramid)

        config.include('pyramid_yelp_conn')
        config.set_yelp_conn_session(schematizer.models.database.session)

        import pyramid_uwsgi_metrics
        # Display metrics on the '/status/metrics' endpoint
        config.include(pyramid_uwsgi_metrics)
    except ImportError:
        config.add_tween(
            "schematizer.schematizer_tweens.db_session_tween_factory",
            under=EXCVIEW
        )

    # Include pyramid_swagger for REST endpoints (see ../api-docs/)
    config.include('pyramid_swagger')

    # Include pyramid_mako for template rendering
    config.include('pyramid_mako')

    # Scan the service package to attach any decorated views.
    config.scan(package='schematizer.views')

    # Including the yelp profiling tween.
    config.include('yelp_profiling')

    return config.make_wsgi_app()


def create_application():
    with logging_util.log_create_application('schematizer_uwsgi'):
        return _create_application()
