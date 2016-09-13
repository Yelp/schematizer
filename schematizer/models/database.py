# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.config import get_config


def _get_schematizer_session(topology_path, cluster_name):
    try:
        if get_config().force_avoid_internal_packages:
            # TODO(DATAPIPE-1506|abrar): Currently we have
            # force_avoid_internal_packages as a means of simulating an absence
            # of a yelp's internal package. And all references
            # of force_avoid_internal_packages have to be removed from
            # schematizer after we have completely ready for open source.
            raise ImportError
        from schematizer.models.connections.yelp_connection \
            import get_schematizer_session
        return get_schematizer_session()
    except ImportError:
        from schematizer.models.connections.default_connection \
            import get_schematizer_session
        return get_schematizer_session(
            topology_path=topology_path,
            cluster_name=cluster_name
        )


def _get_declarative_base():
    try:
        if get_config().force_avoid_internal_packages:
            # TODO(DATAPIPE-1506|abrar): Currently we have
            # force_avoid_internal_packages as a means of simulating an absence
            # of a yelp's internal package. And all references
            # of force_avoid_internal_packages have to be removed from
            # schematizer after we have completely ready for open source.
            raise ImportError
        from yelp_conn.session import declarative_base
    except ImportError:
        from sqlalchemy.ext.declarative import declarative_base
    return declarative_base()


# The common declarative base used by every data model.
Base = _get_declarative_base()
Base.__cluster__ = 'schematizer'

# The single global session manager used to provide sessions through yelp_conn.
session = _get_schematizer_session(
    topology_path=get_config().topology_path,
    cluster_name=get_config().schematizer_cluster
)
