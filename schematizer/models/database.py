# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.config import get_config


def _get_schematizer_session():
    try:
        if get_config().force_avoid_yelp_conn:
            raise ImportError
        from schematizer.models.connections.yelp_connection \
            import get_schematizer_session
    except ImportError:
        from schematizer.models.connections.default_connection \
            import get_schematizer_session

    return get_schematizer_session()


def _get_base_model():
    try:
        if get_config().force_avoid_yelp_conn:
            raise ImportError
        from yelp_conn.session import declarative_base
    except ImportError:
        from sqlalchemy.ext.declarative import declarative_base

    return declarative_base()


# The common declarative base used by every data model.
Base = _get_base_model()
Base.__cluster__ = 'schematizer'

# The single global session manager used to provide sessions through yelp_conn.
session = _get_schematizer_session()
