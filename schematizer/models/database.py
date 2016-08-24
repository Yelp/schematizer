# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy.ext.declarative import declarative_base

from schematizer.models.connections.default_connection \
    import get_schematizer_session


# The common declarative base used by every data model.
Base = declarative_base()

# The single global session manager used to provide sessions through yelp_conn.
session = get_schematizer_session()
