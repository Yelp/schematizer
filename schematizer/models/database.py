# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.connections import get_connection_obj


connection_object = get_connection_obj()


# The common declarative base used by every data model.
Base = connection_object.get_base_model()
Base.__cluster__ = 'schematizer'

# The single global session manager used to provide sessions through yelp_conn.
session = connection_object.get_tracker_session()
