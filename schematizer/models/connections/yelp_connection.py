# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker


def get_schematizer_session(**kwargs):
    return scoped_session(sessionmaker())
