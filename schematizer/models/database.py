# -*- coding: utf-8 -*-
from yelp_conn.session import declarative_base
from yelp_conn.session import scoped_session
from yelp_conn.session import sessionmaker


# The common declarative base used by every data model.
Base = declarative_base()
Base.__cluster__ = 'schematizer'

# The single global session manager used to provide sessions through yelp_conn.
session = scoped_session(sessionmaker())
