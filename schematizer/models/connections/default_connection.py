# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from contextlib import contextmanager

import yaml
from cached_property import cached_property
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.scoping import ScopedSession

from schematizer.config import get_config
from schematizer.helpers.singleton import Singleton


class DefaultConnection(object):

    __metaclass__ = Singleton

    def __init__(self):
        with open(str(get_config().topology_path)) as f:
            topology = f.read()
        self.db_config = yaml.load(topology)

    def _get_engine(self, config):
        return create_engine(
            'mysql://{db_user}@{db_host}/{db_database}'.format(
                db_user=config['user'],
                db_host=config['host'],
                db_database=config['db']
            )
        )

    def get_base_model(self):
        return declarative_base()

    def get_tracker_session(self):
        config = self.schematizer_database_config
        return _RHScopedSession(sessionmaker(bind=self._get_engine(config)))

    def _get_cluster_config(self, cluster_name):
        for topo_item in self.db_config.get('topology'):
            if topo_item.get('cluster') == cluster_name:
                return topo_item['entries'][0]

    @cached_property
    def schematizer_database_config(self):
        return self._get_cluster_config(
            get_config().schematizer_cluster
        )


class _RHScopedSession(ScopedSession):
    """This is a custom subclass of ``sqlalchemy.orm.scoping.ScopedSession``
    that is returned from ``scoped_session``. Use ``scoped_session`` rather
    than this.

    This passes through most functions through to the underlying session.
    """
    @contextmanager
    def connect_begin(self, *args, **kwargs):
        session = self()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()
            self.remove()
