# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import yaml
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from schematizer.config import get_config
from schematizer.models.connections._scoped_session import _ScopedSession


def get_schematizer_session():
    topology = _read_topology(get_config().topology_path)
    cluster_config = _get_cluster_config(
        topology,
        get_config().schematizer_cluster
    )
    engine = _create_engine(cluster_config)
    return _ScopedSession(sessionmaker(bind=engine))


def _read_topology(topology_path):
    with open(str(topology_path)) as f:
        topology_str = f.read()
    return yaml.load(topology_str)


def _create_engine(config):
    return create_engine(
        'mysql://{db_user}@{db_host}/{db_database}'.format(
            db_user=config['user'],
            db_host=config['host'],
            db_database=config['db']
        )
    )


def _get_cluster_config(topology, cluster_name):
    for topo_item in topology.get('topology'):
        if topo_item.get('cluster') == cluster_name:
            return topo_item['entries'][0]
