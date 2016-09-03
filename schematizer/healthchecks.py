# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import random

from schematizer import models
from schematizer.config import get_config
from schematizer.models.database import session

can_watch_config = False

try:
    if get_config().force_avoid_yelp_conn:
        raise ImportError
    from yelp_conn import load
    can_watch_config = True
except ImportError:
    pass


class MysqlHealthCheck(object):

    def __init__(
        self,
        clusters,
        min_reload_interval_s=5,
        max_reload_interval_s=120
    ):
        self.clusters = clusters
        self.reload_interval_s = random.randint(
            min_reload_interval_s,
            max_reload_interval_s
        )
        self.watcher = None

    def get_watcher(self):
        # Lazily instantiate the watcher because the yelp_conn staticconf
        # namespace needs to be loaded.  Do this in the `init` function that
        # you pass to `yelp_pyramid.healthcheck.install_healthcheck`.
        if self.watcher is None:
            self.watcher = load.build_config_watcher(
                self.reload_interval_s,
                self.clusters
            )
        return self.watcher

    def __call__(self, *args, **kwargs):

        if can_watch_config:
            self.get_watcher().reload_if_changed()

        with session.connect_begin(ro=True):
            session.query(models.Namespace).order_by(
                models.Namespace.id
            ).limit(1).all()
