# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer import models
from schematizer.models.database import session


class MysqlHealthCheck(object):

    def __init__(
        self,
        clusters,
        min_reload_interval_s=5,
        max_reload_interval_s=120
    ):
        self.clusters = clusters

    def __call__(self, *args, **kwargs):
        with session.connect_begin(ro=True):
            session.query(models.Namespace).order_by(
                models.Namespace.id
            ).limit(1).all()
