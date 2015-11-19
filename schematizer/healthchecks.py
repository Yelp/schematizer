# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer import models
from schematizer.models.database import session


def mysql_healthcheck():
    with session.connect_begin(ro=True):
        session.query(models.Namespace).order_by(
            models.Namespace.id
        ).limit(1).all()
