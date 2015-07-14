# -*- coding: utf-8 -*-
from schematizer.models.database import session
from schematizer import models


def mysql_healthcheck():
    with session.connect_begin(ro=True):
        session.query(models.Namespace).order_by(
            models.Namespace.id
        ).limit(1).all()
