# -*- coding: utf-8 -*-
from schematizer.models.database import session
from schematizer.models.domain import Domain


def mysql_healthcheck():
    with session.connect_begin(ro=True):
        (session.query(Domain).order_by(Domain.id).limit(1)).all()
