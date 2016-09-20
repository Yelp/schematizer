# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.database import session


def get_entity_by_id(entity_cls, entity_id):
    return session.query(entity_cls).filter(
        getattr(entity_cls, 'id') == entity_id
    ).one()


def get_entity_by_kwargs(entity_cls, **filter_kwargs):
    query = session.query(entity_cls)
    for col in filter_kwargs:
        query.filter(getattr(entity_cls, col) == filter_kwargs[col])
    return query.all()
