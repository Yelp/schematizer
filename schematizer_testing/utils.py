# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.database import session


def get_entity_by_id(entity_cls, entity_id):
    return session.query(entity_cls).filter(
        getattr(entity_cls, 'id') == entity_id
    ).one()
