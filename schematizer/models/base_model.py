# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.database import session


class BaseModel(object):
    """Base class of model classes which contains common simple operations
    (operations that only involve single model class only).
    """

    @classmethod
    def get_by_id(cls, obj_id):
        return session.query(cls).filter(cls.id == obj_id).first()

    @classmethod
    def get_all(cls):
        return session.query(cls).all()
