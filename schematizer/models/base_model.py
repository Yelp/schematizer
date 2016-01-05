# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class BaseModel(object):
    """Base class of model classes which contains common simple operations
    (operations that only involve single model class only).
    """

    @classmethod
    def get_by_id(cls, session, obj_id):
        return session.query(cls).filter(cls.id == obj_id).one()

    @classmethod
    def get_all(cls, session):
        return session.query(cls).all()
