# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy.orm import exc as orm_exc

from schematizer.models.exceptions import EntityNotFoundError


class BaseModel(object):
    """Base class of model classes which contains common simple operations
    (operations that only involve single model class only).
    """

    @classmethod
    def get_by_id(cls, session, obj_id):
        try:
            return session.query(cls).filter(cls.id == obj_id).one()
        except orm_exc.NoResultFound:
            raise EntityNotFoundError(
                entity_cls=cls,
                message='{} id {} not found.'.format(cls.__name__, obj_id)
            )

    @classmethod
    def get_all(cls, session):
        return session.query(cls).all()
