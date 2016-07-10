# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy.orm import exc as orm_exc

from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError


class BaseModel(object):
    """Base class of model classes which contains common simple operations
    (operations that only involve single model class only).

    These functions only work when they are inside the request context manager.
    See http://servicedocs/docs/yelp_conn/session.html.
    """

    @classmethod
    def get_by_id(cls, obj_id):
        try:
            # starting sqlalchemy 1.0.9, it can use one_or_none so that here
            # it doesn't need to catch the exception and re-throw our custom
            # excpetion. See http://docs.sqlalchemy.org/en/latest/orm/
            # query.html#sqlalchemy.orm.query.Query.one_or_none
            return session.query(cls).filter(cls.id == obj_id).one()
        except orm_exc.NoResultFound:
            raise EntityNotFoundError(
                entity_cls=cls,
                message='{} id {} not found.'.format(cls.__name__, obj_id)
            )

    @classmethod
    def get_all(cls):
        return session.query(cls).all()

    @classmethod
    def create(cls, session, **kwargs):
        """Create this entity in the database.  Note this function will call
        `session.flush()`, so do not use this function if there are other
        operations that need to happen before the flush is called.

        Args:
            session (:class:yelp_conn.session.YelpConnScopedSession) global
                session manager used to provide sessions.
            kwargs (dict): pairs of model attributes and their values.

        Returns:
            :class:schematizer.models.[cls]: object that is newly created in
            the database.
        """
        entity = cls(**kwargs)
        session.add(entity)
        session.flush()
        return entity
