# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.orm import relationship

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.source import Source
from schematizer.models.types.time import build_time_column


class Namespace(Base, BaseModel):

    __tablename__ = 'namespace'
    __table_args__ = (
        UniqueConstraint(
            'name',
            name='namespace_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Namespace, such as "yelpmain.db", etc
    name = Column(String, nullable=False)

    sources = relationship(Source, backref="namespace")

    # Timestamp when the entry is created
    created_at = build_time_column(
        default_now=True,
        nullable=False
    )

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )

    @classmethod
    def get_by_name(cls, name):
        try:
            return session.query(Namespace).filter(cls.name == name).one()
        except orm_exc.NoResultFound:
            raise EntityNotFoundError(
                entity_desc='{} name `{}`'.format(cls.__name__, name)
            )

    def get_sources(self, page_info=None):
        qry = session.query(
            Source
        ).filter(Source.namespace_id == self.id)
        if page_info and page_info.min_id:
            qry = qry.filter(
                Source.id >= page_info.min_id
            )
        qry = qry.order_by(Source.id)
        if page_info and page_info.count:
            qry = qry.limit(page_info.count)
        return qry.all()
