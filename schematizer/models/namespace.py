# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.database import Base
from schematizer.models.source import Source
from schematizer.models.types.time import build_time_column


class Namespace(Base):

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

    def to_dict(self):
        return {
            'namespace_id': self.id,
            'name': self.name,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
