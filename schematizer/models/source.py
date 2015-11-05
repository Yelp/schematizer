# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.database import Base
from schematizer.models.source_category import SourceCategory
from schematizer.models.topic import Topic
from schematizer.models.refresh import Refresh
from schematizer.models.types.time import build_time_column


class Source(Base):

    __tablename__ = 'source'
    __table_args__ = (
        UniqueConstraint(
            'name',
            'namespace_id',
            name='name_namespace_id_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Source of the Avro schema, such as table "User",
    # or log "service.foo" etc.
    name = Column(String, nullable=False)

    # Email address of the source owner.
    owner_email = Column(String, nullable=False)

    namespace_id = Column(
        Integer,
        ForeignKey('namespace.id'),
        nullable=False
    )

    topics = relationship(Topic, backref="source")

    refreshes = relationship(Refresh, backref="source")

    # The category relationship object for this source.
    # A source matches 1-to-1 with a SourceCategory.
    category = relationship(SourceCategory, uselist=False, backref="source")

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
            'source_id': self.id,
            'name': self.name,
            'owner_email': self.owner_email,
            'namespace': self.namespace.to_dict(),
            'category': (
                None if self.category is None else self.category.category
            ),
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
