# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.database import Base
from schematizer.models.topic import Topic
from schematizer.models.types.time import build_time_column


class Domain(Base):

    __tablename__ = 'domain'
    __table_args__ = (
        UniqueConstraint(
            'namespace',
            'source',
            name='namespace_source_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Namespace of the source, such as "yelpmain.db", etc
    namespace = Column(String, nullable=False)

    # Source of the Avro schema, such as table "User",
    # or log "service.foo" etc.
    source = Column(String, nullable=False)

    # Email address of the source owner.
    owner_email = Column(String, nullable=False)

    topics = relationship(Topic, backref="domain")

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
