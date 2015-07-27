# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.avro_schema import AvroSchema
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class Topic(Base):

    __tablename__ = 'topic'
    __table_args__ = (
        UniqueConstraint(
            'name',
            name='topic_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Topic name.
    name = Column(String, nullable=False)

    # The associated source_id for this topic.
    source_id = Column(
        Integer,
        ForeignKey('source.id'),
        nullable=False
    )

    avro_schemas = relationship(AvroSchema, backref="topic")

    _is_pii = Column('is_pii', Integer, nullable=False)

    @property
    def is_pii_schema(self):
        return bool(self._is_pii)

    @is_pii_schema.setter
    def is_pii_schema(self, value):
        if not isinstance(value, bool):
            raise ValueError(
                "Wrong type was found. Type of is_pii_schema should be bool."
            )

        self._is_pii = int(value)

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
        topic_dict = {
            'topic_id': self.id,
            'name': self.name,
            'source': self.source.to_dict(),
            'is_pii_schema': self.is_pii_schema,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
        return topic_dict
