# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy import desc
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from yelp_lib.containers import dicts

from schematizer.models.consumer import Consumer
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.producer import Producer
from schematizer.models.types.time import build_time_column


def get_latest_schema_by_topic_id(topic_id):
    return session.query(AvroSchema).filter(
        AvroSchema.topic_id == topic_id
    ).order_by(desc(AvroSchema.created_at)).first()


def get_schema_by_schema_id(schema_id):
    return session.query(AvroSchema).filter(AvroSchema.id == schema_id).one()


def list_schemas_by_topic_id(topic_id):
    return session.query(AvroSchema).filter(
        AvroSchema.topic_id == topic_id
    ).all()


class AvroSchemaStatus(object):

    READ_AND_WRITE = 'RW'
    READ_ONLY = 'R'
    DISABLED = 'Disabled'


class AvroSchema(Base):

    __tablename__ = 'avro_schema'

    id = Column(Integer, primary_key=True)

    # The json formatted avro schema.
    avro_schema = Column(Text, nullable=False)

    # Id of the topic that the schema is associated to.
    # It is a foreign key to Topic table.
    topic_id = Column(
        Integer,
        ForeignKey('topic.id'),
        nullable=False
    )

    # The schema_id where this schema is derived from.
    base_schema_id = Column(Integer, ForeignKey('avro_schema.id'))

    producers = relationship(Producer, backref="avro_schema")

    consumers = relationship(Consumer, backref="avro_schema")

    # Schema status: RW (read/write), R (read-only), Disabled
    status = Column(
        Enum(
            AvroSchemaStatus.READ_AND_WRITE,
            AvroSchemaStatus.READ_ONLY,
            AvroSchemaStatus.DISABLED,
            name='status'
        ),
        default=AvroSchemaStatus.READ_AND_WRITE,
        nullable=False
    )

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
        topic_dict = self.topic.to_dict() if self.topic else {}
        avro_schema_dict = {
            'schema_id': self.id,
            'schema': self.avro_schema,
            'status': self.status,
            'topic_id': self.topic_id,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        # Since swagger cannot take null or None value for integer type,
        # here we just simply strip out this field.
        if self.base_schema_id is not None:
            avro_schema_dict['base_schema_id'] = self.base_schema_id,
        return dicts.dict_merge(topic_dict, avro_schema_dict)
