# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy import desc
from sqlalchemy.orm import relationship

from yelp_lib.containers import dicts

from schematizer.models.avro_schema import AvroSchema
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.types.time import build_time_column


def get_latest_topic_by_source_id(source_id):
    latest_topic = session.query(Topic).filter(
        Topic.domain_id == source_id
    ).order_by(desc(Topic.created_at)).first()
    return latest_topic


def get_topic_by_topic_name(topic_name):
    return session.query(Topic).filter(Topic.topic == topic_name).one()


def list_topics_by_source_id(source_id):
        return session.query(Topic).filter(Topic.domain_id == source_id).all()


class Topic(Base):

    __tablename__ = 'topic'
    __table_args__ = (
        UniqueConstraint(
            'topic',
            name='topic_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Topic name.
    topic = Column(String, nullable=False)

    # The associated domain_id for this topic.
    domain_id = Column(
        Integer,
        ForeignKey('domain.id'),
        nullable=False
    )

    avro_schemas = relationship(AvroSchema, backref="topic")

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
        domain_dict = self.domain.to_dict() if self.domain else {}
        topic_dict = {
            'topic_id': self.id,
            'topic': self.topic,
            'source_id': self.domain_id,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
        return dicts.dict_merge(domain_dict, topic_dict)
