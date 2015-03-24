# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.topic import Topic
from schematizer.models.types.time import build_time_column


def get_source_by_namespace_and_source_name(namespace, source_name):
    return session.query(Domain).filter(
        Domain.namespace == namespace,
        Domain.source == source_name
    ).one()


def get_source_by_source_id(source_id):
    return session.query(Domain).filter(Domain.id == source_id).one()


def list_all_namespaces():
    query_results = session.query(Domain.namespace).distinct(
        Domain.namespace
    ).all()
    return [query_result[0] for query_result in query_results]


def list_all_sources():
    return session.query(Domain).all()


def list_sources_by_namespace(namespace):
    return session.query(Domain).filter(Domain.namespace == namespace).all()


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

    def to_dict(self):
        return {
            'source_id': self.id,
            'namespace': self.namespace,
            'source': self.source,
            'source_owner_email': self.owner_email,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }
