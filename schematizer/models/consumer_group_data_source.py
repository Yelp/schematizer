# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.types import Enum

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class ConsumerGroupType(object):

    NAMESPACE = 'Namespace'
    SOURCE = 'Source'
    SCHEMA = 'Schema'


class ConsumerGroupDataSource(Base):

    __tablename__ = 'consumer_group_data_source'
    id = Column(Integer, primary_key=True)
    consumer_group_id = Column(
        Integer,
        ForeignKey('consumer_group.id'),
        nullable=False
    )

    # The level that this consumer group is interested in.
    # Value from ConsumerGroupType.
    data_source_type = Column(
        Enum(
            ConsumerGroupType.NAMESPACE,
            ConsumerGroupType.SOURCE,
            ConsumerGroupType.SCHEMA,
            name='consumer_group_type'
        )
    )

    # The id of the data_source_type entry in its corresponding table.
    data_source_id = Column(Integer, nullable=False)

    # Timestamp when the entry is created
    created_at = build_time_column(default_now=True, nullable=False)

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )
