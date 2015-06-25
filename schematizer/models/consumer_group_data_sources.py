# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class ConsumerGroupDataSources(Base):

    __tablename__ = 'consumer_group_data_sources'
    id = Column(Integer, primary_key=True)
    consumer_group_id = Column(
        Integer,
        ForeignKey('consumer_group.id'),
        nullable=False
    )
    data_source_type = Column(String, nullable=False)
    data_source_id = Column(Integer, nullable=False)

    # Timestamp when the entry is created
    created_at = build_time_column(default_now=True, nullable=False)

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )
