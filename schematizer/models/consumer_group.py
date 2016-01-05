# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.orm import relationship

from schematizer.models.base_model import BaseModel
from schematizer.models.consumer_group_data_source \
    import ConsumerGroupDataSource
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class ConsumerGroup(Base, BaseModel):

    __tablename__ = 'consumer_group'

    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False, unique=True)
    data_target_id = Column(
        Integer,
        ForeignKey('data_target.id'),
        nullable=False
    )

    data_sources = relationship(
        ConsumerGroupDataSource,
        backref="consumer_group"
    )

    # Timestamp when the entry is created
    created_at = build_time_column(default_now=True, nullable=False)

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )
