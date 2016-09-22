# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.base_model import BaseModel
from schematizer.models.consumer_group import ConsumerGroup
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class DataTarget(Base, BaseModel):

    __tablename__ = 'data_target'
    __table_args__ = (
        UniqueConstraint(
            'name',
            name='name_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Data target name.
    name = Column(String, nullable=False)

    target_type = Column(String, nullable=False)
    destination = Column(String, nullable=False)

    consumer_groups = relationship(ConsumerGroup, backref="data_target")

    # Timestamp when the entry is created
    created_at = build_time_column(default_now=True, nullable=False)

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )
