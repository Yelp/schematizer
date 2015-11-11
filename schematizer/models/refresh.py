# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from enum import Enum
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class Priority(Enum):

    LOW = 25
    MEDIUM = 50
    HIGH = 75
    MAX = 100


class RefreshStatus(Enum):

    NOT_STARTED = 0
    IN_PROGRESS = 1
    PAUSED = 2
    SUCCESS = 3
    FAILED = 4


class Refresh(Base):

    __tablename__ = 'refresh'

    id = Column(Integer, primary_key=True)

    source_id = Column(
        Integer,
        ForeignKey('source.id'),
        nullable=False
    )

    status = Column(
        Integer,
        default=RefreshStatus.NOT_STARTED.value,
        nullable=False
    )

    # Represents the last known position that has been refreshed.
    offset = Column(Integer, default=0, nullable=False)

    batch_size = Column(Integer, default=100, nullable=False)

    priority = Column(
        Integer,
        default=Priority.MEDIUM.value,
        nullable=False
    )

    # This field contains the expression used to filter the records
    # that must be refreshed. E.g. It may be a MySQL where clause
    # if the source of the refresh is a MySQL table.
    filter_condition = Column(String, default=None)

    created_at = build_time_column(
        default_now=True,
        nullable=False
    )

    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )

