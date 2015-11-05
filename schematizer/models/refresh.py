# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy.types import Enum

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class RefreshStatus(object):

    NOT_STARTED = 'Not Started'
    IN_PROGRESS = 'In Progress'
    PAUSED = 'Paused'
    SUCCESS = 'Success'
    FAILED = 'Failed'


class Refresh(Base):

    __tablename__ = 'refresh'

    id = Column(Integer, primary_key=True)

    source_id = Column(
        Integer,
        ForeignKey('source.id'),
        nullable=False
    )

    status = Column(
        Enum(
            RefreshStatus.NOT_STARTED,
            RefreshStatus.IN_PROGRESS,
            RefreshStatus.PAUSED,
            RefreshStatus.SUCCESS,
            RefreshStatus.FAILED,
            name='status'
        ),
        nullable=False
    )

    offset = Column(Integer, nullable=False)

    batch_size = Column(Integer, nullable=False)

    priority = Column(Integer, nullable=False)

    where = Column(String, nullable=False)

    created_at = build_time_column(
        default_now=True,
        nullable=False
    )

    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )

    def to_dict(self):
        return {
            'refresh_id': self.id,
            'source': self.source.to_dict(),
            'status': self.status,
            'offset': self.offset,
            'batch_size': self.batch_size,
            'priority': self.priority,
            'where': self.where,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
