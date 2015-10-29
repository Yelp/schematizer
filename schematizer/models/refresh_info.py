# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class RefreshInfo(Base):

    __tablename__ = 'refresh_info'
    __table_args__ = (
        UniqueConstraint(
            'table_identifier',
            name='table_id_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Cluster and table the refresh info refers to.
    table_identifier = Column(String, nullable=False)

    # 0 -> Last refresh was successful.
    # Any other number -> row number that last refresh stopped at.
    refresh_status = Column(Integer, nullable=False)

    # Timestamp when this table was last refreshed.
    last_refreshed_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )

    def to_dict(self):
        return {
            'id': self.id,
            'table_identifier': self.table_identifier,
            'refresh_status': self.refresh_status,
            'last_refreshed_at': self.last_refreshed_at
        }
