# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class Namespace(Base):

    __tablename__ = 'namespace'

    id = Column(Integer, primary_key=True)

    # Namespace, such as "yelpmain.db", etc
    namespace = Column(String, nullable=False)

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
