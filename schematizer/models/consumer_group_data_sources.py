# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.database import Base


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
