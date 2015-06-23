# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.database import Base


class ConsumerGroup(Base):

    __tablename__ = 'consumer_group'
    id = Column(Integer, primary_key=True)
    group_name = Column(String, nullable=False)
    type = Column(String, nullable=False)
    data_target_id = Column(
        Integer,
        ForeignKey('data_target.id'),
        nullable=False
    )
