# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.database import Base


class DataTarget(Base):

    __tablename__ = 'data_target'
    id = Column(Integer, primary_key=True)
    target_type = Column(String, nullable=False)
    destination = Column(String, nullable=False)
