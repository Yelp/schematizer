# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class SchemaMetaAttributeMapping(Base, BaseModel):

    __tablename__ = 'schema_meta_attribute_mapping'

    # schema_id of schema for which meta attributes are required.
    schema_id = Column(Integer, ForeignKey('avro_schema.id'))

    # The schema_id of the meta attribute to be added.
    meta_attr_schema_id = Column(Integer, ForeignKey('avro_schema.id'))

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
