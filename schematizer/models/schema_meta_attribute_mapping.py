# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from time import time

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base


class SchemaMetaAttributeMapping(Base, BaseModel):
    """This table stores a snapshot of the current state of all meta attributes
    being enforced for each schema. This table is populated when a schema is
    registered with the schematizer. At that time, it parses through all the
    entries in MetaAttributeMappingStore and finds out all the candidate meta
    attributes to be enforced for this schema and adds a row for each mapping.
    """

    __tablename__ = 'schema_meta_attribute_mapping'

    id = Column(Integer, primary_key=True)

    # schema_id of schema for which meta attributes are required.
    schema_id = Column(Integer, ForeignKey('avro_schema.id'))

    # The schema_id of the meta attribute to be added.
    meta_attr_schema_id = Column(Integer, ForeignKey('avro_schema.id'))

    # Timestamp when the entry is created
    created_at = Column(
        Integer,
        nullable=False,
        default=lambda: int(time())
    )

    # Timestamp when the entry is last updated
    updated_at = Column(
        Integer,
        nullable=False,
        default=lambda: int(time()),
        onupdate=lambda: int(time())
    )
