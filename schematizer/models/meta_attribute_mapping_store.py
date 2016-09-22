# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.orm import exc as orm_exc
from sqlalchemy.types import Enum

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.types.time import build_time_column


class MetaAttributeMappingStore(Base, BaseModel):
    """This table stores all the mappings of meta attribute registered for
    each entity. The entities can be Namespace, Source or AvroSchema. This
    table is a source of truth for all mappings currently active in the Data
    Pipeline. Rows are modified in this table by hitting the
    meta_attribute_mappings endpints for each entity. However this table is
    not used for enforcing the meta attributes in messages. This information
    is present in SchemaMetaAttributeMapping.
    """

    __tablename__ = 'meta_attribute_mapping_store'

    id = Column(Integer, primary_key=True)

    # The name of the entity type, can be namespace, source or schema.
    entity_type = Column(
        Enum(
            'Namespace',
            'Source',
            'AvroSchema',
            name='entity_type'
        ),
        nullable=False
    )

    # Id of the entity specified in the entity_type attribute.
    entity_id = Column(Integer, nullable=False)

    # The schema_id of the meta attribute to be mapped.
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

    @classmethod
    def get_by_mapping(cls, entity_type, entity_id, meta_attr_schema_id):
        try:
            return session.query(
                MetaAttributeMappingStore
            ).filter(
                cls.entity_type == entity_type,
                cls.entity_id == entity_id,
                cls.meta_attr_schema_id == meta_attr_schema_id
            ).one()
        except orm_exc.NoResultFound:
            err_mapping = {
                entity_type: entity_id,
                'meta_attribute_schema_id': meta_attr_schema_id
            }
            raise EntityNotFoundError(
                entity_desc='{} mapping `{}`'.format(cls.__name__, err_mapping)
            )
