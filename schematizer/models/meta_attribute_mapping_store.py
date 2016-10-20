# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from time import time

from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy.types import Enum

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError


class MetaAttributeEntity(object):

    NAMESPACE = 'Namespace'
    SOURCE = 'Source'


class MetaAttributeMappingStore(Base, BaseModel):
    """This table stores all the mappings of meta attribute registered for
    each entity. The entities can be Namespace or Source. This table is a
    source of truth for all mappings currently active in the Data Pipeline.
    Rows are modified in this table by hitting the meta_attribute_mappings
    endpoints for each entity. However this table is not used for enforcing
    the meta attributes in messages. This information is present in
    SchemaMetaAttributeMapping.
    """

    __tablename__ = 'meta_attribute_mapping_store'

    id = Column(Integer, primary_key=True)

    # The name of the entity type, can be namespace or source.
    entity_type = Column(
        Enum(
            MetaAttributeEntity.NAMESPACE,
            MetaAttributeEntity.SOURCE,
            name='entity_type'
        ),
        nullable=False
    )

    # Id of the entity specified in the entity_type attribute.
    entity_id = Column(Integer, nullable=False)

    # The schema_id of the meta attribute to be mapped.
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

    @classmethod
    def get_by_mapping(cls, entity_type, entity_id, meta_attr_schema_id):
        result = session.query(
            MetaAttributeMappingStore
        ).filter(
            cls.entity_type == entity_type,
            cls.entity_id == entity_id,
            cls.meta_attr_schema_id == meta_attr_schema_id
        ).one_or_none()
        if result is None:
            err_mapping = {
                entity_type + '_id': entity_id,
                'meta_attribute_schema_id': meta_attr_schema_id
            }
            raise EntityNotFoundError(
                entity_desc='{} mapping `{}`'.format(cls.__name__, err_mapping)
            )
        return result
