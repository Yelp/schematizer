# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import or_
from sqlalchemy import and_
from sqlalchemy.orm import exc as orm_exc

from schematizer.models import AvroSchema
from schematizer.models import EntityType
from schematizer.models import MetaAttributeMappingStore
from schematizer.models import Namespace
from schematizer.models import SchemaMetaAttributeMapping
from schematizer.models import Source
from schematizer.models.database import session


def _verify_id_exists(model_cls, id):
    session.query(model_cls).filter(getattr(model_cls, 'id') == id).one()

def _create_meta_attribute_mapping_if_not_exist(
    entity_type,
    entity_id,
    meta_attr_schema_id
):
    _verify_id_exists(AvroSchema, meta_attr_schema_id)
    return MetaAttributeMappingStore.create(
        session,
        entity_type=entity_type,
        entity_id=entity_id,
        meta_attr_schema_id=meta_attr_schema_id
    )


def _register_meta_attribute_for_entity(
    meta_attr_schema_id,
    entity_type,
    entity_id
):
    try:
        mapping = session.query(
            MetaAttributeMappingStore
        ).filter(
            MetaAttributeMappingStore.entity_type == entity_type,
            MetaAttributeMappingStore.entity_id == entity_id,
            MetaAttributeMappingStore.meta_attr_schema_id == meta_attr_schema_id
        ).one()
    except orm_exc.NoResultFound:
        return _create_meta_attribute_mapping_if_not_exist(
            entity_type=entity_type,
            entity_id=entity_id,
            meta_attr_schema_id=meta_attr_schema_id
        )
    else:
        return mapping


def _delete_meta_attribute_mapping_for_entity(
    meta_attr_schema_id,
    entity_type,
    entity_id
):
    return session.query(
            MetaAttributeMappingStore
        ).filter(
            MetaAttributeMappingStore.entity_type == entity_type,
            MetaAttributeMappingStore.entity_id == entity_id,
            MetaAttributeMappingStore.meta_attr_schema_id == meta_attr_schema_id
        ).delete()


def register_meta_attribute_mapping_for_namespace(
    meta_attr_schema_id,
    namespace_id
):
    _verify_id_exists(Namespace, namespace_id)
    return _register_meta_attribute_for_entity(
        meta_attr_schema_id,
        EntityType.NAMESPACE,
        namespace_id
    )


def register_meta_attribute_mapping_for_source(
    meta_attr_schema_id,
    source_id
):
    _verify_id_exists(Source, source_id)
    return _register_meta_attribute_for_entity(
        meta_attr_schema_id,
        EntityType.SOURCE,
        source_id
    )


def register_meta_attribute_mapping_for_schema(
    meta_attr_schema_id,
    schema_id
):
    _verify_id_exists(AvroSchema, schema_id)
    return _register_meta_attribute_for_entity(
        meta_attr_schema_id,
        EntityType.SCHEMA,
        schema_id
    )


def delete_meta_attribute_mapping_for_namespace(
    meta_attr_schema_id,
    namespace_id
):
    return _delete_meta_attribute_mapping_for_entity(
        meta_attr_schema_id,
        EntityType.NAMESPACE,
        namespace_id
    )


def delete_meta_attribute_mapping_for_source(
    meta_attr_schema_id,
    source_id
):
    return _delete_meta_attribute_mapping_for_entity(
        meta_attr_schema_id,
        EntityType.SOURCE,
        source_id
    )


def delete_meta_attribute_mapping_for_schema(
    meta_attr_schema_id,
    schema_id
):
    return _delete_meta_attribute_mapping_for_entity(
        meta_attr_schema_id,
        EntityType.SCHEMA,
        schema_id
    )


def _filter_param_for_namespace(namespace_id):
    return and_(
        MetaAttributeMappingStore.entity_type == EntityType.NAMESPACE,
        MetaAttributeMappingStore.entity_id == namespace_id
    )


def _filter_param_for_source(source_id):
    return and_(
        MetaAttributeMappingStore.entity_type == EntityType.SOURCE,
        MetaAttributeMappingStore.entity_id == source_id
    )


def _filter_param_for_schema(source_id):
    return and_(
        MetaAttributeMappingStore.entity_type == EntityType.SCHEMA,
        MetaAttributeMappingStore.entity_id == source_id
    )


def get_meta_attributes_by_namespace(namespace):
    meta_attr_mappings = session.query(
        MetaAttributeMappingStore
    ).filter(
        _filter_param_for_namespace(namespace.id)
    ).order_by(
        MetaAttributeMappingStore.meta_attr_schema_id
    ).all()
    return [m.meta_attr_schema_id for m in meta_attr_mappings]


def get_meta_attributes_by_source(source):
    meta_attr_mappings = session.query(
        MetaAttributeMappingStore
    ).filter(
        or_(
            _filter_param_for_namespace(source.namespace_id),
            _filter_param_for_source(source.id)
        )
    ).all()
    return [m.meta_attr_schema_id for m in meta_attr_mappings]


def get_meta_attributes_by_schema(avro_schema):
    schema_id = avro_schema.id
    source_id = avro_schema.topic.source_id
    namespace_id = avro_schema.topic.source.namespace_id
    meta_attr_mappings = session.query(
        MetaAttributeMappingStore
    ).filter(
        or_(
            _filter_param_for_namespace(namespace_id),
            _filter_param_for_source(source_id),
            _filter_param_for_schema(schema_id)
        )
    ).all()
    return [m.meta_attr_schema_id for m in meta_attr_mappings]


def add_meta_attribute_mappings(avro_schema):
    return [
        SchemaMetaAttributeMapping.create(
            session,
            schema_id=avro_schema.id,
            meta_attr_schema_id=meta_attr_schema_id
        ) for meta_attr_schema_id in get_meta_attributes_by_schema(avro_schema)
    ]
