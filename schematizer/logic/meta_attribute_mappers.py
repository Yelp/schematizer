# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy import or_

from schematizer.logic.validators import verify_entity_exists
from schematizer.models import AvroSchema
from schematizer.models import MetaAttributeMappingStore
from schematizer.models import Namespace
from schematizer.models import Source
from schematizer.models.database import session


def _register_meta_attribute_for_entity(
    meta_attr_sch_id,
    entity_model,
    entity_id
):
    """This is a helper function to register a meta_attribute_schema_id to an
    entity. Entities can belong to the Namespace, Source or AvroSchema model.
    First it verifies if all the ids exist within their respective DB models.
    Then we try adding the new mapping. In the case it violates the unique
    constraint and raises an IntegrityError, we will return the existing
    mapping.

    :param meta_attr_sch_id: AvroSchema ID of Meta Attribute
    :param entity_model: DB Model of the entity
    :param entity_id: ID of the entity
    :return: models.MetaAttributeMappingStore object
    """
    verify_entity_exists(session, entity_model, entity_id)
    verify_entity_exists(session, AvroSchema, meta_attr_sch_id)
    try:
        with session.begin_nested():
            new_mapping = MetaAttributeMappingStore(
                entity_type=entity_model.__name__,
                entity_id=entity_id,
                meta_attr_schema_id=meta_attr_sch_id
            )
            session.add(new_mapping)
    except exc.IntegrityError:
        # Ignore this error due to trying to create a duplicate mapping
        new_mapping = session.query(
            MetaAttributeMappingStore
        ).filter(
            MetaAttributeMappingStore.entity_type == entity_model.__name__,
            MetaAttributeMappingStore.entity_id == entity_id,
            MetaAttributeMappingStore.meta_attr_schema_id == meta_attr_sch_id
        ).first()
    return new_mapping


def _delete_meta_attribute_mapping_for_entity(
    meta_attr_sch_id,
    entity_model,
    entity_id
):
    """This is a helper function to delete a meta_attribute_schema_id from an
    entity. Entities can belong to the Namespace, Source or AvroSchema model.
    First it verifies if all the ids exist within their respective DB models.
    Then removing the mapping will return a True if there was a mapping to
    delete and delete was successful. However if there was no mapping to
    delete or if the delete was not successful, it will return False

    :param meta_attr_sch_id: AvroSchema ID of Meta Attribute
    :param entity_model: DB Model of the entity
    :param entity_id: ID of the entity
    """
    verify_entity_exists(session, entity_model, entity_id)
    verify_entity_exists(session, AvroSchema, meta_attr_sch_id)
    return bool(
        session.query(
            MetaAttributeMappingStore
        ).filter(
            MetaAttributeMappingStore.entity_type == entity_model.__name__,
            MetaAttributeMappingStore.entity_id == entity_id,
            MetaAttributeMappingStore.meta_attr_schema_id == meta_attr_sch_id
        ).delete()
    )


def register_meta_attribute_mapping_for_namespace(
    meta_attr_schema_id,
    namespace_id
):
    return _register_meta_attribute_for_entity(
        meta_attr_sch_id=meta_attr_schema_id,
        entity_model=Namespace,
        entity_id=namespace_id
    )


def register_meta_attribute_mapping_for_source(
    meta_attr_schema_id,
    source_id
):
    return _register_meta_attribute_for_entity(
        meta_attr_sch_id=meta_attr_schema_id,
        entity_model=Source,
        entity_id=source_id
    )


def register_meta_attribute_mapping_for_schema(
    meta_attr_schema_id,
    schema_id
):
    return _register_meta_attribute_for_entity(
        meta_attr_sch_id=meta_attr_schema_id,
        entity_model=AvroSchema,
        entity_id=schema_id
    )


def delete_meta_attribute_mapping_for_namespace(
    meta_attr_schema_id,
    namespace_id
):
    return _delete_meta_attribute_mapping_for_entity(
        meta_attr_sch_id=meta_attr_schema_id,
        entity_model=Namespace,
        entity_id=namespace_id
    )


def delete_meta_attribute_mapping_for_source(
    meta_attr_schema_id,
    source_id
):
    return _delete_meta_attribute_mapping_for_entity(
        meta_attr_sch_id=meta_attr_schema_id,
        entity_model=Source,
        entity_id=source_id
    )


def delete_meta_attribute_mapping_for_schema(
    meta_attr_schema_id,
    schema_id
):
    return _delete_meta_attribute_mapping_for_entity(
        meta_attr_sch_id=meta_attr_schema_id,
        entity_model=AvroSchema,
        entity_id=schema_id
    )


def _filter_param_for_namespace(namespace_id):
    return and_(
        MetaAttributeMappingStore.entity_type == Namespace.__name__,
        MetaAttributeMappingStore.entity_id == namespace_id
    )


def _filter_param_for_source(source_id):
    return and_(
        MetaAttributeMappingStore.entity_type == Source.__name__,
        MetaAttributeMappingStore.entity_id == source_id
    )


def _filter_param_for_schema(schema_id):
    return and_(
        MetaAttributeMappingStore.entity_type == AvroSchema.__name__,
        MetaAttributeMappingStore.entity_id == schema_id
    )


def _get_meta_attributes():
    return session.query(
        MetaAttributeMappingStore
    ).order_by(
        MetaAttributeMappingStore.meta_attr_schema_id
    )


def get_meta_attributes_by_namespace(namespace_id):
    Namespace.get_by_id(namespace_id)
    query = _get_meta_attributes()
    meta_attr_mappings = query.filter(
        _filter_param_for_namespace(namespace_id)
    ).all()
    return [m.meta_attr_schema_id for m in meta_attr_mappings]


def get_meta_attributes_by_source(source_id):
    source = Source.get_by_id((source_id))
    query = _get_meta_attributes()
    meta_attr_mappings = query.filter(
        or_(
            _filter_param_for_namespace(source.namespace_id),
            _filter_param_for_source(source.id)
        )
    ).all()
    return [m.meta_attr_schema_id for m in meta_attr_mappings]


def get_meta_attributes_by_schema(avro_schema_id):
    avro_schema = AvroSchema.get_by_id(avro_schema_id)
    source_id = avro_schema.topic.source_id
    namespace_id = avro_schema.topic.source.namespace_id
    query = _get_meta_attributes()
    meta_attr_mappings = query.filter(
        or_(
            _filter_param_for_namespace(namespace_id),
            _filter_param_for_source(source_id),
            _filter_param_for_schema(avro_schema.id)
        )
    ).all()
    return [m.meta_attr_schema_id for m in meta_attr_mappings]
