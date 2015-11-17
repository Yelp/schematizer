# -*- coding: utf-8 -*-
"""This module contains the responses of schematizer v1 APIs.

Most of them are shared in various API responses, and therefore keep them
in this module.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from yelp_avro.data_pipeline.avro_meta_data import AvroMetaDataKeys


def _format_datetime(datetime_value):
    return datetime_value.isoformat()


def get_namespace_response_from_namespace(namespace):
    return {
        'namespace_id': namespace.id,
        'name': namespace.name,
        'created_at': _format_datetime(namespace.created_at),
        'updated_at': _format_datetime(namespace.updated_at)
    }


def get_source_response_from_source(source):
    return {
        'source_id': source.id,
        'name': source.name,
        'owner_email': source.owner_email,
        'namespace': get_namespace_response_from_namespace(source.namespace),
        'category': (
            None if source.category is None else source.category.category
        ),
        'created_at': _format_datetime(source.created_at),
        'updated_at': _format_datetime(source.updated_at)
    }


def get_topic_response_from_topic(topic):
    return {
        'topic_id': topic.id,
        'name': topic.name,
        'source': get_source_response_from_source(topic.source),
        'contains_pii': topic.contains_pii,
        'created_at': _format_datetime(topic.created_at),
        'updated_at': _format_datetime(topic.updated_at)
    }


def get_schema_response_from_avro_schema(avro_schema):
    response = {
        'schema_id': avro_schema.id,
        'schema': avro_schema.avro_schema,
        'status': avro_schema.status,
        'topic': get_topic_response_from_topic(avro_schema.topic),
        'primary_keys': avro_schema.avro_schema_json.get(
            AvroMetaDataKeys.PRIMARY_KEY,
            []
        ),
        'note': get_note_response_from_note(avro_schema.note),
        'created_at': _format_datetime(avro_schema.created_at),
        'updated_at': _format_datetime(avro_schema.updated_at)
    }
    # Since swagger cannot take null or None value for integer type,
    # here we just simply strip out this field.
    if avro_schema.base_schema_id is not None:
        response['base_schema_id'] = avro_schema.base_schema_id
    return response


def get_note_response_from_note(note):
    if note is not None:
        response = {
            'id': note.id,
            'reference_type': note.reference_type,
            'reference_id': note.reference_id,
            'note': note.note,
            'last_updated_by': note.last_updated_by,
            'created_at': _format_datetime(note.created_at),
            'updated_at': _format_datetime(note.updated_at)
        }
        return response
    return None


def get_category_response_from_source_category(source_category):
    return {
        'source_id': source_category.source_id,
        'category': source_category.category,
        'created_at': _format_datetime(source_category.created_at),
        'updated_at': _format_datetime(source_category.updated_at)
    }
