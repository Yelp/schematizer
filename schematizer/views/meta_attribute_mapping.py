# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import meta_attribute_mappers as meta_attr_logic
from schematizer.logic import schema_repository


def _verify_and_get_namespace_from_request(request, key):
    namespace_id = int(request.matchdict.get(key))
    namespace = schema_repository.get_namespace_by_id(namespace_id)
    if namespace is None:
        raise exceptions_v1.namespace_not_found_exception()
    return namespace


def _verify_and_get_source_from_request(request, key):
    source_id = int(request.matchdict.get(key))
    source = schema_repository.get_source_by_id(source_id)
    if source is None:
        raise exceptions_v1.source_not_found_exception()
    return source


def _verify_and_get_schema_from_request(request, key):
    schema_id = int(request.matchdict.get(key))
    schema = schema_repository.get_schema_by_id(schema_id)
    if schema is None:
        raise exceptions_v1.schema_not_found_exception()
    return schema


@view_config(
    route_name='api.v1.register_meta_attribute_mapping_for_namespace',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def register_meta_attribute_mapping_for_namespace(request):
    namespace_id = _verify_and_get_namespace_from_request(request, 'entity_id').id
    meta_attr_schema_id = _verify_and_get_schema_from_request(
        request, 'meta_attribute_schema_id').id
    mapping = meta_attr_logic.register_meta_attribute_mapping_for_namespace(
        meta_attr_schema_id,
        namespace_id
    )
    return responses_v1.get_meta_attr_mapping_response(
        'namespace_id', mapping.entity_id, [mapping.meta_attr_schema_id]
    )


@view_config(
    route_name='api.v1.delete_meta_attribute_mapping_for_namespace',
    request_method='DELETE',
    renderer='json'
)
@transform_api_response()
def delete_meta_attribute_mapping_for_namespace(request):
    namespace_id = _verify_and_get_namespace_from_request(request, 'entity_id').id
    meta_attr_schema_id = _verify_and_get_schema_from_request(
        request, 'meta_attribute_schema_id').id
    return meta_attr_logic.delete_meta_attribute_mapping_for_namespace(
        meta_attr_schema_id,
        namespace_id
    )


@view_config(
    route_name='api.v1.register_meta_attribute_mapping_for_source',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def register_meta_attribute_mapping_for_source(request):
    source_id = _verify_and_get_source_from_request(request, 'entity_id').id
    meta_attr_schema_id = _verify_and_get_schema_from_request(
        request, 'meta_attribute_schema_id').id
    mapping = meta_attr_logic.register_meta_attribute_mapping_for_source(
        meta_attr_schema_id,
        source_id
    )
    return responses_v1.get_meta_attr_mapping_response(
        'source_id', mapping.entity_id, [mapping.meta_attr_schema_id]
    )


@view_config(
    route_name='api.v1.delete_meta_attribute_mapping_for_source',
    request_method='DELETE',
    renderer='json'
)
@transform_api_response()
def delete_meta_attribute_mapping_for_source(request):
    source_id = _verify_and_get_source_from_request(request, 'entity_id').id
    meta_attr_schema_id = _verify_and_get_schema_from_request(
        request, 'meta_attribute_schema_id').id
    return meta_attr_logic.delete_meta_attribute_mapping_for_source(
        meta_attr_schema_id,
        source_id
    )


@view_config(
    route_name='api.v1.register_meta_attribute_mapping_for_schema',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def register_meta_attribute_mapping_for_schema(request):
    schema_id = _verify_and_get_schema_from_request(request, 'entity_id').id
    meta_attr_schema_id = _verify_and_get_schema_from_request(
        request, 'meta_attribute_schema_id').id
    mapping = meta_attr_logic.register_meta_attribute_mapping_for_schema(
        meta_attr_schema_id,
        schema_id
    )
    return responses_v1.get_meta_attr_mapping_response(
        'schema_id', mapping.entity_id, [mapping.meta_attr_schema_id]
    )


@view_config(
    route_name='api.v1.delete_meta_attribute_mapping_for_schema',
    request_method='DELETE',
    renderer='json'
)
@transform_api_response()
def delete_meta_attribute_mapping_for_schema(request):
    schema_id = _verify_and_get_schema_from_request(request, 'entity_id').id
    meta_attr_schema_id = _verify_and_get_schema_from_request(
        request, 'meta_attribute_schema_id').id
    return meta_attr_logic.delete_meta_attribute_mapping_for_schema(
        meta_attr_schema_id,
        schema_id
    )


@view_config(
    route_name='api.v1.get_meta_attr_mappings_by_namespace_id',
    request_method='GET',
)
@transform_api_response()
def get_meta_attr_mappings_by_namespace_id(request):
    namespace = _verify_and_get_namespace_from_request(request, 'namespace_id')
    meta_attr_ids = meta_attr_logic.get_meta_attributes_by_namespace(namespace)
    return responses_v1.get_meta_attr_mapping_response(
        'namespace_id', namespace.id, meta_attr_ids
    )


@view_config(
    route_name='api.v1.get_meta_attr_mappings_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_meta_attr_mappings_by_source_id(request):
    source = _verify_and_get_source_from_request(request, 'source_id')
    meta_attr_ids = meta_attr_logic.get_meta_attributes_by_source(source)
    return responses_v1.get_meta_attr_mapping_response(
        'source_id', source.id, meta_attr_ids
    )


@view_config(
    route_name='api.v1.get_meta_attr_mappings_by_schema_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_meta_attr_mappings_by_schema_id(request):
    schema = _verify_and_get_schema_from_request(request, 'schema_id')
    meta_attr_ids = meta_attr_logic.get_meta_attributes_by_schema(schema)
    return responses_v1.get_meta_attr_mapping_response(
        'schema_id', schema.id, meta_attr_ids
    )
