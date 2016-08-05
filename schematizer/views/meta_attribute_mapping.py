# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import meta_attribute_mappers as meta_attr_logic
from schematizer.models.exceptions import EntityNotFoundError


@view_config(
    route_name='api.v1.register_meta_attribute_mapping_for_namespace',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def register_meta_attribute_mapping_for_namespace(request):
    try:
        namespace_id = int(request.matchdict.get('entity_id'))
        meta_attr_schema_id = int(
            request.matchdict.get('meta_attribute_schema_id')
        )
        mapping = \
            meta_attr_logic.register_meta_attribute_mapping_for_namespace(
                meta_attr_schema_id,
                namespace_id
            )
        return responses_v1.get_meta_attr_mapping_response(
            'namespace_id', mapping.entity_id, [mapping.meta_attr_schema_id]
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.delete_meta_attribute_mapping_for_namespace',
    request_method='DELETE',
    renderer='json'
)
@transform_api_response()
def delete_meta_attribute_mapping_for_namespace(request):
    try:
        namespace_id = int(request.matchdict.get('entity_id'))
        meta_attr_schema_id = int(
            request.matchdict.get('meta_attribute_schema_id')
        )
        meta_attr_logic.delete_meta_attribute_mapping_for_namespace(
            meta_attr_schema_id,
            namespace_id
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.register_meta_attribute_mapping_for_source',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def register_meta_attribute_mapping_for_source(request):
    try:
        source_id = int(request.matchdict.get('entity_id'))
        meta_attr_schema_id = int(
            request.matchdict.get('meta_attribute_schema_id')
        )
        mapping = meta_attr_logic.register_meta_attribute_mapping_for_source(
            meta_attr_schema_id,
            source_id
        )
        return responses_v1.get_meta_attr_mapping_response(
            'source_id', mapping.entity_id, [mapping.meta_attr_schema_id]
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.delete_meta_attribute_mapping_for_source',
    request_method='DELETE',
    renderer='json'
)
@transform_api_response()
def delete_meta_attribute_mapping_for_source(request):
    try:
        source_id = int(request.matchdict.get('entity_id'))
        meta_attr_schema_id = int(
            request.matchdict.get('meta_attribute_schema_id')
        )
        meta_attr_logic.delete_meta_attribute_mapping_for_source(
            meta_attr_schema_id,
            source_id
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.register_meta_attribute_mapping_for_schema',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def register_meta_attribute_mapping_for_schema(request):
    try:
        schema_id = int(request.matchdict.get('entity_id'))
        meta_attr_schema_id = int(
            request.matchdict.get('meta_attribute_schema_id')
        )
        mapping = meta_attr_logic.register_meta_attribute_mapping_for_schema(
            meta_attr_schema_id,
            schema_id
        )
        return responses_v1.get_meta_attr_mapping_response(
            'avroschema_id', mapping.entity_id, [mapping.meta_attr_schema_id]
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.delete_meta_attribute_mapping_for_schema',
    request_method='DELETE',
    renderer='json'
)
@transform_api_response()
def delete_meta_attribute_mapping_for_schema(request):
    try:
        schema_id = int(request.matchdict.get('entity_id'))
        meta_attr_schema_id = int(
            request.matchdict.get('meta_attribute_schema_id')
        )
        meta_attr_logic.delete_meta_attribute_mapping_for_schema(
            meta_attr_schema_id,
            schema_id
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.get_meta_attr_mappings_by_namespace_id',
    request_method='GET',
)
@transform_api_response()
def get_meta_attr_mappings_by_namespace_id(request):
    try:
        namespace_id = int(request.matchdict.get('namespace_id'))
        meta_attr_ids = meta_attr_logic.get_meta_attributes_by_namespace(
            namespace_id
        )
        return responses_v1.get_meta_attr_mapping_response(
            'namespace_id', namespace_id, meta_attr_ids
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.get_meta_attr_mappings_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_meta_attr_mappings_by_source_id(request):
    try:
        source_id = int(request.matchdict.get('source_id'))
        meta_attr_ids = meta_attr_logic.get_meta_attributes_by_source(
            source_id
        )
        return responses_v1.get_meta_attr_mapping_response(
            'source_id', source_id, meta_attr_ids
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.get_meta_attr_mappings_by_schema_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_meta_attr_mappings_by_schema_id(request):
    try:
        schema_id = int(request.matchdict.get('schema_id'))
        meta_attr_ids = meta_attr_logic.get_meta_attributes_by_schema(
            schema_id
        )
        return responses_v1.get_meta_attr_mapping_response(
            'avroschema_id', schema_id, meta_attr_ids
        )
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)
