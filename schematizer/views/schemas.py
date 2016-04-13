# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson
from pyramid.view import view_config

from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.config import log
from schematizer.logic import schema_repository
from schematizer.utils.utils import get_current_func_arg_name_values
from schematizer.views import view_common


@view_config(
    route_name='api.v1.get_schema_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_by_id(request):
    schema_id = request.matchdict.get('schema_id')
    avro_schema = schema_repository.get_schema_by_id(int(schema_id))
    if avro_schema is None:
        raise exceptions_v1.schema_not_found_exception()
    return responses_v1.get_schema_response_from_avro_schema(avro_schema)


@view_config(
    route_name='api.v1.register_schema',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def register_schema(request):
    try:
        req = requests_v1.RegisterSchemaRequest(**request.json_body)
        validate_names([req.namespace, req.source])
        return _register_avro_schema(
            schema_json=req.schema_json,
            namespace=req.namespace,
            source=req.source,
            source_email_owner=req.source_owner_email,
            contains_pii=req.contains_pii,
            base_schema_id=req.base_schema_id
        )
    except simplejson.JSONDecodeError as e:
        log.exception("Failed to construct RegisterSchemaRequest. {}"
                      .format(request.json_body))
        raise exceptions_v1.invalid_schema_exception(
            'Error "{error}" encountered decoding JSON: "{schema}"'.format(
                error=str(e),
                schema=request.json_body['schema']
            )
        )


@view_config(
    route_name='api.v1.register_schema_from_mysql_stmts',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def register_schema_from_mysql_stmts(request):
    req = requests_v1.RegisterSchemaFromMySqlRequest(**request.json_body)
    avro_schema_json = view_common.convert_to_avro_from_mysql(
        schema_repository,
        req.new_create_table_stmt,
        req.old_create_table_stmt,
        req.alter_table_stmt
    )
    return _register_avro_schema(
        schema_json=avro_schema_json,
        namespace=req.namespace,
        source=req.source,
        source_email_owner=req.source_owner_email,
        contains_pii=req.contains_pii
    )


def _register_avro_schema(
    schema_json,
    namespace,
    source,
    source_email_owner,
    contains_pii,
    base_schema_id=None
):
    try:
        validate_names([namespace, source])
        avro_schema = schema_repository.register_avro_schema_from_avro_json(
            avro_schema_json=schema_json,
            namespace_name=namespace,
            source_name=source,
            source_email_owner=source_email_owner,
            contains_pii=contains_pii,
            base_schema_id=base_schema_id
        )
        return responses_v1.get_schema_response_from_avro_schema(avro_schema)
    except ValueError as e:
        log.exception('{0}'.format(get_current_func_arg_name_values()))
        raise exceptions_v1.invalid_schema_exception(e.message)


@view_config(
    route_name='api.v1.get_schema_elements_by_schema_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_elements_by_schema_id(request):
    schema_id = int(request.matchdict.get('schema_id'))
    # First check if schema exists
    schema = schema_repository.get_schema_by_id(schema_id)
    if schema is None:
        raise exceptions_v1.schema_not_found_exception()
    # Get schema elements
    elements = schema_repository.get_schema_elements_by_schema_id(schema_id)
    return [element.to_dict() for element in elements]


@view_config(
    route_name='api.v1.get_derived_schema_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_derived_schema_by_id(request):
    schema_id = int(request.matchdict.get('schema_id'))
    avro_schema = schema_repository.get_schema_by_id(schema_id)
    if avro_schema is None:
        raise exceptions_v1.schema_not_found_exception()

    avro_schema_json = simplejson.loads(avro_schema.avro_schema)
    avro_schema_json['fields'] += request.json_body.get('extra_columns', [])
    return avro_schema_json


def validate_name(name):
    if '|' in name:
        # Restrict '|' to avoid ambiguity when parsing input of
        # data_pipeline tailer. One of the tailer arguments is topic
        # and optional offset separated by '|'.
        raise exceptions_v1.restricted_char_exception()
    if name.isdigit():
        raise exceptions_v1.numeric_name_exception()


def validate_names(names):
    for name in names:
        validate_name(name)
