# -*- coding: utf-8 -*-
from avro import schema
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.logic import schema_repository as schema_repo
from schematizer.views import view_common


@view_config(
    route_name='api.v1.is_avro_schema_compatible',
    request_method='POST',
    renderer='json'
)
@transform_response()
def is_avro_schema_compatible(request):
    req = requests_v1.AvroSchemaCompatibilityRequest(**request.json_body)
    return _is_schema_compatible(req.schema_json, req.namespace, req.source)


@view_config(
    route_name='api.v1.is_mysql_schema_compatible',
    request_method='POST',
    renderer='json'
)
@transform_response()
def is_mysql_schema_compatible(request):
    req = requests_v1.MysqlSchemaCompatibilityRequest(**request.json_body)
    avro_schema_json = view_common.convert_to_avro_from_mysql(
        req.mysql_statements,
        schema_repo
    )
    return _is_schema_compatible(avro_schema_json, req.namespace, req.source)


def _is_schema_compatible(schema_json, namespace, source):
    try:
        return schema_repo.is_schema_compatible(
            schema_json,
            namespace,
            source
        )
    except schema.AvroException as e:
        raise exceptions_v1.invalid_schema_exception(e.message)
