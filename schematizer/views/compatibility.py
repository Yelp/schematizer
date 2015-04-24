# -*- coding: utf-8 -*-
from avro import schema
from pyramid.view import view_config

from schematizer import models
from schematizer.api.decorators import transform_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.components import mysql_handlers
from schematizer.components.converters import converter_base
from schematizer.logic import schema_repository as schema_repo


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
    avro_schema_json = _convert_to_avro_from_mysql(req.mysql_statements)
    return _is_schema_compatible(avro_schema_json, req.namespace, req.source)


def _convert_to_avro_from_mysql(mysql_statements):
    try:
        sql_table = mysql_handlers.create_sql_table_from_mysql_stmts(
            mysql_statements
        )
        return schema_repo.convert_schema(
            models.SchemaKindEnum.MySQL,
            models.SchemaKindEnum.Avro,
            sql_table
        )
    except (mysql_handlers.MySQLHandlerException,
            converter_base.SchemaConversionException) as e:
        raise exceptions_v1.invalid_schema_exception(e.message)


def _is_schema_compatible(schema_json, namespace, source):
    try:
        return schema_repo.is_schema_compatible(
            schema_json,
            namespace,
            source
        )
    except schema.AvroException as e:
        raise exceptions_v1.invalid_schema_exception(e.message)
