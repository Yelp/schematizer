# -*- coding: utf-8 -*-
from pyramid.view import view_config

from schematizer import models
from schematizer.api.decorators import transform_response
from schematizer.api.requests import requests_v1
from schematizer.components import mysql_processor
from schematizer.logic import schema_repository as schema_repo


@view_config(
    route_name='api.v1.is_avro_schema_compatible',
    request_method='POST',
    renderer='json'
)
@transform_response()
def is_avro_schema_compatible(request):
    req = requests_v1.AvroSchemaCompatibilityRequest.create_from_string(
        request.body
    )
    return schema_repo.is_schema_compatible(
        req.schema_json,
        req.namespace,
        req.source
    )


@view_config(
    route_name='api.v1.is_mysql_schema_compatible',
    request_method='POST',
    renderer='json'
)
@transform_response()
def is_mysql_schema_compatible(request):
    req = requests_v1.MysqlSchemaCompatibilityRequest.create_from_string(
        request.body
    )
    sql_table = mysql_processor.process_table_schema_mysql(
        req.mysql_statements
    )
    avro_schema_json = schema_repo.convert_schema(
        models.SchemaKindEnum.MySQL,
        models.SchemaKindEnum.Avro,
        sql_table
    )
    return schema_repo.is_schema_compatible(
        avro_schema_json,
        req.namespace,
        req.source
    )
