# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config

from schematizer import models
from schematizer.api.decorators import transform_response
from schematizer.api.requests import requests_v1
from schematizer.components import mysql_processor
from schematizer.logic import schema_repository
from schematizer.views import constants


@view_config(
    route_name='api.v1.get_schema_by_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_schema_by_id(request):
    schema_id = request.matchdict.get('schema_id')
    schema = schema_repository.get_schema_by_id(int(schema_id))
    if schema is None:
        raise exception_response(
            404,
            detail=constants.SCHEMA_NOT_FOUND_ERROR_MESSAGE
        )
    return schema.to_dict()


@view_config(
    route_name='api.v1.register_avro_schema',
    request_method='POST',
    renderer='json'
)
@transform_response()
def register_avro_schema(request):
    req = requests_v1.RegisterSchemaRequest.create_from_string(
        request.body
    )
    return schema_repository.create_avro_schema_from_avro_json(
        avro_schema_json=req.schema,
        namespace=req.namespace,
        source=req.source,
        domain_email_owner=req.source_owner_email,
        base_schema_id=req.base_schema_id
    ).to_dict()


@view_config(
    route_name='api.v1.register_avro_schema_from_mysql_stmts',
    request_method='POST',
    renderer='json'
)
@transform_response()
def register_avro_schema_from_mysql_stmts(request):
    req = requests_v1.RegisterSchemaFromMySqlRequest.create_from_string(
        request.body
    )
    sql_table = mysql_processor.process_table_schema_mysql(
        req.mysql_statements
    )
    avro_schema_json = schema_repository.convert_schema(
        models.SchemaKindEnum.MySQL,
        models.SchemaKindEnum.Avro,
        sql_table
    )
    return schema_repository.create_avro_schema_from_avro_json(
        avro_schema_json=avro_schema_json,
        namespace=req.namespace,
        source=req.source,
        domain_email_owner=req.source_owner_email
    ).to_dict()
