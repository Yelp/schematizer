# -*- coding: utf-8 -*-
import simplejson
from avro import schema
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.logic import schema_repository
from schematizer.views import view_common


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
        raise exceptions_v1.schema_not_found_exception()
    return schema.to_dict()


@view_config(
    route_name='api.v1.register_schema',
    request_method='POST',
    renderer='json'
)
@transform_response()
def register_schema(request):
    try:
        req = requests_v1.RegisterSchemaRequest(**request.json_body)
        return _register_avro_schema(
            schema_json=req.schema_json,
            namespace=req.namespace,
            source=req.source,
            domain_email_owner=req.source_owner_email,
            base_schema_id=req.base_schema_id
        )
    except simplejson.JSONDecodeError as e:
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
@transform_response()
def register_schema_from_mysql_stmts(request):
    req = requests_v1.RegisterSchemaFromMySqlRequest(**request.json_body)
    avro_schema_json = view_common.convert_to_avro_from_mysql(
        req.mysql_statements,
        schema_repository
    )
    return _register_avro_schema(
        schema_json=avro_schema_json,
        namespace=req.namespace,
        source=req.source,
        domain_email_owner=req.source_owner_email
    )


def _register_avro_schema(schema_json, namespace, source,
                          domain_email_owner, base_schema_id=None):
    try:
        return schema_repository.create_avro_schema_from_avro_json(
            avro_schema_json=schema_json,
            namespace=namespace,
            source=source,
            domain_email_owner=domain_email_owner,
            base_schema_id=base_schema_id
        ).to_dict()
    except schema.AvroException as e:
        raise exceptions_v1.invalid_schema_exception(e.message)
