# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.components.redshift_schema_migration import RedshiftSchemaMigration
from schematizer.components.converters.avro_to_redshift_converter import AvroToRedshiftConverter


@view_config(
    route_name='api.v1.get_table_create_statement_from_avro_schema',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_table_create_statement_from_avro_schema(request):
    avro_schema_json = request.json_body.get('avro_schema')

    redshift_table = AvroToRedshiftConverter().convert(avro_schema_json)
    return RedshiftSchemaMigration.create_table_sql(redshift_table)


@view_config(
    route_name='api.v1.get_schema_migration',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_migration(request):
    new_avro_schema_json = request.json_body.get('new_avro_schema')
    old_avro_schema_json = request.json_body.get('old_avro_schema')

    new_redshift_table = AvroToRedshiftConverter().convert(new_avro_schema_json)
    old_redshift_table = AvroToRedshiftConverter().convert(old_avro_schema_json)
    return RedshiftSchemaMigration().create_simple_push_plan(new_redshift_table, old_redshift_table)
