# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson as json
from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.components.converters.avro_to_redshift_converter \
    import AvroToRedshiftConverter
from schematizer.components.redshift_schema_migration \
    import RedshiftSchemaMigration


def _get_redshift_schema_migration(new_avro_schema, old_avro_schema):
    new_redshift_table = AvroToRedshiftConverter().convert(
        new_avro_schema
    )
    old_redshift_table = AvroToRedshiftConverter().convert(
        old_avro_schema
    )
    return RedshiftSchemaMigration().create_simple_push_plan(
        new_redshift_table,
        old_redshift_table
    )


SCHEMA_MIGRATION_STRATEGY_MAP = {
    'redshift': _get_redshift_schema_migration
}


@view_config(
    route_name='api.v1.get_schema_migration',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_schema_migration(request):
    new_schema_json = request.json_body.get('new_schema')
    old_schema_json = request.json_body.get('old_schema')
    target_schema_type = request.json_body.get('target_schema_type')

    _get_migration = SCHEMA_MIGRATION_STRATEGY_MAP.get(target_schema_type)
    if _get_migration is None:
        raise exceptions_v1.unsupported_target_schema_exception()

    return _get_migration(
        new_avro_schema=json.loads(new_schema_json),
        old_avro_schema=json.loads(old_schema_json) if old_schema_json else {}
    )
