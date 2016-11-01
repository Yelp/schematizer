# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import simplejson
from avro import schema
from pyramid.view import view_config

from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.config import log
from schematizer.logic import schema_repository as schema_repo
from schematizer.utils.utils import get_current_func_arg_name_values
from schematizer.views import view_common


@view_config(
    route_name='api.v1.is_avro_schema_compatible',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def is_avro_schema_compatible(request):
    try:
        req = requests_v1.AvroSchemaCompatibilityRequest(**request.json_body)
        return _is_schema_compatible(
            req.schema_json,
            req.namespace,
            req.source
        )
    except simplejson.JSONDecodeError as e:
        log.exception("Failed to construct AvroSchemaCompatibilityRequest. {}"
                      .format(request.json_body))
        raise exceptions_v1.invalid_schema_exception(
            'Error "{error}" encountered decoding JSON: "{schema}"'.format(
                error=str(e),
                schema=request.json_body['schema']
            )
        )


@view_config(
    route_name='api.v1.is_mysql_schema_compatible',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def is_mysql_schema_compatible(request):
    req = requests_v1.MysqlSchemaCompatibilityRequest(**request.json_body)
    avro_schema_json = view_common.convert_to_avro_from_mysql(
        schema_repo,
        req.new_create_table_stmt,
        req.old_create_table_stmt,
        req.alter_table_stmt
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
        log.exception('{0}'.format(get_current_func_arg_name_values()))
        raise exceptions_v1.invalid_schema_exception(e.message)
