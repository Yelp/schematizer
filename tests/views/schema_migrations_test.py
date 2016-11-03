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

import copy

import pytest
import simplejson as json

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import schema_migrations as schema_migrations_view
from tests.views.api_test_base import ApiTestBase


class TestGetSchemaMigration(ApiTestBase):

    def test_get_schema_migration_on_existing_table(
            self,
            mock_request,
            biz_schema_json
    ):
        new_schema = copy.deepcopy(biz_schema_json)
        new_schema['fields'].append(
            {'maxlen': '22', 'name': 'test_1', 'type': ['null', 'string']}
        )

        mock_request.json_body = {
            'old_schema': json.dumps(biz_schema_json),
            'new_schema': json.dumps(new_schema),
            'target_schema_type': 'redshift'
        }
        actual = schema_migrations_view.get_schema_migration(mock_request)
        expected = [
            'BEGIN;',
            'CREATE TABLE biz_tmp (id integer not null default 0,'
            'test_1 varchar(44));',
            'INSERT INTO biz_tmp (id) (SELECT id FROM biz);',
            'ALTER TABLE biz RENAME TO "biz_old";',
            'ALTER TABLE biz_tmp RENAME TO "biz";',
            'DROP TABLE biz_old;',
            'COMMIT;'
        ]
        assert actual == expected

    def test_get_schema_migration_on_table_name_change(
        self,
        mock_request,
        biz_schema_json
    ):
        new_schema = copy.deepcopy(biz_schema_json)
        new_schema['name'] = 'business'

        mock_request.json_body = {
            'old_schema': json.dumps(biz_schema_json),
            'new_schema': json.dumps(new_schema),
            'target_schema_type': 'redshift'
        }
        actual = schema_migrations_view.get_schema_migration(mock_request)
        expected = [
            'BEGIN;',
            'CREATE TABLE business (id integer not null default 0);',
            'INSERT INTO business (id) (SELECT id FROM biz);',
            'COMMIT;'
        ]
        assert actual == expected

    def test_get_schema_migration_on_new_table(
            self,
            mock_request,
            biz_schema_json
    ):
        mock_request.json_body = {
            'new_schema': json.dumps(biz_schema_json),
            'target_schema_type': 'redshift'
        }
        actual = schema_migrations_view.get_schema_migration(mock_request)
        expected = [
            'BEGIN;',
            'CREATE TABLE biz (id integer not null default 0);',
            '',
            'COMMIT;'
        ]
        assert actual == expected

    def test_invalid_schema(
            self,
            mock_request,
            biz_schema_json
    ):
        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = {
                'new_schema': '{"bad_json_reason": "missing bracket"',
                'target_schema_type': 'redshift'
            }
            schema_migrations_view.get_schema_migration(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.INVALID_AVRO_SCHEMA_ERROR

    def test_get_unsupported_schema_migration(
            self,
            mock_request,
            biz_schema_json
    ):
        expected_exception = self.get_http_exception(501)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = {
                'new_schema': json.dumps(biz_schema_json),
                'target_schema_type': 'unsupported_schema_type'
            }
            schema_migrations_view.get_schema_migration(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.UNSUPPORTED_TARGET_SCHEMA_MESSAGE
