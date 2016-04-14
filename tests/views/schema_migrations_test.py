# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

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
            'old_avro_schema': biz_schema_json,
            'new_avro_schema': new_schema
        }
        actual = schema_migrations_view.get_schema_migration(mock_request)
        expected = [
            'BEGIN;',
            'CREATE TABLE biz_tmp (id integer not null default 0,test_1 varchar(44));',  # noqa
            'INSERT INTO biz_tmp (id) (SELECT id FROM biz);',
            'ALTER TABLE biz RENAME TO biz_old;',
            'ALTER TABLE biz_tmp RENAME TO biz;',
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
            'old_avro_schema': biz_schema_json,
            'new_avro_schema': new_schema
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
            'old_avro_schema': None,
            'new_avro_schema': biz_schema_json
        }
        actual = schema_migrations_view.get_schema_migration(mock_request)
        expected = [
            'BEGIN;',
            'CREATE TABLE biz (id integer not null default 0);',
            '',
            'COMMIT;'
        ]
        assert actual == expected
