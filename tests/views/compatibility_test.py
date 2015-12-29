# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

import pytest
import simplejson

from schematizer.views import compatibility as compatibility_views
from tests.views.api_test_base import ApiTestBase


@pytest.mark.usefixtures("biz_schema")
class TestAvroSchemaCompatibility(ApiTestBase):

    @pytest.fixture
    def new_biz_schema_json(self, biz_schema_json):
        new_schema = copy.deepcopy(biz_schema_json)
        new_schema['fields'].append(
            {'type': 'int', 'name': 'bar', 'default': 10, 'doc': 'bar'}
        )
        return new_schema

    @pytest.fixture
    def request_json(self, biz_schema, new_biz_schema_json):
        return {
            "schema": simplejson.dumps(new_biz_schema_json),
            "namespace": biz_schema.topic.source.namespace.name,
            "source": biz_schema.topic.source.name
        }

    def test_compatible(self, mock_request, request_json):
        mock_request.json_body = request_json
        actual = compatibility_views.is_avro_schema_compatible(mock_request)
        assert actual is True

    def test_incompatible_schema(
        self,
        mock_request,
        request_json,
        biz_schema_json
    ):
        new_schema_json = copy.deepcopy(biz_schema_json)
        new_schema_json['fields'][-1]['type'] = 'string'
        request_json['schema'] = simplejson.dumps(new_schema_json)
        mock_request.json_body = request_json
        actual = compatibility_views.is_avro_schema_compatible(mock_request)
        assert actual is False

    def test_invalid_json_exception(self, mock_request, request_json):
        request_json['schema'] = 'Not valid json!%#!#$#'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            compatibility_views.is_avro_schema_compatible(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            'Error "Expecting value: line 1 column 1 (char 0)" encountered '
            'decoding JSON: "Not valid json!%#!#$#"'
        )

    def test_invalid_avro_schema(self, mock_request, request_json):
        request_json['schema'] = '{"type": "record", "name": "A"}'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            compatibility_views.is_avro_schema_compatible(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            'Record schema requires a non-empty fields property.'
        )


class TestMySQLSchemaCompatibility(ApiTestBase):

    @property
    def new_create_table_stmt(self):
        return ("create table `biz` ("
                "`id` int(11) not null, "
                "`x` varchar(8) default '');")

    @property
    def old_create_table_stmt(self):
        return 'create table `biz` (`id` int(11) not null);'

    @property
    def alter_table_stmt(self):
        return "alter table `biz` add column `x` varchar(8) default '';"

    @pytest.fixture
    def request_json(self, biz_schema):
        return {
            "new_create_table_stmt": self.new_create_table_stmt,
            "namespace": biz_schema.topic.source.namespace.name,
            "source": biz_schema.topic.source.name
        }

    def test_compatible_new_table(self, mock_request, request_json):
        mock_request.json_body = request_json
        actual = compatibility_views.is_mysql_schema_compatible(mock_request)
        assert actual is True

    def test_incompatible_new_table(self, mock_request, request_json):
        request_json["new_create_table_stmt"] = (
            'create table `biz` (`id` char(10));'
        )
        mock_request.json_body = request_json
        actual = compatibility_views.is_mysql_schema_compatible(mock_request)
        assert actual is False

    def test_compatible_updated_table(self, mock_request, request_json):
        request_json["old_create_table_stmt"] = self.old_create_table_stmt
        request_json["alter_table_stmt"] = self.alter_table_stmt
        mock_request.json_body = request_json

        actual = compatibility_views.is_mysql_schema_compatible(mock_request)
        assert actual is True

    def test_invalid_sql_table_stmt(self, mock_request, request_json):
        request_json["new_create_table_stmt"] = 'create table biz ();'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        assert e.value.code == expected_exception.code
        assert 'No column exists in the table.' in str(e.value)

    def test_unsupported_avro_type(self, mock_request, request_json):
        request_json["new_create_table_stmt"] = 'create table biz (t blob);'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        assert e.value.code == expected_exception.code
        assert 'Unable to convert MySQL data type' in str(e.value)

    def test_invalid_request(self, mock_request, request_json):
        request_json["old_create_table_stmt"] = self.old_create_table_stmt
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == (
            'Both old_create_table_stmt and alter_table_stmt must be provided.'
        )
