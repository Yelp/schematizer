# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
import simplejson

from schematizer import models
from schematizer.api.exceptions import exceptions_v1
from schematizer.logic import schema_repository
from schematizer.views import schemas as schema_views
from tests.views.api_test_base import ApiTestBase


class TestGetSchemaByID(ApiTestBase):

    def test_non_existing_schema(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'schema_id': '0'}
            schema_views.get_schema_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_get_schema_by_id(self, mock_request, biz_schema):
        mock_request.matchdict = {'schema_id': '{}'.format(biz_schema.id)}
        actual = schema_views.get_schema_by_id(mock_request)
        expected = self.get_expected_schema_resp(biz_schema.id)
        assert actual == expected

    def test_get_schema_with_base_schema(self, mock_request, biz_schema):
        biz_schema.base_schema_id = 2
        mock_request.matchdict = {'schema_id': '{}'.format(biz_schema.id)}
        actual = schema_views.get_schema_by_id(mock_request)

        expected = self.get_expected_schema_resp(
            biz_schema.id,
            base_schema_id=2
        )
        assert actual == expected


class RegisterSchemaTestBase(ApiTestBase):

    def _assert_equal_schema_response(self, actual, request_json):
        expected_vals = {}
        if 'base_schema_id' in request_json:
            expected_vals = {'base_schema_id': request_json['base_schema_id']}
        expected = self.get_expected_schema_resp(
            actual['schema_id'],
            **expected_vals
        )
        assert actual == expected

        expected_src = schema_repository.get_source_by_fullname(
            request_json['namespace'],
            request_json['source']
        )
        assert actual['topic']['source']['source_id'] == expected_src.id


class TestRegisterSchema(RegisterSchemaTestBase):

    @pytest.fixture
    def request_json(self, biz_schema_json, biz_source):
        return {
            "schema": simplejson.dumps(biz_schema_json),
            "namespace": biz_source.namespace.name,
            "source": biz_source.name,
            "source_owner_email": 'biz.user@yelp.com',
            'contains_pii': False
        }

    def test_register_schema(self, mock_request, request_json):
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_create_schema_with_base_schema(self, mock_request, request_json):
        request_json['base_schema_id'] = 2
        mock_request.json_body = request_json
        actual = schema_views.register_schema(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_invalid_schema_json(self, mock_request, request_json):
        request_json['schema'] = 'Not valid json!%#!#$#'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            'Error "Expecting value: line 1 column 1 (char 0)" encountered '
            'decoding JSON: "Not valid json!%#!#$#"'
        )

    def test_register_invalid_avro_format(self, mock_request, request_json):
        request_json['schema'] = '{"type": "record", "name": "A"}'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert "Invalid Avro schema JSON." in str(e.value)

    def test_register_invalid_namespace_name(self, mock_request, request_json):
        request_json['namespace'] = 'yelp|main'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == (
            'Source name or Namespace name should not contain the '
            'restricted character: |'
        )

    def test_register_numeric_src_name(self, mock_request, request_json):
        request_json['source'] = '12345'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'Source or Namespace name should not be numeric'


class TestRegisterSchemaFromMySQL(RegisterSchemaTestBase):

    @property
    def new_create_table_stmt(self):
        return 'create table `biz` (`id` int(11), `name` varchar(10));'

    @property
    def old_create_table_stmt(self):
        return 'create table `biz` (`id` int(11));'

    @property
    def alter_table_stmt(self):
        return 'alter table `biz` add column `name` varchar(10);'

    @pytest.fixture
    def request_json(self, biz_source):
        return {
            "new_create_table_stmt": self.new_create_table_stmt,
            "namespace": biz_source.namespace.name,
            "source": biz_source.name,
            "source_owner_email": 'biz.test@yelp.com',
            'contains_pii': False
        }

    def test_register_new_table(self, mock_request, request_json):
        mock_request.json_body = request_json
        actual = schema_views.register_schema_from_mysql_stmts(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_updated_table(self, mock_request, request_json):
        request_json["old_create_table_stmt"] = self.old_create_table_stmt
        request_json["alter_table_stmt"] = self.alter_table_stmt
        mock_request.json_body = request_json

        actual = schema_views.register_schema_from_mysql_stmts(mock_request)
        self._assert_equal_schema_response(actual, request_json)

    def test_register_invalid_sql_table_stmt(self, mock_request, request_json):
        request_json["new_create_table_stmt"] = 'create table biz ();'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert 'No column exists in the table.' in str(e.value)

    def test_register_table_with_unsupported_avro_type(
        self,
        mock_request,
        request_json
    ):
        request_json["new_create_table_stmt"] = 'create table biz (t blob);'
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert 'Unable to convert MySQL data type' in str(e.value)

    def test_register_invalid_avro_schema(self, mock_request, request_json):
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(422)
        with mock.patch.object(
            models.AvroSchema,
            'verify_avro_schema',
            return_value=(False, 'oops')
        ), pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert 'Invalid Avro schema JSON.' in str(e.value)

    def test_invalid_register_request(self, mock_request, request_json):
        request_json["old_create_table_stmt"] = self.old_create_table_stmt
        mock_request.json_body = request_json

        expected_exception = self.get_http_exception(400)
        expected_error = (
            'Both old_create_table_stmt and alter_table_stmt must be provided.'
        )

        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == expected_error


class TestGetSchemaElements(ApiTestBase):

    def test_non_existing_schema(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'schema_id': '0'}
            schema_views.get_schema_elements_by_schema_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_get_schema_elements(self, mock_request, biz_schema):
        mock_request.matchdict = {'schema_id': '{}'.format(biz_schema.id)}
        actual = schema_views.get_schema_elements_by_schema_id(mock_request)
        assert actual == self._get_expected_elements_response(biz_schema)

    def _get_expected_elements_response(self, biz_schema):
        response = []
        for element in biz_schema.avro_schema_elements:
            response.append(
                {
                    'id': element.id,
                    'schema_id': biz_schema.id,
                    'element_type': element.element_type,
                    'key': element.key,
                    'doc': element.doc,
                    'created_at': element.created_at.isoformat(),
                    'updated_at': element.updated_at.isoformat()
                }
            )

        return response
