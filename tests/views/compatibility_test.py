# -*- coding: utf-8 -*-
import mock
import pytest
import simplejson
from avro import schema

from schematizer.components.converters import converter_base
from schematizer.components.mysql_handlers import MySQLHandlerException
from schematizer.views import compatibility as compatibility_views
from testing import factories
from tests.views.api_test_base import TestApiBase


class TestCompatibilityViewBase(TestApiBase):

    test_view_module = 'schematizer.views.compatibility'

    @pytest.yield_fixture
    def mock_repo(self):
        with mock.patch(
            self.test_view_module + '.schema_repo',
            autospec=True
        ) as mock_repo:
            yield mock_repo


class TestAvroSchemaCompatibility(TestCompatibilityViewBase):

    @property
    def request_json(self):
        return {
            "schema": factories.fake_avro_schema,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source
        }

    def test_compatible_schema(self, mock_request, mock_repo):
        mock_request.json_body = self.request_json
        mock_repo.is_schema_compatible.return_value = True

        actual = compatibility_views.is_avro_schema_compatible(mock_request)

        assert True == actual
        mock_repo.is_schema_compatible.assert_called_once_with(
            simplejson.loads(factories.fake_avro_schema),
            factories.fake_namespace,
            factories.fake_source
        )

    def test_compatible_schema_with_avro_exception(
        self,
        mock_request,
        mock_repo
    ):
        mock_request.json_body = self.request_json
        mock_repo.is_schema_compatible.side_effect = schema.AvroException('ex')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            compatibility_views.is_avro_schema_compatible(mock_request)

        assert expected_exception.code == e.value.code
        assert 'ex' == str(e.value)


class TestMySQLSchemaCompatibility(TestCompatibilityViewBase):

    @property
    def request_json(self):
        return {
            "mysql_statements": factories.fake_mysql_create_stmts,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source
        }

    @pytest.fixture
    def setup_mock_request_json_body(self, mock_request):
        mock_request.json_body = self.request_json

    @property
    def converted_schema(self):
        return 'converted avro schema'

    @pytest.fixture
    def setup_mock_convert_schema_func(self, mock_repo):
        mock_repo.convert_schema.return_value = self.converted_schema

    @property
    def compatible_value(self):
        return True

    @pytest.fixture
    def setup_mock_is_schema_compatible_func(self, mock_repo):
        mock_repo.is_schema_compatible.return_value = self.compatible_value

    @pytest.mark.usefixtures(
        'setup_mock_request_json_body',
        'mock_create_sql_table_from_mysql_stmts',
        'setup_mock_convert_schema_func',
        'setup_mock_is_schema_compatible_func',
    )
    def test_compatible_schema(self, mock_request, mock_repo):
        actual = compatibility_views.is_mysql_schema_compatible(mock_request)

        assert self.compatible_value == actual
        mock_repo.is_schema_compatible.assert_called_once_with(
            self.converted_schema,
            factories.fake_namespace,
            factories.fake_source
        )

    @pytest.mark.usefixtures('setup_mock_request_json_body')
    def test_compatible_schema_with_mysql_handler_exception(
        self,
        mock_request,
        mock_create_sql_table_from_mysql_stmts
    ):
        mock_create_sql_table_from_mysql_stmts.side_effect = \
            MySQLHandlerException('oops')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        assert expected_exception.code == e.value.code
        assert 'oops' == str(e.value)

    @pytest.mark.usefixtures(
        'setup_mock_request_json_body',
        'mock_create_sql_table_from_mysql_stmts',
        'setup_mock_is_schema_compatible_func',
    )
    def test_compatible_schema_with_conversion_exception(
        self,
        mock_request,
        mock_repo
    ):
        mock_repo.convert_schema.side_effect = \
            converter_base.SchemaConversionException('oops')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        assert expected_exception.code == e.value.code
        assert 'oops' == str(e.value)

    @pytest.mark.usefixtures(
        'setup_mock_request_json_body',
        'mock_create_sql_table_from_mysql_stmts',
        'setup_mock_convert_schema_func',
    )
    def test_compatible_schema_with_avro_exception(
        self,
        mock_request,
        mock_repo
    ):
        mock_repo.is_schema_compatible.side_effect = schema.AvroException('ex')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        assert expected_exception.code == e.value.code
        assert 'ex' == str(e.value)
