# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
import simplejson
from avro import schema

from schematizer.components.converters import converter_base
from schematizer.components.handlers import sql_handler_base
from schematizer.views import compatibility as compatibility_views
from schematizer.views import view_common
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

        assert actual is True
        mock_repo.is_schema_compatible.assert_called_once_with(
            simplejson.loads(factories.fake_avro_schema),
            factories.fake_namespace,
            factories.fake_source
        )

    def test_compatible_schema_with_json_exception(
        self,
        mock_request
    ):
        mock_request.json_body = self.request_json
        mock_request.json_body['schema'] = 'Not valid json!%#!#$#'
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            compatibility_views.is_avro_schema_compatible(mock_request)

        assert expected_exception.code == e.value.code
        assert ('Error "Expecting value: line 1 column 1 (char 0)" encountered'
                ' decoding JSON: "Not valid json!%#!#$#"') == str(e.value)

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
    def new_create_table_stmt(self):
        return 'create new table'

    @property
    def old_create_table_stmt(self):
        return 'create old table'

    @pytest.fixture
    def setup_mock_request_for_new_table(self, mock_request):
        mock_request.json_body = {
            "new_create_table_stmt": self.new_create_table_stmt,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source
        }

    @property
    def converted_schema(self):
        return 'converted avro schema'

    @property
    def compatible_value(self):
        return True

    @pytest.yield_fixture
    def mock_create_sql_table_from_mysql_stmts(self):
        with mock.patch.object(
            view_common.sql_handler,
            'create_sql_table_from_sql_stmts',
        ) as mock_func:
            yield mock_func

    @pytest.mark.parametrize("request_body", [
        {
            "new_create_table_stmt": 'create new table',
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
        },
        {
            "new_create_table_stmt": 'create new table',
            "old_create_table_stmt": 'create old table',
            "alter_table_stmt": 'update existing table',
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
        },
    ])
    @pytest.mark.usefixtures(
        'mock_create_sql_table_from_mysql_stmts'
    )
    def test_compatible_schema(
        self,
        mock_request,
        mock_repo,
        request_body
    ):
        mock_request.json_body = request_body
        mock_repo.convert_schema.return_value = self.converted_schema
        mock_repo.is_schema_compatible.return_value = self.compatible_value

        actual = compatibility_views.is_mysql_schema_compatible(mock_request)

        assert self.compatible_value == actual
        mock_repo.is_schema_compatible.assert_called_once_with(
            self.converted_schema,
            factories.fake_namespace,
            factories.fake_source
        )

    @pytest.mark.usefixtures('setup_mock_request_for_new_table')
    def test_compatible_schema_with_mysql_handler_exception(
        self,
        mock_request,
        mock_create_sql_table_from_mysql_stmts
    ):
        mock_create_sql_table_from_mysql_stmts.side_effect = \
            sql_handler_base.SQLHandlerException('oops')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        assert expected_exception.code == e.value.code
        assert 'oops' == str(e.value)

    @pytest.mark.usefixtures(
        'setup_mock_request_for_new_table',
        'mock_create_sql_table_from_mysql_stmts',
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
        'setup_mock_request_for_new_table',
        'mock_create_sql_table_from_mysql_stmts'
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

    def test_compatible_schema_with_invalid_request(
        self,
        mock_request,
        mock_repo
    ):
        mock_request.json_body = {
            "new_create_table_stmt": self.new_create_table_stmt,
            "old_create_table_stmt": self.old_create_table_stmt,
            "alter_table_stmt": None,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
        }
        expected_exception = self.get_http_exception(400)

        with pytest.raises(expected_exception) as e:
            compatibility_views.is_mysql_schema_compatible(mock_request)

        expected_err = 'Both old_create_table_stmt and alter_table_stmt ' \
                       'must be provided.'
        assert expected_exception.code == e.value.code
        assert expected_err == str(e.value)
