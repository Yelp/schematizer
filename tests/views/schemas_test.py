# -*- coding: utf-8 -*-
import pytest
import simplejson
from avro import schema

from schematizer.api.exceptions import exceptions_v1
from schematizer.components.mysql_handlers import MySQLHandlerException
from schematizer.components.converters import converter_base
from schematizer.views import schemas as schema_views
from testing import factories
from tests.views.api_test_base import TestApiBase


class TestSchemasViewBase(TestApiBase):

    test_view_module = 'schematizer.views.schemas'

    @pytest.fixture
    def setup_mock_create_schema_func(self, mock_repo):
        mock_repo.create_avro_schema_from_avro_json.return_value = self.schema

    def assert_mock_create_schema_func_call(self, mock_repo, **param_override):
        expected_call_args = {
            'avro_schema_json': simplejson.loads(factories.fake_avro_schema),
            'namespace': factories.fake_namespace,
            'source': factories.fake_source,
            'domain_email_owner': factories.fake_owner_email,
            'base_schema_id': factories.fake_base_schema_id
        }
        expected_call_args.update(param_override)
        mock_repo.create_avro_schema_from_avro_json.assert_called_once_with(
            **expected_call_args
        )


class TestGetSchemaByID(TestSchemasViewBase):

    def test_non_existing_schema(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'schema_id': '0'})
            mock_repo.get_schema_by_id.return_value = None
            schema_views.get_schema_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_schema_by_id.assert_called_once_with(0)

    def test_get_schema_by_id(self, mock_request, mock_repo):
        mock_request.matchdict = self.get_mock_dict({'schema_id': '1'})
        mock_repo.get_schema_by_id.return_value = self.schema

        actual = schema_views.get_schema_by_id(mock_request)

        assert self.schema_response == actual
        mock_repo.get_schema_by_id.assert_called_once_with(1)

    def test_get_schema_by_id_with_base_schema(self, mock_request, mock_repo):
        self.schema.base_schema_id = 2
        mock_request.matchdict = self.get_mock_dict({'schema_id': '1'})
        mock_repo.get_schema_by_id.return_value = self.schema

        actual = schema_views.get_schema_by_id(mock_request)

        self.schema_response.update({'base_schema_id': 2})
        assert self.schema_response == actual
        mock_repo.get_schema_by_id.assert_called_once_with(1)


class TestRegisterSchema(TestSchemasViewBase):

    @property
    def request_json(self):
        return {
            "schema": factories.fake_avro_schema,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
            "source_owner_email": factories.fake_owner_email
        }

    @property
    def with_base_schema_request_json(self):
        return dict(
            self.request_json,
            base_schema_id=factories.fake_base_schema_id
        )

    @pytest.mark.usefixtures('setup_mock_create_schema_func')
    def test_register_schema(self, mock_request, mock_repo):
        mock_request.json_body = self.request_json

        actual = schema_views.register_schema(mock_request)

        assert self.schema_response == actual
        self.assert_mock_create_schema_func_call(
            mock_repo,
            base_schema_id=None,
        )

    @pytest.mark.usefixtures('setup_mock_create_schema_func')
    def test_register_schema_with_base_schema(self, mock_request, mock_repo):
        mock_request.json_body = self.with_base_schema_request_json

        actual = schema_views.register_schema(mock_request)

        self.schema_response.update(
            {'base_schema_id': factories.fake_base_schema_id}
        )
        assert self.schema_response == actual
        self.assert_mock_create_schema_func_call(
            mock_repo,
        )

    def test_register_schema_with_json_exception(
        self,
        mock_request
    ):
        mock_request.json_body = self.request_json
        mock_request.json_body['schema'] = 'Not valid json!%#!#$#'
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert expected_exception.code == e.value.code
        assert ('Error "Expecting value: line 1 column 1 (char 0)" encountered'
                ' decoding JSON: "Not valid json!%#!#$#"') == str(e.value)

    def test_register_schema_with_avro_exception(
        self,
        mock_request,
        mock_repo
    ):
        mock_request.json_body = self.request_json
        mock_repo.create_avro_schema_from_avro_json.side_effect = \
            schema.AvroException('oops')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            schema_views.register_schema(mock_request)

        assert expected_exception.code == e.value.code
        assert 'oops' == str(e.value)


class TestRegisterSchemaFromMySQL(TestSchemasViewBase):

    @property
    def request_json(self):
        return {
            "mysql_statements": factories.fake_mysql_create_stmts,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
            "source_owner_email": factories.fake_owner_email
        }

    @pytest.fixture
    def setup_mock_request_json_body(self, mock_request):
        mock_request.json_body = self.request_json

    @property
    def converted_schema(self):
        return 'converted avro schema'

    @pytest.mark.usefixtures(
        'setup_mock_request_json_body',
        'mock_create_sql_table_from_mysql_stmts'
    )
    def test_register_schema(self, mock_request, mock_repo):
        mock_repo.convert_schema.return_value = self.converted_schema
        mock_repo.create_avro_schema_from_avro_json.return_value = self.schema

        actual = schema_views.register_schema_from_mysql_stmts(mock_request)

        assert self.schema_response == actual
        self.assert_mock_create_schema_func_call(
            mock_repo,
            avro_schema_json=self.converted_schema,
            base_schema_id=None
        )

    @pytest.mark.usefixtures('setup_mock_request_json_body')
    def test_register_schema_with_mysql_handler_exception(
        self,
        mock_request,
        mock_create_sql_table_from_mysql_stmts
    ):
        mock_create_sql_table_from_mysql_stmts.side_effect = \
            MySQLHandlerException('oops')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert expected_exception.code == e.value.code
        assert 'oops' == str(e.value)

    @pytest.mark.usefixtures(
        'setup_mock_request_json_body',
        'mock_create_sql_table_from_mysql_stmts'
    )
    def test_register_schema_with_conversion_exception(
        self,
        mock_request,
        mock_repo
    ):
        mock_repo.convert_schema.side_effect = \
            converter_base.SchemaConversionException('oops')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert expected_exception.code == e.value.code
        assert 'oops' == str(e.value)

    @pytest.mark.usefixtures(
        'setup_mock_request_json_body',
        'mock_create_sql_table_from_mysql_stmts'
    )
    def test_register_schema_with_avro_exception(
        self,
        mock_request,
        mock_repo
    ):
        mock_repo.create_avro_schema_from_avro_json.side_effect = \
            schema.AvroException('oops')
        expected_exception = self.get_http_exception(422)

        with pytest.raises(expected_exception) as e:
            schema_views.register_schema_from_mysql_stmts(mock_request)

        assert expected_exception.code == e.value.code
        assert 'oops' == str(e.value)
