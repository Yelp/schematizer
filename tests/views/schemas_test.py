# -*- coding: utf-8 -*-
import mock
import pytest
import simplejson
from pyramid.httpexceptions import HTTPNotFound

from schematizer.views import constants
from schematizer.views.schemas import get_schema_by_id
from schematizer.views.schemas import register_avro_schema
from schematizer.views.schemas import register_avro_schema_from_mysql_stmts
from testing import factories
from tests.views.api_test_base import TestApiBase


class TestSchemasViewBase(TestApiBase):

    test_view_module = 'schematizer.views.schemas'


class TestGetSchemaByID(TestSchemasViewBase):

    def test_non_existing_schema(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_request.matchdict = self.get_mock_dict({'schema_id': '0'})
            mock_repo.get_schema_by_id.return_value = None
            get_schema_by_id(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.SCHEMA_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_schema_by_id.assert_called_once_with(0)

    def test_get_schema_by_id(self, mock_request, mock_repo):
        mock_request.matchdict = self.get_mock_dict({'schema_id': '1'})
        mock_repo.get_schema_by_id.return_value = self.schema

        actual = get_schema_by_id(mock_request)

        assert self.schema_response == actual
        mock_repo.get_schema_by_id.assert_called_once_with(1)

    def test_get_schema_by_id_with_base_schema(self, mock_request, mock_repo):
        self.schema.base_schema_id = 2
        mock_request.matchdict = self.get_mock_dict({'schema_id': '1'})
        mock_repo.get_schema_by_id.return_value = self.schema

        actual = get_schema_by_id(mock_request)

        self.schema_response.update({'base_schema_id': 2})
        assert self.schema_response == actual
        mock_repo.get_schema_by_id.assert_called_once_with(1)


class TestRegisterAvroSchema(TestSchemasViewBase):

    @property
    def request_string(self):
        request_json = {
            "schema": factories.fake_avro_schema,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
            "source_owner_email": factories.fake_owner_email
        }
        return simplejson.dumps(request_json)

    @property
    def with_base_schema_request_str(self):
        request_json = {
            "schema": factories.fake_avro_schema,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
            "source_owner_email": factories.fake_owner_email,
            "base_schema_id": factories.fake_base_schema_id
        }
        return simplejson.dumps(request_json)

    def test_register_schema(self, mock_request, mock_repo):
        mock_request.body = self.request_string
        mock_repo.create_avro_schema_from_avro_json.return_value = self.schema

        actual = register_avro_schema(mock_request)

        assert self.schema_response == actual
        mock_repo.create_avro_schema_from_avro_json.assert_called_once_with(
            avro_schema_json=factories.fake_avro_schema,
            namespace=factories.fake_namespace,
            source=factories.fake_source,
            domain_email_owner=factories.fake_owner_email,
            base_schema_id=None
        )

    def test_register_schema_with_base_schema(self, mock_request, mock_repo):
        mock_request.body = self.with_base_schema_request_str
        mock_repo.create_avro_schema_from_avro_json.return_value = self.schema

        actual = register_avro_schema(mock_request)

        self.schema_response.update(
            {'base_schema_id': factories.fake_base_schema_id}
        )
        assert self.schema_response == actual
        mock_repo.create_avro_schema_from_avro_json.assert_called_once_with(
            avro_schema_json=factories.fake_avro_schema,
            namespace=factories.fake_namespace,
            source=factories.fake_source,
            domain_email_owner=factories.fake_owner_email,
            base_schema_id=factories.fake_base_schema_id
        )


class TestRegisterAvroSchemaFromMySQL(TestSchemasViewBase):

    @property
    def request_string(self):
        request_json = {
            "mysql_statements": factories.fake_mysql_create_stmts,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source,
            "source_owner_email": factories.fake_owner_email
        }
        return simplejson.dumps(request_json)

    def test_register_schema(self, mock_request, mock_repo,
                             mock_mysql_processor):
        mock_request.body = self.request_string
        avro_schema_to_register = mock.Mock()
        mock_repo.convert_schema.return_value = avro_schema_to_register
        mock_repo.create_avro_schema_from_avro_json.return_value = self.schema

        actual = register_avro_schema_from_mysql_stmts(mock_request)

        assert self.schema_response == actual
        mock_repo.create_avro_schema_from_avro_json.assert_called_once_with(
            avro_schema_json=avro_schema_to_register,
            namespace=factories.fake_namespace,
            source=factories.fake_source,
            domain_email_owner=factories.fake_owner_email
        )
