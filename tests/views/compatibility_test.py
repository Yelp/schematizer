# -*- coding: utf-8 -*-
import mock
import pytest
import simplejson

from schematizer.views.compatibility import is_avro_schema_compatible
from schematizer.views.compatibility import is_mysql_schema_compatible
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
    def request_string(self):
        request_json = {
            "schema": factories.fake_avro_schema,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source
        }
        return simplejson.dumps(request_json)

    def test_compatible_schema(self, mock_request, mock_repo):
        mock_request.body = self.request_string
        mock_repo.is_schema_compatible.return_value = True

        actual = is_avro_schema_compatible(mock_request)

        assert True == actual
        mock_repo.is_schema_compatible.assert_called_once_with(
            simplejson.loads(factories.fake_avro_schema),
            factories.fake_namespace,
            factories.fake_source
        )


class TestMySQLSchemaCompatibility(TestCompatibilityViewBase):

    @property
    def request_string(self):
        request_json = {
            "mysql_statements": factories.fake_mysql_create_stmts,
            "namespace": factories.fake_namespace,
            "source": factories.fake_source
        }
        return simplejson.dumps(request_json)

    @pytest.mark.usefixtures('mock_mysql_processor')
    def test_compatible_schema(self, mock_request, mock_repo):
        mock_request.body = self.request_string
        avro_schema_to_check = mock.Mock()
        mock_repo.convert_schema.return_value = avro_schema_to_check
        mock_repo.is_schema_compatible.return_value = True

        actual = is_mysql_schema_compatible(mock_request)

        assert True == actual
        mock_repo.is_schema_compatible.assert_called_once_with(
            avro_schema_to_check,
            factories.fake_namespace,
            factories.fake_source
        )
