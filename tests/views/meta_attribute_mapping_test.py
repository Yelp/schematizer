# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models import EntityType
from schematizer.api.exceptions import exceptions_v1
from schematizer.views import meta_attribute_mapping as meta_attr_views
from testing import factories
from tests.views.api_test_base import ApiTestBase


class GetMetaAttributeBase(ApiTestBase):
    """Entities here can be Namespaces, Sources or Schemas. You need to implement
    the following to use this Base class:

        entity_type: The entity for which you are querying the MetaAttributes
        entity_type_id: The id column name for it
        getter_logic_method: The logic funtion being tested.
        entity: A sample test entity to run tests against.
        entity_not_found_exception: Exception when entity doesn't exist.
    """

    def _setup_meta_attribute_mapping(self, meta_attr_schema, entity_id):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            entity_id
        )

    def test_non_existing_entity(self, mock_request, setup_test):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {self.entity_type_id: 1234}
            self.getter_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == self.entity_not_found_exception

    def test_happy_case(self, mock_request, meta_attr_schema, setup_test):
        self._setup_meta_attribute_mapping(meta_attr_schema, self.entity.id)
        mock_request.matchdict = {self.entity_type_id: self.entity.id}
        actual = self.getter_logic_method(mock_request)
        expected = self.get_expected_meta_attr_response(self.entity_type, self.entity.id)
        assert actual == expected


class TestGetMetaAttributesByNamespace(GetMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, yelp_namespace):
        self.entity_type = EntityType.NAMESPACE
        self.entity_type_id = self.entity_type + '_id'
        self.getter_logic_method = meta_attr_views.get_meta_attr_mappings_by_namespace_id
        self.entity = yelp_namespace
        self.entity_not_found_exception = exceptions_v1.NAMESPACE_NOT_FOUND_ERROR_MESSAGE


class TestGetMetaAttributesBySource(ApiTestBase):

    @pytest.fixture
    def setup_test(self, biz_source):
        self.entity_type = EntityType.SOURCE
        self.entity_type_id = self.entity_type + '_id'
        self.getter_logic_method = meta_attr_views.get_meta_attr_mappings_by_source_id
        self.entity = biz_source
        self.entity_not_found_exception = exceptions_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE


class TestGetMetaAttributesBySchema(ApiTestBase):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_type = EntityType.SCHEMA
        self.entity_type_id = self.entity_type + '_id'
        self.getter_logic_method = meta_attr_views.get_meta_attr_mappings_by_schema_id
        self.entity = biz_schema
        self.entity_not_found_exception = exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE


class RegisterMetaAttributeBase(ApiTestBase):
    """Entities here can be Namespaces, Sources or Schemas. You need to implement
    the following to use this Base class:

        entity_type: The entity for which you are querying the MetaAttributes
        register_logic_method: The logic funtion to add a mapping.
        delete_logic_method: The logic function to remove a mapping.
        entity: A sample test entity to run tests against.
        entity_not_found_exception: Exception when entity doesn't exist.
    """

    def test_non_existing_entity(self, setup_test, mock_request, meta_attr_schema):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {
                'entity_id': 1234,
                'meta_attribute_schema_id': meta_attr_schema.id
            }
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == self.entity_not_found_exception

    def test_non_existing_meta_attribute(self, setup_test, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {
                'entity_id': self.entity.id,
                'meta_attribute_schema_id': 1234
            }
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_registration_and_idempotency(self, setup_test, mock_request, meta_attr_schema):
        mock_request.matchdict = {
            'entity_id': self.entity.id,
            'meta_attribute_schema_id': meta_attr_schema.id
        }
        actual = self.register_logic_method(mock_request)
        expected = self.get_expected_meta_attr_response(
            self.entity_type, self.entity.id
        )
        assert actual == expected

        # Calling it again should not add a duplicate row.
        actual = self.register_logic_method(mock_request)
        assert actual == expected

    def test_deletion(self, setup_test, mock_request, meta_attr_schema):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            self.entity.id
        )
        mock_request.matchdict = {
            'entity_id': self.entity.id,
            'meta_attribute_schema_id': meta_attr_schema.id
        }
        self.delete_logic_method(mock_request)
        assert self.get_expected_meta_attr_response(
                self.entity_type, self.entity.id
            ) == {}


class TestRegisterMetaAttributeForNamespace(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, yelp_namespace):
        self.entity_type = EntityType.NAMESPACE
        self.register_logic_method = meta_attr_views.register_meta_attribute_mapping_for_namespace
        self.delete_logic_method = meta_attr_views.delete_meta_attribute_mapping_for_namespace
        self.entity = yelp_namespace
        self.entity_not_found_exception = exceptions_v1.NAMESPACE_NOT_FOUND_ERROR_MESSAGE


class TestRegisterMetaAttributeForSource(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_source):
        self.entity_type = EntityType.SOURCE
        self.register_logic_method = meta_attr_views.register_meta_attribute_mapping_for_source
        self.delete_logic_method = meta_attr_views.delete_meta_attribute_mapping_for_source
        self.entity = biz_source
        self.entity_not_found_exception = exceptions_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE


class TestRegisterMetaAttributeForSchema(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_type = EntityType.SCHEMA
        self.register_logic_method = meta_attr_views.register_meta_attribute_mapping_for_schema
        self.delete_logic_method = meta_attr_views.delete_meta_attribute_mapping_for_schema
        self.entity = biz_schema
        self.entity_not_found_exception = exceptions_v1.SCHEMA_NOT_FOUND_ERROR_MESSAGE
