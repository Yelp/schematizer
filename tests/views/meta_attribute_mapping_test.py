# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models import AvroSchema
from schematizer.models import Namespace
from schematizer.models import Source
from schematizer.views import meta_attribute_mapping as meta_attr_views
from schematizer_testing import factories
from tests.views.api_test_base import ApiTestBase


class TestGetMetaAttributesByNamespace(ApiTestBase):

    entity_type = Namespace.__name__

    def _setup_meta_attribute_mapping(self, meta_attr_schema, namespace):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            namespace.id
        )

    def test_non_existing_entity(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_namespace_name = 'not_a_namepsace'
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'namespace': fake_namespace_name}
            meta_attr_views.get_namespace_meta_attribute_mappings(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} name `{1}` not found.'.format(
            self.entity_type, fake_namespace_name
        )

    def test_happy_case(self, mock_request, meta_attr_schema, yelp_namespace):
        self._setup_meta_attribute_mapping(meta_attr_schema, yelp_namespace)
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        actual = meta_attr_views.get_namespace_meta_attribute_mappings(
            mock_request
        )
        expected = self.get_expected_meta_attr_response(
            self.entity_type, yelp_namespace.id
        )
        assert actual == expected


class GetMetaAttributeBase(ApiTestBase):
    """Entities here can be Namespaces, Sources or Schemas. You need to
    implement the following to use this Base class:

        entity_type: The entity for which you are querying the MetaAttributes
        entity_type_id: The id column name for it
        getter_logic_method: The logic funtion being tested.
        entity: A sample test entity to run tests against.
    """

    def _setup_meta_attribute_mapping(self, meta_attr_schema, entity_id):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            entity_id
        )

    def test_non_existing_entity(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_entity_id = 0
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {self.entity_type_id: fake_entity_id}
            self.getter_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} id {1} not found.'.format(
            self.entity_type, fake_entity_id
        )

    def test_happy_case(self, mock_request, meta_attr_schema):
        self._setup_meta_attribute_mapping(meta_attr_schema, self.entity.id)
        mock_request.matchdict = {self.entity_type_id: self.entity.id}
        actual = self.getter_logic_method(mock_request)
        expected = self.get_expected_meta_attr_response(
            self.entity_type, self.entity.id
        )
        assert actual == expected


@pytest.mark.usefixtures('setup_test')
class TestGetMetaAttributesBySource(GetMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_source):
        self.entity_type = Source.__name__
        self.entity_type_id = 'source_id'
        self.getter_logic_method = meta_attr_views.\
            get_source_meta_attribute_mappings
        self.entity = biz_source


@pytest.mark.usefixtures('setup_test')
class TestGetMetaAttributesBySchema(GetMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_type = AvroSchema.__name__
        self.entity_type_id = 'schema_id'
        self.getter_logic_method = meta_attr_views.\
            get_schema_meta_attribute_mappings
        self.entity = biz_schema


class RegisterMetaAttributeBase(ApiTestBase):
    """Entities here can be Namespaces, Sources or Schemas. You need to
    implement the following to use this Base class:

        entity_type: The entity for which you are querying the MetaAttributes
        register_logic_method: The logic funtion to add a mapping.
        delete_logic_method: The logic function to remove a mapping.
        entity: A sample test entity to run tests against.
    """

    def test_non_existing_entity(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_entity_id = 0
        with pytest.raises(expected_exception) as e:
            request_dict = self.req_dict.copy()
            request_dict[self.entity_key] = fake_entity_id
            mock_request.json_body = request_dict
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} id {1} not found.'.format(
            self.entity_type,
            fake_entity_id
        )

    def test_non_existing_meta_attribute(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_meta_attr_id = 0
        with pytest.raises(expected_exception) as e:
            request_dict = self.req_dict.copy()
            request_dict['meta_attribute_schema_id'] = fake_meta_attr_id
            mock_request.json_body = request_dict
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == 'AvroSchema id {0} not found.'.format(
            fake_meta_attr_id
        )

    def test_registration_and_idempotency(self, mock_request):
        mock_request.json_body = self.req_dict.copy()
        actual = self.register_logic_method(mock_request)
        expected = self.get_expected_meta_attr_response(
            self.entity_type,
            self.entity.id
        )
        assert actual == expected

        # Calling it again should not add a duplicate row.
        mock_request.json_body = self.req_dict.copy()
        actual = self.register_logic_method(mock_request)
        assert actual == expected

    def test_deletion(self, mock_request, meta_attr_schema):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            self.entity.id
        )
        mock_request.json_body = self.req_dict.copy()
        self.delete_logic_method(mock_request)
        assert self.get_expected_meta_attr_response(
            self.entity_type,
            self.entity.id
        ) == {}

    def test_non_existing_mapping_deletion(
        self,
        mock_request,
        meta_attr_schema
    ):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = self.req_dict.copy()
            self.delete_logic_method(mock_request)
            assert e.value.code == expected_exception.code
            expected_err_msg = {
                self.entity_type.__name__: self.entity.id,
                'meta_attribute_schema_id': meta_attr_schema.id
            }
            assert str(e.value) == (
                'MetaAttributeMappingStore mapping {0} not found.'.format(
                    expected_err_msg
                ))


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForNamespace(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, yelp_namespace, meta_attr_schema):
        self.entity_type = Namespace.__name__
        self.entity_key = 'namespace'
        self.req_dict = {
            self.entity_key: yelp_namespace.name,
            'meta_attribute_schema_id': meta_attr_schema.id
        }
        self.entity = yelp_namespace
        self.register_logic_method = (
            meta_attr_views.register_namepsace_meta_attribute_mapping)
        self.delete_logic_method = (
            meta_attr_views.delete_namespace_meta_attribute_mapping)

    def test_non_existing_entity(self, mock_request):
        """Overriding this test because unlike Sources and AvroSchemas,
        Namespace uses name instead of id to register mappings."""
        expected_exception = self.get_http_exception(404)
        fake_entity_name = 'fake_namepsace'
        with pytest.raises(expected_exception) as e:
            request_dict = self.req_dict.copy()
            request_dict[self.entity_key] = fake_entity_name
            mock_request.json_body = request_dict
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} name `{1}` not found.'.format(
            self.entity_type,
            fake_entity_name
        )


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForSource(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_source, meta_attr_schema):
        self.entity_type = Source.__name__
        self.entity_key = 'source_id'
        self.req_dict = {
            self.entity_key: biz_source.id,
            'meta_attribute_schema_id': meta_attr_schema.id
        }
        self.entity = biz_source
        self.register_logic_method = (
            meta_attr_views.register_source_meta_attribute_mapping)
        self.delete_logic_method = (
            meta_attr_views.delete_source_meta_attribute_mapping)


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForSchema(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_schema, meta_attr_schema):
        self.entity_type = AvroSchema.__name__
        self.entity_req = {'schema_id': biz_schema}
        self.entity_key = 'schema_id'
        self.req_dict = {
            self.entity_key: biz_schema.id,
            'meta_attribute_schema_id': meta_attr_schema.id
        }
        self.entity = biz_schema
        self.register_logic_method = (
            meta_attr_views.register_schema_meta_attribute_mapping)
        self.delete_logic_method = (
            meta_attr_views.delete_schema_meta_attribute_mapping)
