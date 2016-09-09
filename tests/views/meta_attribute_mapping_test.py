# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models import AvroSchema
from schematizer.models import Namespace
from schematizer.models import Source
from schematizer.views import meta_attribute_mapping as meta_attr_views
from testing import factories
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
            meta_attr_views.get_meta_attr_mappings_by_namespace(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} name `{1}` not found.'.format(
            self.entity_type, fake_namespace_name
        )

    def test_happy_case(self, mock_request, meta_attr_schema, yelp_namespace):
        self._setup_meta_attribute_mapping(meta_attr_schema, yelp_namespace)
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        actual = meta_attr_views.get_meta_attr_mappings_by_namespace(
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
        fake_entity_id = 1234
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
            get_meta_attr_mappings_by_source_id
        self.entity = biz_source


@pytest.mark.usefixtures('setup_test')
class TestGetMetaAttributesBySchema(GetMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_type = AvroSchema.__name__
        self.entity_type_id = 'schema_id'
        self.getter_logic_method = meta_attr_views.\
            get_meta_attr_mappings_by_schema_id
        self.entity = biz_schema


class RegisterMetaAttributeBase(ApiTestBase):
    """Entities here can be Namespaces, Sources or Schemas. You need to
    implement the following to use this Base class:

        entity_type: The entity for which you are querying the MetaAttributes
        register_logic_method: The logic funtion to add a mapping.
        delete_logic_method: The logic function to remove a mapping.
        entity: A sample test entity to run tests against.
    """

    def test_non_existing_entity(
        self,
        mock_request,
        meta_attr_schema
    ):
        expected_exception = self.get_http_exception(404)
        fake_entity_id = 1234
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = {
                'entity_id': fake_entity_id,
                'meta_attribute_schema_id': meta_attr_schema.id
            }
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} id {1} not found.'.format(
            self.entity_type, fake_entity_id
        )

    def test_non_existing_meta_attribute(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_meta_attr_id = 1234
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = {
                'entity_id': self.entity.id,
                'meta_attribute_schema_id': fake_meta_attr_id
            }
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == 'AvroSchema id {0} not found.'.format(
            fake_meta_attr_id
        )

    def test_registration_and_idempotency(
        self,
        mock_request,
        meta_attr_schema
    ):
        mock_request.json_body = {
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

    def test_deletion(self, mock_request, meta_attr_schema):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            self.entity.id
        )
        mock_request.json_body = {
            'entity_id': self.entity.id,
            'meta_attribute_schema_id': meta_attr_schema.id
        }
        self.delete_logic_method(mock_request)
        assert self.get_expected_meta_attr_response(
            self.entity_type, self.entity.id
        ) == {}


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForNamespace(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, yelp_namespace):
        self.entity_type = Namespace.__name__
        self.register_logic_method = meta_attr_views.\
            register_meta_attribute_mapping_for_namespace
        self.delete_logic_method = meta_attr_views.\
            delete_meta_attribute_mapping_for_namespace
        self.entity = yelp_namespace


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForSource(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_source):
        self.entity_type = Source.__name__
        self.register_logic_method = meta_attr_views.\
            register_meta_attribute_mapping_for_source
        self.delete_logic_method = meta_attr_views.\
            delete_meta_attribute_mapping_for_source
        self.entity = biz_source


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForSchema(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_type = AvroSchema.__name__
        self.register_logic_method = meta_attr_views.\
            register_meta_attribute_mapping_for_schema
        self.delete_logic_method = meta_attr_views.\
            delete_meta_attribute_mapping_for_schema
        self.entity = biz_schema
