# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

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
            self.entity_type,
            fake_namespace_name
        )

    def test_happy_case(self, mock_request, meta_attr_schema, yelp_namespace):
        self._setup_meta_attribute_mapping(meta_attr_schema, yelp_namespace)
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        actual = meta_attr_views.get_namespace_meta_attribute_mappings(
            mock_request
        )
        expected = self.get_expected_meta_attr_response(
            self.entity_type,
            yelp_namespace.id
        )
        assert actual == expected


class TestGetMetaAttributesBySource(ApiTestBase):

    entity_type = Source.__name__

    def _setup_meta_attribute_mapping(self, meta_attr_schema, biz_source):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            biz_source.id
        )

    def test_non_existing_entity(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_source_id = 0
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': fake_source_id}
            meta_attr_views.get_source_meta_attribute_mappings(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} id {1} not found.'.format(
            self.entity_type,
            fake_source_id
        )

    def test_happy_case(self, mock_request, meta_attr_schema, biz_source):
        self._setup_meta_attribute_mapping(meta_attr_schema, biz_source)
        mock_request.matchdict = {'source_id': biz_source.id}
        actual = meta_attr_views.get_source_meta_attribute_mappings(
            mock_request
        )
        expected = self.get_expected_meta_attr_response(
            self.entity_type,
            biz_source.id
        )
        assert actual == expected


class RegisterMetaAttributeBase(ApiTestBase):
    """Entities here can be Namespaces or Sources. You need to implement the
    following to use this Base class:

        entity_type: The entity for which you are querying the MetaAttributes
        register_logic_method: The logic funtion to add a mapping.
        delete_logic_method: The logic function to remove a mapping.
        entity: A sample test entity to run tests against.
    """

    def test_non_existing_entity(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_id = 0
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {self.entity_key: fake_id}
            mock_request.json_body = self.req_json_body
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == '{0} id {1} not found.'.format(
            self.entity_type,
            fake_id
        )

    def test_non_existing_meta_attribute(self, mock_request):
        expected_exception = self.get_http_exception(404)
        fake_id = 0
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.req_matchdict
            mock_request.json_body = {'meta_attribute_schema_id': fake_id}
            self.register_logic_method(mock_request)
        assert e.value.code == expected_exception.code
        assert str(e.value) == 'AvroSchema id {0} not found.'.format(
            fake_id
        )

    def test_registration_and_idempotency(self, mock_request):
        mock_request.json_body = self.req_json_body
        mock_request.matchdict = self.req_matchdict
        actual = self.register_logic_method(mock_request)
        expected = self.get_expected_meta_attr_response(
            self.entity_type,
            self.entity.id
        )[0]
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
        mock_request.json_body = self.req_json_body
        mock_request.matchdict = self.req_matchdict
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
            mock_request.json_body = self.req_json_body
            mock_request.matchdict = self.req_matchdict
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
        self.req_matchdict = {self.entity_key: yelp_namespace.name}
        self.req_json_body = {'meta_attribute_schema_id': meta_attr_schema.id}
        self.entity = yelp_namespace
        self.register_logic_method = (
            meta_attr_views.register_namespace_meta_attribute_mapping)
        self.delete_logic_method = (
            meta_attr_views.delete_namespace_meta_attribute_mapping)

    def test_non_existing_entity(self, mock_request):
        """Overriding this test because unlike Sources and AvroSchemas,
        Namespace uses name instead of id to register mappings."""
        expected_exception = self.get_http_exception(404)
        fake_entity_name = 'fake_namepsace'
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = self.req_json_body
            mock_request.matchdict = {'namespace': fake_entity_name}
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
        self.req_matchdict = {self.entity_key: biz_source.id}
        self.req_json_body = {'meta_attribute_schema_id': meta_attr_schema.id}
        self.entity = biz_source
        self.register_logic_method = (
            meta_attr_views.register_source_meta_attribute_mapping)
        self.delete_logic_method = (
            meta_attr_views.delete_source_meta_attribute_mapping)
