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

from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.meta_attribute_mapping_store import (
    MetaAttributeMappingStore)
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.testing_db import DBTestCase


class TestGetMetaAttributeMappingByMapping(DBTestCase):

    @pytest.fixture
    def namespace_foo(self):
        return factories.create_namespace('foo')

    @pytest.fixture
    def meta_attr_mapping_bar(self, namespace_foo, meta_attr_schema):
        return factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            namespace_foo.__class__.__name__,
            namespace_foo.id
        )

    def test_happy_case(self, meta_attr_mapping_bar):
        actual = MetaAttributeMappingStore.get_by_mapping(
            meta_attr_mapping_bar.entity_type,
            meta_attr_mapping_bar.entity_id,
            meta_attr_mapping_bar.meta_attr_schema_id
        )
        asserts.assert_equal_meta_attribute_mapping(
            actual,
            expected=meta_attr_mapping_bar
        )

    def test_non_existed_namespace(self, meta_attr_mapping_bar):
        fake_meta_attr_schema_id = 0
        with pytest.raises(EntityNotFoundError):
            MetaAttributeMappingStore.get_by_mapping(
                meta_attr_mapping_bar.entity_type,
                meta_attr_mapping_bar.entity_id,
                fake_meta_attr_schema_id
            )
