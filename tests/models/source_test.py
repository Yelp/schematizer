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

from schematizer.models.source import Source
from schematizer_testing import factories
from tests.models.base_model_test import GetAllModelTestBase


class TestGetAllSources(GetAllModelTestBase):

    def create_source(self, source_no):
        return factories.create_source(
            namespace_name='foo',
            source_name='source_{}'.format(source_no),
            owner_email='test.dev@yelp.com'
        )

    entity_model = Source
    create_entity_func = create_source
    assert_func_name = 'assert_equal_source'
