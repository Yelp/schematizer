# -*- coding: utf-8 -*-
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
