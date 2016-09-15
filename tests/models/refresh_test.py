# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.refresh import Refresh
from schematizer_testing import factories
from tests.models.base_model_test import GetAllModelTestBase


class TestGetAllRefreshes(GetAllModelTestBase):

    def create_refresh(self, refresh_no):
        source_bar = factories.get_or_create_source(
            namespace_name='foo',
            source_name='bar',
            owner_email='test.dev@yelp.com'
        )
        return factories.create_refresh(source_id=source_bar.id)

    entity_model = Refresh
    create_entity_func = create_refresh
    assert_func_name = 'assert_equal_refresh'
