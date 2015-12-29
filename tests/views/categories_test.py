# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.views import categories as categories
from testing import factories
from tests.views.api_test_base import ApiTestBase


class TestListCategories(ApiTestBase):

    def test_no_categories(self, mock_request):
        actual = categories.list_categories(mock_request)
        assert actual == []

    @pytest.fixture
    def biz_category_assigned_to_biz_src(self, biz_source):
        return factories.create_source_category(biz_source.id, 'biz')

    @pytest.fixture
    def review_source(self, yelp_namespace_name):
        return factories.create_source(yelp_namespace_name, 'review')

    @pytest.fixture
    def review_category_assigned_to_review_src(self, review_source):
        return factories.create_source_category(review_source.id, 'review')

    @pytest.fixture
    def deal_source(self, yelp_namespace_name):
        return factories.create_source(yelp_namespace_name, 'deal')

    @pytest.fixture
    def biz_category_assigned_to_deal_src(self, deal_source):
        return factories.create_source_category(deal_source.id, 'biz')

    def test_happy_case(
        self,
        mock_request,
        biz_category_assigned_to_biz_src,
        review_category_assigned_to_review_src,
        biz_category_assigned_to_deal_src
    ):
        actual = categories.list_categories(mock_request)
        expected = ['biz', 'review']
        assert sorted(actual) == expected
