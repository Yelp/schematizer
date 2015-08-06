# -*- coding: utf-8 -*-
from schematizer.views import categories as categories
from tests.views.api_test_base import TestApiBase


class TestCategoriesViewBase(TestApiBase):

    test_view_module = 'schematizer.views.categories'


class TestListCategories(TestCategoriesViewBase):

    def test_no_categories(self, mock_request, mock_doc_tool):
        mock_doc_tool.get_source_categories.return_value = []
        actual = categories.list_categories(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, mock_doc_tool):
        mock_doc_tool.get_source_categories.return_value = \
            self.source_categories_response
        actual = categories.list_categories(mock_request)
        assert self.categories == actual
