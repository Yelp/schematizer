# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import namespaces as namespace_views
from schematizer.views.namespaces import list_namespaces
from tests.views.api_test_base import TestApiBase


class TestNamespaceViewBase(TestApiBase):

    test_view_module = 'schematizer.views.namespaces'


class TestListSourcesByNamespace(TestNamespaceViewBase):

    def test_non_existing_namespace(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_namespace_by_name.return_value = None
            mock_request.matchdict = self.get_mock_dict({'namespace': 'foo'})
            namespace_views.list_sources_by_namespace(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exceptions_v1.NAMESPACE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_namespace_by_name.assert_called_once_with('foo')

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_sources_by_namespace.return_value = self.sources
        mock_request.matchdict = self.get_mock_dict({'namespace': 'yelp'})

        sources = namespace_views.list_sources_by_namespace(mock_request)

        assert self.sources_response == sources
        mock_repo.get_sources_by_namespace.assert_called_once_with('yelp')


class TestListNamespaces(TestNamespaceViewBase):

    def test_no_namespaces(self, mock_request, mock_repo):
        mock_repo.get_namespaces.return_value = []
        actual = list_namespaces(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_namespaces.return_value = self.namespaces_response
        actual = list_namespaces(mock_request)
        assert self.namespaces == actual
