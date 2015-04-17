# -*- coding: utf-8 -*-
import pytest
from pyramid.httpexceptions import HTTPNotFound

from schematizer.views import constants
from schematizer.views.namespaces import list_namespaces
from schematizer.views.namespaces import list_sources_by_namespace
from tests.views.api_test_base import TestApiBase


class TestNamespaceViewBase(TestApiBase):

    test_view_module = 'schematizer.views.namespaces'


class TestListSourcesByNamespace(TestNamespaceViewBase):

    test_view_module = 'schematizer.views.namespaces'

    def test_non_existing_namespace(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_repo.get_domains_by_namespace.return_value = []
            mock_request.matchdict = self.get_mock_dict({'namespace': 'foo'})
            list_sources_by_namespace(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.NAMESPACE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_domains_by_namespace.assert_called_once_with('foo')

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_domains_by_namespace.return_value = self.sources
        mock_request.matchdict = self.get_mock_dict({'namespace': 'yelp'})

        sources = list_sources_by_namespace(mock_request)

        assert self.sources_response == sources
        mock_repo.get_domains_by_namespace.assert_called_once_with('yelp')


class TestListNamespaces(TestNamespaceViewBase):

    def test_no_namespaces(self, mock_request, mock_repo):
        mock_repo.get_namespaces.return_value = []
        actual = list_namespaces(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_namespaces.return_value = self.namespaces
        actual = list_namespaces(mock_request)
        assert self.namespaces_response == actual
