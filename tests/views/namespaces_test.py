# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.views import namespaces as namespace_views
from tests.views.api_test_base import ApiTestBase


class TestListSourcesByNamespace(ApiTestBase):

    def test_non_existing_namespace(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'namespace': 'foo'}
            mock_request.params = {}
            namespace_views.list_sources_by_namespace(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Namespace name `foo` not found."

    def test_happy_case(self, mock_request, yelp_namespace, biz_source):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        mock_request.params = {}
        actual = namespace_views.list_sources_by_namespace(mock_request)
        expected = [self.get_expected_src_resp(biz_source.id)]
        assert actual == expected

    def test_with_min_id(self, mock_request, yelp_namespace, biz_source):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        mock_request.params = {'min_id': biz_source.id + 1}
        actual = namespace_views.list_sources_by_namespace(mock_request)
        assert actual == []

    def test_with_count(
            self,
            mock_request,
            yelp_namespace,
            biz_source,
            another_biz_source
    ):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        mock_request.params = {'count': 1}
        actual = namespace_views.list_sources_by_namespace(mock_request)
        expected = [self.get_expected_src_resp(biz_source.id)]
        assert actual == expected


class TestListNamespaces(ApiTestBase):

    def test_no_namespaces(self, mock_request):
        actual = namespace_views.list_namespaces(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, yelp_namespace):
        actual = namespace_views.list_namespaces(mock_request)
        expected = [self.get_expected_namespace_resp(yelp_namespace.id)]
        assert actual == expected


class TestListRefreshesByNamespace(ApiTestBase):

    def test_non_existing_namespace(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'namespace': 'foo'}
            namespace_views.list_refreshes_by_namespace(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Namespace name `foo` not found."

    def test_happy_case(self, mock_request, yelp_namespace, biz_src_refresh):
        mock_request.matchdict = {'namespace': yelp_namespace.name}
        actual = namespace_views.list_refreshes_by_namespace(mock_request)
        expected = [self.get_expected_src_refresh_resp(biz_src_refresh.id)]
        assert actual == expected
