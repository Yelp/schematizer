# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import namespaces as namespace_views
from tests.views.api_test_base import ApiTestBase


class TestListSourcesByNamespace(ApiTestBase):

    def test_non_existing_namespace(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'namespace': 'foo'})
            namespace_views.list_sources_by_namespace(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.NAMESPACE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_source, biz_source_response):
        mock_request.matchdict = self.get_mock_dict(
            {'namespace': biz_source.namespace.name}
        )
        actual = namespace_views.list_sources_by_namespace(mock_request)
        assert actual == [biz_source_response]


class TestListNamespaces(ApiTestBase):

    def test_no_namespaces(self, mock_request):
        actual = namespace_views.list_namespaces(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, yelp_namespace_response):
        actual = namespace_views.list_namespaces(mock_request)
        assert actual == [yelp_namespace_response]


class TestListRefreshesByNamespace(ApiTestBase):

    def test_non_existing_namespace(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'namespace': 'foo'})
            namespace_views.list_refreshes_by_namespace(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.NAMESPACE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, yelp_namespace,
                        biz_src_refresh_response):
        mock_request.matchdict = self.get_mock_dict(
            {'namespace': yelp_namespace.name}
        )
        actual = namespace_views.list_refreshes_by_namespace(mock_request)
        assert actual == [biz_src_refresh_response]
