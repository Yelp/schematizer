# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import refreshes as refresh_views
from tests.views.api_test_base import TestApiBase


class TestGetRefreshByID(TestApiBase):

    def test_happy_case(self, mock_request, refresh, refresh_response):
        mock_request.matchdict = self.get_mock_dict({
            'refresh_id': refresh.id
        })
        actual = refresh_views.get_refresh_by_id(mock_request)
        assert actual == refresh_response

    def test_non_existing_topic_name(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'refresh_id': 0})
            refresh_views.get_refresh_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.REFRESH_NOT_FOUND_ERROR_MESSAGE


class TestUpdateRefresh(TestApiBase):

    def test_update_refresh(
            self,
            mock_request,
            refresh,
            update_refresh_request,
            updated_refresh_response
    ):
        mock_request.json_body = update_refresh_request
        mock_request.matchdict = self.get_mock_dict({'refresh_id': refresh.id})
        actual = refresh_views.update_refresh(mock_request)
        assert actual == updated_refresh_response

    def test_non_existing_refresh_id(
            self,
            mock_request,
            update_refresh_request
    ):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'refresh_id': 0})
            mock_request.json_body = update_refresh_request
            refresh_views.update_refresh(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exceptions_v1.REFRESH_NOT_FOUND_ERROR_MESSAGE


class TestGetRefreshesByCriteria(TestApiBase):

    def test_non_existing_namespace(self, mock_request):
        mock_request.params = self.get_mock_dict({'namespace': 'missing'})
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        assert actual == []

    def test_no_matching_refreshes(self, mock_request, yelp_namespace):
        mock_request.params = self.get_mock_dict(
            {
                'namespace': yelp_namespace.name,
                'status': 'FAILED'
            }
        )
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        assert actual == []

    def test_filter_by_namespace(
            self,
            mock_request,
            yelp_namespace,
            refresh_response_list
    ):
        mock_request.params = self.get_mock_dict(
            {
                'namespace': yelp_namespace.name,
                'status': 'NOT_STARTED'
            }
        )
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        assert actual == refresh_response_list
