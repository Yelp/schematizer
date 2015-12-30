# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import refreshes as refresh_views
from tests.views.api_test_base import ApiTestBase


class TestGetRefreshByID(ApiTestBase):

    def test_happy_case(self, mock_request, biz_src_refresh):
        mock_request.matchdict = {'refresh_id': biz_src_refresh.id}
        actual = refresh_views.get_refresh_by_id(mock_request)
        expected = self.get_expected_src_refresh_resp(biz_src_refresh.id)
        assert actual == expected

    def test_non_existing_topic_name(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'refresh_id': '0'}
            refresh_views.get_refresh_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.REFRESH_NOT_FOUND_ERROR_MESSAGE


class TestUpdateRefresh(ApiTestBase):

    @property
    def update_request(self):
        return {
            'status': 'IN_PROGRESS',
            'offset': 100
        }

    def test_update_refresh(self, mock_request, biz_src_refresh):
        mock_request.json_body = self.update_request
        mock_request.matchdict = {'refresh_id': str(biz_src_refresh.id)}
        actual = refresh_views.update_refresh(mock_request)

        expected = self.get_expected_src_refresh_resp(
            biz_src_refresh.id,
            status='IN_PROGRESS',
            offset=100
        )
        assert actual == expected

    def test_non_existing_refresh_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'refresh_id': '0'}
            mock_request.json_body = self.update_request
            refresh_views.update_refresh(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.REFRESH_NOT_FOUND_ERROR_MESSAGE


class TestGetRefreshesByCriteria(ApiTestBase):

    def test_non_existing_namespace(self, mock_request):
        mock_request.params = {'namespace': 'missing'}
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        assert actual == []

    def test_no_matching_refreshes(self, mock_request, yelp_namespace):
        mock_request.params = {
            'namespace': yelp_namespace.name,
            'status': 'FAILED'
        }
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        assert actual == []

    def test_filter_by_namespace(
        self,
        mock_request,
        yelp_namespace,
        biz_src_refresh
    ):
        mock_request.params = {
            'namespace': yelp_namespace.name,
            'status': 'NOT_STARTED'
        }
        actual = refresh_views.get_refreshes_by_criteria(mock_request)
        expected = [self.get_expected_src_refresh_resp(biz_src_refresh.id)]
        assert actual == expected
