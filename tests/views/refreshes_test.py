# -*- coding: utf-8 -*-
import pytest

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import refreshes as refresh_views
from tests.views.api_test_base import TestApiBase


class TestRefreshesViewBase(TestApiBase):

    test_view_module = 'schematizer.views.refreshes'


class TestGetRefreshByID(TestRefreshesViewBase):

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_refresh_by_id.return_value = self.refresh
        mock_request.matchdict = self.get_mock_dict({'refresh_id': '1'})

        refresh = refresh_views.get_refresh_by_id(mock_request)
        assert self.refresh_response == refresh
        mock_repo.get_refresh_by_id.assert_called_once_with(1)

    def test_non_existing_refresh_id(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_refresh_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'refresh_id': '0'})
            refresh_views.get_refresh_by_id(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exceptions_v1.REFRESH_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_refresh_by_id.assert_called_once_with(0)


class TestUpdateRefresh(TestRefreshesViewBase):

    def test_update_refresh(self, mock_request, mock_repo):
        mock_request.json_body = self.update_refresh_request
        mock_request.matchdict = self.get_mock_dict({'refresh_id': '0'})
        mock_repo.get_refresh_by_id.return_value = self.refresh
        actual = refresh_views.update_refresh(mock_request)
        assert actual == self.refresh_response

    def test_non_existing_refresh_id(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_refresh_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'refresh_id': '0'})
            mock_request.json_body = self.update_refresh_request
            refresh_views.update_refresh(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exceptions_v1.REFRESH_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_refresh_by_id.assert_called_once_with(0)
