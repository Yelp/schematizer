# -*- coding: utf-8 -*-
import pytest

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import refresh as refresh_views
from tests.views.api_test_base import TestApiBase


class TestRefreshViewBase(TestApiBase):

    test_view_module = 'schematizer.views.refresh'


class TestCreateRefreshInfo(TestRefreshViewBase):

    def test_create_refresh_info(self, mock_request, mock_refresh):
        mock_request.json_body = self.create_refresh_info_request
        mock_refresh.create_refresh_info.return_value = self.refresh_info
        actual = refresh_views.create_refresh_info(mock_request)
        assert actual == self.refresh_response


class TestUpdateRefreshInfo(TestRefreshViewBase):

    def test_update_refresh_info(self, mock_request, mock_refresh):
        mock_request.json_body = self.update_refresh_info_request
        mock_request.matchdict = self.get_mock_dict(
            {
                'table_identifier': 'db_table'
            }
        )
        mock_refresh.get_refresh_info_by_table.return_value = self.refresh_info
        actual = refresh_views.update_refresh_info(mock_request)
        assert actual == self.refresh_response

    def test_update_non_existing_table(self, mock_request, mock_refresh):
        expected_exception = self.get_http_exception(404)
        err_message = exceptions_v1.REFRESH_INFO_NOT_FOUND_ERROR_MESSAGE
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = self.update_refresh_info_request
            mock_request.matchdict = self.get_mock_dict(
                {
                    'table_identifier': 'non_existing'
                }
            )
            mock_refresh.get_refresh_info_by_table.return_value = None
            refresh_views.update_refresh_info(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == err_message


class TestGetRefreshInfoByTable(TestRefreshViewBase):

    def test_get_refresh_info_by_table(self, mock_request, mock_refresh):
        mock_request.matchdict = self.get_mock_dict(
            {
                'table_identifier': 'db_table'
            }
        )
        mock_refresh.get_refresh_info_by_table.return_value = self.refresh_info
        actual = refresh_views.get_refresh_info_by_table(mock_request)
        assert actual == self.refresh_response
        mock_refresh.get_refresh_info_by_table.assert_called_once_with(
            'db_table'
        )

    def test_get_non_existing_refresh_info(self, mock_request, mock_refresh):
        expected_exception = self.get_http_exception(404)
        err_message = exceptions_v1.REFRESH_INFO_NOT_FOUND_ERROR_MESSAGE
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict(
                {
                    'table_identifier': 'non_existing'
                }
            )
            mock_refresh.get_refresh_info_by_table.return_value = None
            refresh_views.get_refresh_info_by_table(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == err_message
        mock_refresh.get_refresh_info_by_table.assert_called_once_with(
            'non_existing'
        )


class TestListIncompleteRefreshes(TestRefreshViewBase):

    def test_no_incomplete_refreshes(self, mock_request, mock_refresh):
        mock_refresh.list_incomplete_refreshes.return_value = []
        incomplete_refreshes = refresh_views.list_incomplete_refreshes(
            mock_request
        )
        assert incomplete_refreshes == []
        mock_refresh.list_incomplete_refreshes.assert_called_once_with()

    def test_list_incomplete_refreshes(self, mock_request, mock_refresh):
        mock_function = mock_refresh.list_incomplete_refreshes
        mock_function.return_value = self.refresh_info_list
        incomplete_refreshes = refresh_views.list_incomplete_refreshes(
            mock_request
        )
        assert self.refresh_response_list == incomplete_refreshes
        mock_refresh.list_incomplete_refreshes.assert_called_once_with()
