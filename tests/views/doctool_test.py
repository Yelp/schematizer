# -*- coding: utf-8 -*-
import mock
import pytest

from schematizer.views.doctool import get_admin_user_id_from_request
from schematizer.views.doctool import get_admin_user_info
from schematizer.views.doctool import doctool_index


class TestDoctool(object):

    @pytest.fixture
    def stub_expected_value(self):
        return {'email': 'test@yelp.com'}

    @pytest.yield_fixture
    def mock_stub(self, stub_expected_value):
        with mock.patch(
            'schematizer.views.doctool.get_yelp_main_internalapi_stub'
        ) as mock_stub:
            mock_stub().get_admin_user_info = mock.Mock(
                return_value=mock.Mock(
                    return_value=stub_expected_value
                )
            )
            yield mock_stub

    @pytest.fixture
    def valid_admin_user_id(self):
        return 1

    @pytest.fixture
    def valid_request_mock(self, valid_admin_user_id):
        request_mock = mock.Mock()
        request_mock.headers = {'X-User-Id': valid_admin_user_id}
        return request_mock

    @pytest.fixture
    def invalid_request_mock(self):
        request_mock = mock.Mock()
        request_mock.headers = {}
        return request_mock

    def test_get_admin_user_id_from_request_correct_when_provided(
            self,
            valid_request_mock,
            valid_admin_user_id
    ):
        assert get_admin_user_id_from_request(
            request=valid_request_mock
        ) == valid_admin_user_id

    def test_get_admin_user_id_None_when_missing_header(
            self,
            invalid_request_mock
    ):
        assert get_admin_user_id_from_request(
            request=invalid_request_mock
        ) is None

    def test_get_admin_user_info_returns_empty_dict_when_no_id(self):
        assert get_admin_user_info(admin_user_id=None) == {}

    def test_get_admin_user_info_returns_dict_when_id(
            self,
            mock_stub,
            stub_expected_value,
            valid_admin_user_id
    ):
        assert get_admin_user_info(
            admin_user_id=valid_admin_user_id
        ) == stub_expected_value
        assert mock_stub().get_admin_user_info.mock_calls == [
            mock.call(admin_user_id=valid_admin_user_id)
        ]

    def test_doctool_index_with_invalid_request_header(
            self,
            mock_stub,
            invalid_request_mock
    ):
        assert doctool_index(invalid_request_mock) == {'user_email': None}
        assert mock_stub().get_admin_user_info.mock_calls == []

    def test_doctool_index_with_valid_request_header(
            self,
            mock_stub,
            stub_expected_value,
            valid_admin_user_id,
            valid_request_mock
    ):
        assert doctool_index(valid_request_mock) == {
            'user_email': stub_expected_value['email']
        }
        assert mock_stub().get_admin_user_info.mock_calls == [
            mock.call(admin_user_id=valid_admin_user_id)
        ]
