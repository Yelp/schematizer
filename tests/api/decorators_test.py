# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

import pytest
from mock import call
from mock import Mock
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPServerError
from pyramid.request import Request
from pyramid.response import Response
from sqlalchemy.orm.exc import NoResultFound

from schematizer.api.decorators import handle_view_exception
from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from tests.models.testing_db import DBTestCase


class TestHandleViewExceptionDecorator(DBTestCase):

    def test_handle_view_unknown_exception(self):
        request_mock = Mock()
        random_exception = Exception()

        @handle_view_exception(Exception, 500)
        def _view_mock_raise_unknown_exception(request):
            raise random_exception

        with pytest.raises(HTTPServerError) as e:
            _view_mock_raise_unknown_exception(request_mock)
            assert e.code == 500
            assert str(e) == repr(random_exception)

    def test_handle_view_no_result_found_exception(self):
        request_mock = Mock()
        no_result_found_exception = NoResultFound()
        no_result_found_err_message = "Result not found."

        @handle_view_exception(Exception, 500)
        @handle_view_exception(NoResultFound, 404, no_result_found_err_message)
        def _view_mock_raise_no_result_found_exception(request):
            raise no_result_found_exception

        with pytest.raises(HTTPNotFound) as e:
            _view_mock_raise_no_result_found_exception(request_mock)
            assert e.code == 404
            assert str(e) == no_result_found_err_message


class TestTransformResponseDecorator(DBTestCase):

    def _format_time(self, time):
        return time.isoformat() + 'Z'

    @pytest.fixture
    def source_response(self, biz_source):
        return {
            'source_id': biz_source.id,
            'namespace': self._get_namespace_resp(biz_source.namespace),
            'name': biz_source.name,
            'owner_email': biz_source.owner_email,
            'created_at': self._format_time(biz_source.created_at),
            'updated_at': self._format_time(biz_source.updated_at)
        }

    def _get_namespace_resp(self, namespace):
        return {
            'namespace_id': namespace.id,
            'name': namespace.name,
            'created_at': self._format_time(namespace.created_at),
            'updated_at': self._format_time(namespace.updated_at)
        }

    @pytest.fixture
    def sources_response(self, source_response):
        return [source_response]

    def test_transform_api_response_of_object_list(self, sources_response):
        request_mock = Mock()
        expected_response = copy.deepcopy(sources_response)

        @transform_api_response()
        def _view_mock_return_list_of_sources(request):
            return sources_response

        actual_response = _view_mock_return_list_of_sources(request_mock)
        for actual, expected in zip(actual_response, expected_response):
            assert actual == expected

    def test_transform_api_response_of_single_object(self, source_response):
        request_mock = Mock()
        expected = copy.deepcopy(source_response)

        @transform_api_response()
        def _view_mock_return_source(request):
            return source_response

        actual = _view_mock_return_source(request_mock)
        assert actual == expected

    @pytest.mark.parametrize("data, expected", [
        ({'good': 1, 'bad': None}, {'good': 1}),
        (
            [{'good': 1, 'bad': None}, {'good': 2, 'bad': None}],
            [{'good': 1}, {'good': 2}]
        )
    ])
    def test_none_fields_removed_from_api_response(self, data, expected):
        @transform_api_response()
        def _mock_pass_request_as_response(pass_through_response):
            return pass_through_response

        actual = _mock_pass_request_as_response(data)
        assert actual == expected


class TestLogApiDecorator(object):

    @pytest.fixture
    def mock_request(self):
        return Mock(name='mock_request', spec=Request)

    @pytest.fixture
    def mock_response(self):
        return Mock(name='mock_response', spec=Response)

    @pytest.fixture
    def mock_log(self):
        return Mock(name='mock_log')

    def test_log_success_response(self, mock_request, mock_response, mock_log):

        @log_api(logger=mock_log)
        def api_with_success_response(request):
            return mock_response

        response = api_with_success_response(mock_request)
        assert response == mock_response
        assert mock_log.mock_calls == [
            call.debug("Received request: {}".format(mock_request))
        ]
