# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
from datetime import datetime

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
from testing import factories


exception = Exception()
no_result_found_exception = NoResultFound()
no_result_found_error_message = "Result not found."


@handle_view_exception(Exception, 500)
def _view_mock_raise_unknown_exception(request):
    raise exception


@handle_view_exception(Exception, 500)
@handle_view_exception(NoResultFound, 404, no_result_found_error_message)
def _view_mock_raise_no_result_found_exception(request):
    raise no_result_found_exception


current_time = datetime.now()


namespace = factories.NamespaceFactory.create(
    factories.fake_namespace,
    created_at=current_time,
    updated_at=current_time
)
namespace.id = 1

source_response = factories.SourceFactory.create(
    factories.fake_source,
    namespace,
    created_at=current_time,
    updated_at=current_time
).to_dict()
source_response['source_id'] = 1


list_of_source_response = [
    factories.SourceFactory.create(
        factories.fake_source,
        namespace,
        created_at=current_time,
        updated_at=current_time
    ).to_dict(),
    factories.SourceFactory.create(
        factories.fake_source,
        namespace,
        created_at=current_time,
        updated_at=current_time
    ).to_dict()
]
list_of_source_response[0]['source_id'] = 2
list_of_source_response[1]['source_id'] = 3


@transform_api_response()
def _view_mock_return_list_of_sources(request):
    return list_of_source_response


@transform_api_response()
def _view_mock_return_source(request):
    return source_response


@transform_api_response()
def _mock_pass_request_as_response(pass_through_response):
    return pass_through_response


class TestDecorators(object):

    def test_handle_view_unknown_exception(self):
        request_mock = Mock()
        with pytest.raises(HTTPServerError) as e:
            _view_mock_raise_unknown_exception(request_mock)
            assert e.code == 500
            assert str(e) == repr(exception)

    def test_handle_view_no_result_found_exception(self):
        request_mock = Mock()
        with pytest.raises(HTTPNotFound) as e:
            _view_mock_raise_no_result_found_exception(request_mock)
            assert e.code == 404
            assert str(e) == no_result_found_error_message

    def test_transform_api_response_with_list_of_objects(self):
        request_mock = Mock()
        expected_response = copy.deepcopy(list_of_source_response)
        response = _view_mock_return_list_of_sources(request_mock)
        for source, expected_source in zip(response, expected_response):
            self.assert_source_response(source, expected_source)

    def test_transform_api_response_with_single_object(self):
        request_mock = Mock()
        expected_response = copy.deepcopy(source_response)
        response = _view_mock_return_source(request_mock)
        self.assert_source_response(response, expected_response)

    def test_transform_api_response_object_none_fields_removed(self):
        response = _mock_pass_request_as_response({'good': 1, 'bad': None})
        assert response == {'good': 1}

    def test_transform_api_response_list_of_objects_none_fields_removed(self):
        response = _mock_pass_request_as_response(
            [{'good': 1, 'bad': None}, {'good': 2, 'bad': None}]
        )
        assert response == [{'good': 1}, {'good': 2}]

    def assert_source_response(self, source, expected_source):
        assert source['source_id'] == expected_source['source_id']
        assert source['name'] == expected_source['name']
        assert source['owner_email'] == expected_source['owner_email']
        assert source['created_at'] == current_time.isoformat()
        assert source['updated_at'] == current_time.isoformat()
        self.assert_namespace_response(
            source['namespace'],
            expected_source['namespace']
        )

    def assert_namespace_response(self, namespace, expected_namespace):
        assert namespace['namespace_id'] == expected_namespace['namespace_id']
        assert namespace['name'] == expected_namespace['name']
        assert namespace['created_at'] == current_time.isoformat()
        assert namespace['updated_at'] == current_time.isoformat()


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
