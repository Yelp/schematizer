# -*- coding: utf-8 -*-
import copy
import pytest
from datetime import datetime

from mock import Mock
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPServerError
from sqlalchemy.orm.exc import NoResultFound

from schematizer.api.decorators import transform_response
from schematizer.api.decorators import handle_view_exception
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


namespace = factories.NamespaceFactory.create(factories.fake_namespace)

source_response = factories.SourceFactory.create(
    factories.fake_source,
    namespace,
    created_at=current_time,
    updated_at=current_time
).to_dict()


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


@transform_response()
def _view_mock_return_list_of_sources(request):
    return list_of_source_response


@transform_response()
def _view_mock_return_source(request):
    return source_response


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

    def test_transform_response_with_list_of_objects(self):
        request_mock = Mock()
        expected_response = copy.deepcopy(list_of_source_response)
        response = _view_mock_return_list_of_sources(request_mock)
        for source, expected_source in zip(response, expected_response):
            self.assert_source_response(source, expected_source)

    def test_transform_response_with_single_object(self):
        request_mock = Mock()
        expected_response = copy.deepcopy(source_response)
        response = _view_mock_return_source(request_mock)
        self.assert_source_response(response, expected_response)

    def assert_source_response(self, source, expected_source):
        assert source['source_id'] == expected_source['source_id']
        assert source['namespace'] == expected_source['namespace']
        assert source['source'] == expected_source['source']
        assert source['source_owner_email'] == expected_source[
            'source_owner_email'
        ]
        assert source['created_at'] == current_time.isoformat()
        assert source['updated_at'] == current_time.isoformat()
