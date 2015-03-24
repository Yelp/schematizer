# -*- coding: utf-8 -*-
import unittest
from mock import Mock
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPServerError
from sqlalchemy.orm.exc import NoResultFound

from schematizer.utils.decorators import handle_view_exception


@handle_view_exception(Exception, 500, None)
def _view_mock_raise_unknow_exception(request):
    raise Exception()


@handle_view_exception(Exception, 500, None)
@handle_view_exception(NoResultFound, 404, "Result not found.")
def _view_mock_raise_no_result_found_exception(request):
    raise NoResultFound()


class TestDecorators(unittest.TestCase):

    def test_handle_view_unknown_exception(self):
        request_mock = Mock()
        self.assertRaises(
            HTTPServerError,
            _view_mock_raise_unknow_exception,
            request_mock
        )

    def test_handle_view_no_result_found_exception(self):
        request_mock = Mock()
        self.assertRaises(
            HTTPNotFound,
            _view_mock_raise_no_result_found_exception,
            request_mock
        )
