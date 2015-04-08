# -*- coding: utf-8 -*-
from datetime import datetime

from pyramid.httpexceptions import HTTPException
from pyramid.httpexceptions import exception_response


def handle_view_exception(exception, status_code, error_message=None):
    def handle_view_exception_decorator(func):
        def handle_exception(request):
            try:
                return func(request)
            except exception as e:
                if not isinstance(e, HTTPException):
                    raise exception_response(
                        status_code,
                        detail=error_message or repr(e)
                    )
                else:
                    raise e
        return handle_exception
    return handle_view_exception_decorator


def transform_response():
    def serialize_response_decorator(func):
        def serialize_response_func(request):
            response = func(request)
            if isinstance(response, list):
                for item in response:
                    _transform_datetime_field(item)
            else:
                _transform_datetime_field(response)
            return response
        return serialize_response_func
    return serialize_response_decorator


def _transform_datetime_field(response):
    if isinstance(response, dict):
        for key, value in response.iteritems():
            if isinstance(value, datetime):
                response[key] = value.isoformat()
            elif isinstance(value, dict):
                _transform_datetime_field(value)
