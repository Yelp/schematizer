# -*- coding: utf-8 -*-
from pyramid.httpexceptions import HTTPException
from pyramid.httpexceptions import exception_response


def handle_view_exception(exception, status_code, error_message):
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
