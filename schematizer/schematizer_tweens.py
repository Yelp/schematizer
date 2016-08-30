# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import sys
from collections import namedtuple

import six
from pyramid.interfaces import IExceptionResponse

import schematizer.models.database

ExceptionInfo = namedtuple('ExceptionInfo', 'type exception traceback')


class AbortResponse(Exception):
    def __init__(self, response, exc_info):
        self.response = response
        self.exc_info = exc_info


def db_session_tween_factory(handler, registry):
    """This python tween is used to manage sqlalchamy session
    for every web request made to pyramid. If the request was processed
    successfully this tween commits the session else it will rollback the
    session, finally it removes the session at the end of request.
    It also handles and reports appropriate request exceptions.
    """
    
    session = schematizer.models.database.session

    def commit_veto(request, response, exc_info):
        if response is None and IExceptionResponse.providedBy(
            exc_info.exception
        ):
            response = exc_info.exception

        if response is None and exc_info:
            six.reraise(exc_info.type, exc_info.exception, exc_info.traceback)

        return response.status.startswith(('4', '5'))

    def session_tween(request):
        response = None
        exc_info = None
        try:
            try:
                response = handler(request)
            except Exception as e:
                exc_info = ExceptionInfo(*sys.exc_info())
            if commit_veto(request, response, exc_info):
                raise AbortResponse(response, exc_info)
            session.commit()
            if exc_info:
                six.reraise(
                    exc_info.type, exc_info.exception, exc_info.traceback)
            return response
        except AbortResponse as e:
            session.rollback()
            if e.exc_info:
                six.reraise(
                    exc_info.type, exc_info.exception, exc_info.traceback)
            return e.response
        except:
            session.rollback()
            raise
        finally:
            session.remove()
    return session_tween
