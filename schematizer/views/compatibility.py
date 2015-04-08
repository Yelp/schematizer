# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config

from schematizer.views import constants


@view_config(
    route_name='api.is_avro_schema_compatible',
    request_method='POST',
    renderer='json'
)
def is_avro_schema_compatible(request):
    # TODO(sichang|DATAPIPE-97)
    # This is blocked by DATAPIPE-45.
    raise exception_response(
        501,
        detail=constants.NOT_IMPLEMENTED_ERROR_MESSAGE
    )


@view_config(
    route_name='api.is_mysql_schema_compatible',
    request_method='POST',
    renderer='json'
)
def is_mysql_schema_compatible(request):
    # TODO(sichang|DATAPIPE-97)
    # This is blocked by DATAPIPE-45.
    raise exception_response(
        501,
        detail=constants.NOT_IMPLEMENTED_ERROR_MESSAGE
    )
