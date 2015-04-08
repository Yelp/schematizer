# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config

from schematizer.logic import schema_repository
from schematizer.utils.decorators import transform_response
from schematizer.views import constants


@view_config(
    route_name='api.get_schema_by_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_schema_by_id(request):
    schema_id = request.matchdict.get('schema_id')
    schema = schema_repository.get_schema_by_id(int(schema_id))
    if schema is None:
        raise exception_response(
            404,
            detail=constants.SCHEMA_NOT_FOUND_ERROR_MESSAGE
        )
    return schema.to_dict()


@view_config(
    route_name='api.register_avro_schema',
    request_method='POST',
    renderer='json'
)
@transform_response()
def register_avro_schema(request):
    # TODO(sichang|DATAPIPE-97)
    # This is blocked by DATAPIPE-45.
    raise exception_response(
        501,
        detail=constants.NOT_IMPLEMENTED_ERROR_MESSAGE
    )


@view_config(
    route_name='api.register_avro_schema_from_mysql_statements',
    request_method='POST',
    renderer='json'
)
@transform_response()
def register_avro_schema_from_mysql_statements(request):
    # TODO(sichang|DATAPIPE-97)
    # This is blocked by DATAPIPE-45.
    raise exception_response(
        501,
        detail=constants.NOT_IMPLEMENTED_ERROR_MESSAGE
    )
