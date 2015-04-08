# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config

from schematizer.logic import schema_repository
from schematizer.utils.decorators import transform_response
from schematizer.views import constants


@view_config(
    route_name='api.get_topic_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_topic_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    topic = schema_repository.get_topic_by_name(topic_name)
    if topic is None:
        raise exception_response(
            404,
            detail=constants.TOPIC_NOT_FOUND_ERROR_MESSAGE
        )
    return topic.to_dict()


@view_config(
    route_name='api.list_schemas_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_response()
def list_schemas_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    schemas = schema_repository.get_schemas_by_topic_name(topic_name)
    if len(schemas) == 0:
        topic = schema_repository.get_topic_by_name(topic_name)
        if topic is None:
            raise exception_response(
                404,
                detail=constants.TOPIC_NOT_FOUND_ERROR_MESSAGE
            )
    return [schema.to_dict() for schema in schemas]


@view_config(
    route_name='api.get_latest_schema_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_latest_schema_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    schema = schema_repository.get_latest_schema_by_topic_name(topic_name)
    if schema is None:
        topic = schema_repository.get_topic_by_name(topic_name)
        if topic is None:
            raise exception_response(
                404,
                detail=constants.TOPIC_NOT_FOUND_ERROR_MESSAGE
            )

        raise exception_response(
            404,
            detail=constants.LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE
        )
    return schema.to_dict()
