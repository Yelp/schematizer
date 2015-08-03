# -*- coding: utf-8 -*-
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import schema_repository


@view_config(
    route_name='api.v1.get_topic_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_topic_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    topic = schema_repository.get_topic_by_name(topic_name)
    if topic is None:
        raise exceptions_v1.topic_not_found_exception()
    return responses_v1.get_topic_response_from_topic(topic)


@view_config(
    route_name='api.v1.list_schemas_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_response()
def list_schemas_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    schemas = schema_repository.get_schemas_by_topic_name(topic_name)
    if not schemas and not schema_repository.get_topic_by_name(topic_name):
        raise exceptions_v1.topic_not_found_exception()
    return [responses_v1.get_schema_response_from_avro_schema(avro_schema)
            for avro_schema in schemas]


@view_config(
    route_name='api.v1.get_latest_schema_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_latest_schema_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    avro_schema = schema_repository.get_latest_schema_by_topic_name(topic_name)
    if avro_schema is None:
        topic = schema_repository.get_topic_by_name(topic_name)
        if not topic:
            raise exceptions_v1.topic_not_found_exception()
        raise exceptions_v1.latest_schema_not_found_exception()
    return responses_v1.get_schema_response_from_avro_schema(avro_schema)
