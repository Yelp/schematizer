# -*- coding: utf-8 -*-
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.logic import schema_repository


@view_config(
    route_name='api.v1.list_sources',
    request_method='GET',
    renderer='json'
)
@transform_response()
def list_sources(request):
    sources = schema_repository.get_sources()
    return [source.to_dict() for source in sources]


@view_config(
    route_name='api.v1.get_source_by_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_source_by_id(request):
    source_id = request.matchdict.get('source_id')
    source = schema_repository.get_source_by_id(int(source_id))
    if not source:
        raise exceptions_v1.source_not_found_exception()
    return source.to_dict()


@view_config(
    route_name='api.v1.list_topics_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def list_topics_by_source_id(request):
    source_id = int(request.matchdict.get('source_id'))
    topics = schema_repository.get_topics_by_source_id(source_id)
    if not topics and not schema_repository.get_source_by_id(source_id):
        raise exceptions_v1.source_not_found_exception()
    return [topic.to_dict() for topic in topics]


@view_config(
    route_name='api.v1.get_latest_topic_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_latest_topic_by_source_id(request):
    source_id = int(request.matchdict.get('source_id'))
    latest_topic = schema_repository.get_latest_topic_of_source_id(source_id)
    if not latest_topic:
        source = schema_repository.get_source_by_id(source_id)
        if not source:
            raise exceptions_v1.source_not_found_exception()
        raise exceptions_v1.latest_topic_not_found_exception()
    return latest_topic.to_dict()
