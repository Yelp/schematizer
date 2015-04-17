# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.logic import schema_repository
from schematizer.views import constants


@view_config(
    route_name='api.v1.list_sources',
    request_method='GET',
    renderer='json'
)
@transform_response()
def list_sources(request):
    domains = schema_repository.get_domains()
    return [domain.to_dict() for domain in domains]


@view_config(
    route_name='api.v1.get_source_by_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_source_by_id(request):
    source_id = request.matchdict.get('source_id')
    source = schema_repository.get_domain_by_id(int(source_id))
    if not source:
        raise exception_response(
            404,
            detail=constants.SOURCE_NOT_FOUND_ERROR_MESSAGE
        )
    return source.to_dict()


@view_config(
    route_name='api.v1.list_topics_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def list_topics_by_source_id(request):
    source_id = request.matchdict.get('source_id')
    topics = schema_repository.get_topics_by_domain_id(int(source_id))
    if (len(topics) == 0 and
            not schema_repository.get_domain_by_id(source_id)):
        raise exception_response(
            404,
            detail=constants.SOURCE_NOT_FOUND_ERROR_MESSAGE
        )

    return [topic.to_dict() for topic in topics]


@view_config(
    route_name='api.v1.get_latest_topic_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_response()
def get_latest_topic_by_source_id(request):
    source_id = request.matchdict.get('source_id')
    latest_topic = schema_repository.get_latest_topic_of_domain_id(
        int(source_id)
    )
    if latest_topic is None:
        if schema_repository.get_domain_by_id(source_id) is None:
            raise exception_response(
                404,
                detail=constants.SOURCE_NOT_FOUND_ERROR_MESSAGE
            )

        raise exception_response(
            404,
            detail=constants.LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE
        )
    return latest_topic.to_dict()
