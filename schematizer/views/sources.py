# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import doc_tool
from schematizer.logic import schema_repository


@view_config(
    route_name='api.v1.list_sources',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_sources(request):
    sources = schema_repository.get_sources()
    return [source.to_dict() for source in sources]


@view_config(
    route_name='api.v1.get_source_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_source_by_id(request):
    source_id = request.matchdict.get('source_id')
    source = schema_repository.get_source_by_id(int(source_id))
    if not source:
        raise exceptions_v1.source_not_found_exception()
    return responses_v1.get_source_response_from_source(source)


@view_config(
    route_name='api.v1.list_topics_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
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
@transform_api_response()
def get_latest_topic_by_source_id(request):
    source_id = int(request.matchdict.get('source_id'))
    latest_topic = schema_repository.get_latest_topic_of_source_id(source_id)
    if not latest_topic:
        source = schema_repository.get_source_by_id(source_id)
        if not source:
            raise exceptions_v1.source_not_found_exception()
        raise exceptions_v1.latest_topic_not_found_exception()
    return latest_topic.to_dict()


@view_config(
    route_name='api.v1.update_category',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def update_category(request):
    source_id = int(request.matchdict.get('source_id'))
    source = schema_repository.get_source_by_id(int(source_id))
    if not source:
        raise exceptions_v1.source_not_found_exception()
    req = requests_v1.UpdateCategoryRequest(**request.json_body)
    source_category = doc_tool.get_source_category_by_source_id(source_id)
    if source_category is None:
        source_category = doc_tool.create_source_category(
            source_id,
            req.category
        )
    else:
        doc_tool.update_source_category(source_id, req.category)
    return responses_v1.get_category_response_from_source_category(
        source_category
    )


@view_config(
    route_name='api.v1.delete_category',
    request_method='DELETE',
    renderer='json'
)
@transform_api_response()
def delete_category(request):
    source_id = int(request.matchdict.get('source_id'))
    source = schema_repository.get_source_by_id(int(source_id))
    if not source:
        raise exceptions_v1.source_not_found_exception()
    source_category = doc_tool.get_source_category_by_source_id(source_id)
    if source_category is None:
        raise exceptions_v1.category_not_found_exception()
    doc_tool.delete_source_category_by_source_id(source_id)
    return responses_v1.get_category_response_from_source_category(
        source_category
    )
