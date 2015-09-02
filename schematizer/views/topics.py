# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import schema_repository


@view_config(
    route_name='api.v1.get_topic_by_topic_name',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
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
@transform_api_response()
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
@transform_api_response()
def get_latest_schema_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    avro_schema = schema_repository.get_latest_schema_by_topic_name(topic_name)
    if avro_schema is None:
        topic = schema_repository.get_topic_by_name(topic_name)
        if not topic:
            raise exceptions_v1.topic_not_found_exception()
        raise exceptions_v1.latest_schema_not_found_exception()
    return responses_v1.get_schema_response_from_avro_schema(avro_schema)


@view_config(
    route_name='api.v1.get_topics_by_criteria',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_topics_by_criteria(request):
    criteria = requests_v1.GetTopicsRequest(request.params)

    if (criteria.namespace and
            not schema_repository.get_namespace_by_name(criteria.namespace)):
        raise exceptions_v1.namespace_not_found_exception()

    if (criteria.namespace and criteria.source and
            not schema_repository.get_source_by_fullname(
                criteria.namespace,
                criteria.source)):
        raise exceptions_v1.source_not_found_exception()

    topics = schema_repository.get_topics_by_criteria(
        namespace=criteria.namespace,
        source=criteria.source,
        created_after=criteria.created_after_datetime
    )
    return [responses_v1.get_topic_response_from_topic(topic)
            for topic in topics]
