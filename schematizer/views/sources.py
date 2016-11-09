# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import doc_tool
from schematizer.logic import schema_repository
from schematizer.models.source import Source


@view_config(
    route_name='api.v1.list_sources',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_sources(request):
    req = requests_v1.GetSourcesRequest(request.params)
    return [responses_v1.get_source_response_from_source(src)
            for src in Source.get_all(req.page_info)]


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
    return [responses_v1.get_topic_response_from_topic(t) for t in topics]


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
    return responses_v1.get_topic_response_from_topic(latest_topic)


@view_config(
    route_name='api.v1.update_category',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
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


@view_config(
    route_name='api.v1.create_refresh',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def create_refresh(request):
    source_id = int(request.matchdict.get('source_id'))
    source = schema_repository.get_source_by_id(int(source_id))
    if not source:
        raise exceptions_v1.source_not_found_exception()
    req = requests_v1.CreateRefreshRequest(**request.json_body)
    refresh = schema_repository.create_refresh(
        source_id=source_id,
        offset=req.offset,
        batch_size=req.batch_size,
        priority=req.priority,
        filter_condition=req.filter_condition,
        avg_rows_per_second_cap=req.avg_rows_per_second_cap
    )
    return responses_v1.get_refresh_response_from_refresh(refresh)


@view_config(
    route_name='api.v1.list_refreshes_by_source_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_refreshes_by_source_id(request):
    source_id = int(request.matchdict.get('source_id'))
    source = schema_repository.get_source_by_id(source_id)
    if not source:
        raise exceptions_v1.source_not_found_exception()
    refresh_history = schema_repository.list_refreshes_by_source_id(
        source_id
    )
    return [responses_v1.get_refresh_response_from_refresh(refresh)
            for refresh in refresh_history]
