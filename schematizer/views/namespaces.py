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

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests.requests_v1 import get_pagination_info
from schematizer.api.responses import responses_v1
from schematizer.logic import schema_repository
from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.namespace import Namespace


@view_config(
    route_name='api.v1.list_namespaces',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_namespaces(request):
    namespaces = Namespace.get_all()
    return [responses_v1.get_namespace_response_from_namespace(namespace)
            for namespace in namespaces]


@view_config(
    route_name='api.v1.list_sources_by_namespace',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_sources_by_namespace(request):
    namespace_name = request.matchdict.get('namespace')
    page_info = get_pagination_info(request.params)
    try:
        sources = Namespace.get_by_name(namespace_name).get_sources(page_info)
        return [responses_v1.get_source_response_from_source(source)
                for source in sources]
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.list_refreshes_by_namespace',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_refreshes_by_namespace(request):
    namespace_name = request.matchdict.get('namespace')
    try:
        sources = Namespace.get_by_name(namespace_name).sources
    except EntityNotFoundError as e:
        raise exceptions_v1.entity_not_found_exception(e.message)

    refreshes = []
    for source in sources:
        refreshes += schema_repository.list_refreshes_by_source_id(source.id)
    return [responses_v1.get_refresh_response_from_refresh(refresh)
            for refresh in refreshes]
