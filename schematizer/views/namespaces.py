# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import schema_repository


@view_config(
    route_name='api.v1.list_namespaces',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_namespaces(request):
    namespaces = schema_repository.get_namespaces()
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
    namespace = schema_repository.get_namespace_by_name(namespace_name)
    if namespace is None:
        raise exceptions_v1.namespace_not_found_exception()
    sources = schema_repository.get_sources_by_namespace(namespace_name)
    return [responses_v1.get_source_response_from_source(source)
            for source in sources]


@view_config(
    route_name='api.v1.list_refreshes_by_namespace',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_refreshes_by_namespace(request):
    namespace_name = request.matchdict.get('namespace')
    namespace = schema_repository.get_namespace_by_name(namespace_name)
    if namespace is None:
        raise exceptions_v1.namespace_not_found_exception()
    sources = schema_repository.get_sources_by_namespace(namespace_name)
    source_ids = [source.id for source in sources]
    refreshes = []
    for source_id in source_ids:
        refreshes += schema_repository.list_refreshes_by_source_id(source_id)
    return [responses_v1.get_refresh_response_from_refresh(refresh)
            for refresh in refreshes]
