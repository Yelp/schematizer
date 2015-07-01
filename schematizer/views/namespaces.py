# -*- coding: utf-8 -*-
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.logic import schema_repository


@view_config(
    route_name='api.v1.list_namespaces',
    request_method='GET',
    renderer='json'
)
def list_namespaces(request):
    namespaces = schema_repository.get_namespaces()
    return namespaces


@view_config(
    route_name='api.v1.list_sources_by_namespace',
    request_method='GET',
    renderer='json'
)
@transform_response()
def list_sources_by_namespace(request):
    namespace = request.matchdict.get('namespace')
    sources = schema_repository.get_sources_by_namespace(namespace)
    # Since we store namespace and source in the same table, so
    # if there is no domain records, it means there is no namespace.
    # So we just throw namespace not found error.
    if len(sources) == 0:
        raise exceptions_v1.namespace_not_found_exception()
    return [source.to_dict() for source in sources]
