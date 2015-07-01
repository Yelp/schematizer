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
    namespace_name = request.matchdict.get('namespace')
    namespace = schema_repository.get_namespace_by_name(namespace_name)
    # Raise an exception if this namespace does not exist
    if namespace is None:
        raise exceptions_v1.namespace_not_found_exception()
    sources = schema_repository.get_sources_by_namespace(namespace_name)
    return [source.to_dict() for source in sources]
