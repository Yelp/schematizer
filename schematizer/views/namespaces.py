# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.logic import schema_repository
from schematizer.views import constants


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
    sources = schema_repository.get_domains_by_namespace(namespace)
    # Since we store namespace and source in the same table, so
    # if there is no domain records, it means there is no namespace.
    # So we just throw namespace not found error.
    if len(sources) == 0:
        raise exception_response(
            404,
            detail=constants.NAMESPACE_NOT_FOUND_ERROR_MESSAGE
        )
    return [source.to_dict() for source in sources]
