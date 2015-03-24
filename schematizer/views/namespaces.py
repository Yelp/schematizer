# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config

from schematizer import models
from schematizer.utils.decorators import handle_view_exception


@view_config(
    route_name='api.list_namespaces',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
def list_namespaces(request):
    namespaces = models.domain.list_all_namespaces()
    return namespaces


@view_config(
    route_name='api.list_sources_by_namespace',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
def list_sources_by_namespace(request):
    namespace = request.matchdict.get('namespace')
    sources = models.domain.list_sources_by_namespace(namespace)
    if len(sources) == 0:
        raise exception_response(404, detail="Namespace is not found.")
    return [source.to_dict() for source in sources]
