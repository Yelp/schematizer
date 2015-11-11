# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.api.exceptions import exceptions_v1
from schematizer.logic import schema_repository


@view_config(
    route_name='api.v1.get_refresh_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_refresh_by_id(request):
    refresh_id = request.matchdict.get('refresh_id')
    refresh = schema_repository.get_refresh_by_id(int(refresh_id))
    if refresh is None:
        raise exceptions_v1.refresh_not_found_exception()
    return responses_v1.get_refresh_response_from_refresh(refresh)


@view_config(
    route_name='api.v1.update_refresh',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
def update_refresh(request):
    refresh_id = request.matchdict.get('refresh_id')
    req = requests_v1.UpdateRefreshStatusRequest(**request.json_body)
    refresh = schema_repository.get_refresh_by_id(int(refresh_id))
    if refresh is None:
        raise exceptions_v1.refresh_not_found_exception()
    schema_repository.update_refresh(
        refresh_id=refresh_id,
        status=req.status,
        offset=req.offset
    )
    return responses_v1.get_refresh_response_from_refresh(refresh)

@view_config(
    route_name='api.v1.get_refreshes_by_criteria',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_refreshes_by_criteria(request):
    criteria = requests_v1.GetRefreshesRequest(request.params)

    refreshes = schema_repository.get_refreshes_by_criteria(
        namespace=criteria.namespace,
        status=criteria.status,
        created_after=criteria.created_after_datetime
    )
    return [responses_v1.get_refresh_response_from_refresh(refresh)
            for refresh in refreshes]
