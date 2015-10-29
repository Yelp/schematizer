# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer.api.decorators import transform_api_response
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.api.exceptions import exceptions_v1
from schematizer.logic import refresher


@view_config(
    route_name='api.v1.create_refresh_info',
    request_method='POST',
    renderer='json'
)
def create_refresh_info(request):
    req = requests_v1.CreateRefreshInfoRequest(**request.json_body)
    refresh_info = refresher.create_refresh_info(
        table_identifier=req.table_identifier,
        refresh_status=req.refresh_status
    )
    return responses_v1.get_refresh_response_by_refresh_info(refresh_info)


@view_config(
    route_name='api.v1.update_refresh_info',
    request_method='POST',
    renderer='json'
)
def update_refresh_info(request):
    req = requests_v1.UpdateRefreshInfoRequest(**request.json_body)
    table_identifier = request.matchdict.get('table_identifier')
    refresh_info = refresher.get_refresh_info_by_table(table_identifier)
    if refresh_info is None:
        raise exceptions_v1.REFRESH_INFO_NOT_FOUND_ERROR_MESSAGE
    refresher.update_refresh_info(
        table_identifier=table_identifier,
        refresh_status=req.refresh_status
    )
    return responses_v1.get_refresh_response_by_refresh_info(refresh_info)


@view_config(
    route_name='api.v1.get_refresh_info_by_table',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_refresh_info_by_table(request):
    table_identifier = request.matchdict.get('table_identifier')
    refresh_info = refresher.get_refresh_info_by_table(table_identifier)
    if refresh_info is None:
        raise exceptions_v1.refresh_info_not_found_exception()
    return responses_v1.get_refresh_response_by_refresh_info(refresh_info)


@view_config(
    route_name='api.v1.list_incomplete_refreshes',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def list_incomplete_refreshes(request):
    return [responses_v1.get_refresh_response_by_refresh_info(refresh_info)
            for refresh_info in refresher.list_incomplete_refreshes()]
