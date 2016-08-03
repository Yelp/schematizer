# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from pyramid.view import view_config

from schematizer import models
from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1 as exc_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1 as resp_v1
from schematizer.logic import registration_repository as reg_repo
from schematizer.models import exceptions as sch_exc


@view_config(
    route_name='api.v1.get_consumer_groups',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_consumer_groups(request):
    return [resp_v1.get_consumer_group_response_from_consumer_group(group)
            for group in models.ConsumerGroup.get_all()]


@view_config(
    route_name='api.v1.get_consumer_group_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_consumer_group_by_id(request):
    consumer_group_id = int(request.matchdict.get('consumer_group_id'))
    try:
        group = models.ConsumerGroup.get_by_id(consumer_group_id)
        return resp_v1.get_consumer_group_response_from_consumer_group(group)
    except sch_exc.EntityNotFoundError as e:
        raise exc_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.get_data_sources_by_consumer_group_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_data_sources_by_consumer_group_id(request):
    consumer_group_id = int(request.matchdict.get('consumer_group_id'))
    try:
        data_sources = reg_repo.get_data_sources_by_consumer_group_id(
            consumer_group_id
        )
        return [
            resp_v1.get_response_from_consumer_group_data_source(src)
            for src in data_sources
        ]
    except sch_exc.EntityNotFoundError as e:
        raise exc_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.create_consumer_group_data_source',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def create_consumer_group_data_source(request):
    consumer_group_id = int(request.matchdict.get('consumer_group_id'))
    req = requests_v1.CreateConsumerGroupDataSourceRequest(**request.json_body)
    try:
        data_src = reg_repo.register_consumer_group_data_source(
            consumer_group_id=consumer_group_id,
            data_source_type=req.data_source_type,
            data_source_id=req.data_source_id
        )
        return resp_v1.get_response_from_consumer_group_data_source(
            data_src
        )
    except ValueError as value_ex:
        raise exc_v1.invalid_request_exception(value_ex.message)
    except sch_exc.EntityNotFoundError as not_found_ex:
        raise exc_v1.entity_not_found_exception(not_found_ex.message)
