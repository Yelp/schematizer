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
    route_name='api.v1.get_data_targets',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_data_targets(request):
    return [resp_v1.get_data_target_response_from_data_target(data_target)
            for data_target in models.DataTarget.get_all()]


@view_config(
    route_name='api.v1.create_data_target',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def create_data_target(request):
    try:
        req = requests_v1.CreateDataTargetRequest(**request.json_body)
        data_target = reg_repo.create_data_target(
            req.target_type,
            req.destination
        )
        return resp_v1.get_data_target_response_from_data_target(data_target)
    except ValueError as e:
        raise exc_v1.invalid_request_exception(e.message)


@view_config(
    route_name='api.v1.get_data_target_by_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_data_target_by_id(request):
    data_target_id = int(request.matchdict.get('data_target_id'))
    try:
        data_target = models.DataTarget.get_by_id(data_target_id)
        return resp_v1.get_data_target_response_from_data_target(data_target)
    except sch_exc.EntityNotFoundError as e:
        raise exc_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.get_consumer_groups_by_data_target_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_consumer_groups_by_data_target_id(request):
    data_target_id = request.matchdict.get('data_target_id')
    try:
        groups = reg_repo.get_consumer_groups_by_data_target_id(data_target_id)
        return [resp_v1.get_consumer_group_response_from_consumer_group(group)
                for group in groups]
    except sch_exc.EntityNotFoundError as e:
        raise exc_v1.entity_not_found_exception(e.message)


@view_config(
    route_name='api.v1.create_consumer_group',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def create_consumer_group(request):
    data_target_id = int(request.matchdict.get('data_target_id'))
    req = requests_v1.CreateConsumerGroupRequest(**request.json_body)
    try:
        group = reg_repo.create_consumer_group(req.group_name, data_target_id)
        return resp_v1.get_consumer_group_response_from_consumer_group(group)
    except ValueError as value_ex:
        raise exc_v1.invalid_request_exception(value_ex.message)
    except sch_exc.EntityNotFoundError as not_found_ex:
        raise exc_v1.entity_not_found_exception(not_found_ex.message)


@view_config(
    route_name='api.v1.get_topics_by_data_target_id',
    request_method='GET',
    renderer='json'
)
@transform_api_response()
def get_topics_by_data_target_id(request):
    data_target_id = int(request.matchdict.get('data_target_id'))
    req = requests_v1.GetTopicsByDataTargetIdRequest(request.params)
    try:
        topics = reg_repo.get_topics_by_data_target_id(
            data_target_id,
            created_after=req.created_after_datetime
        )
        return [resp_v1.get_topic_response_from_topic(t) for t in topics]
    except sch_exc.EntityNotFoundError as e:
        raise exc_v1.entity_not_found_exception(e.message)
