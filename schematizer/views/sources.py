# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config
from sqlalchemy.orm.exc import NoResultFound

from schematizer import models
from schematizer.utils.decorators import handle_view_exception


@view_config(
    route_name='api.list_sources',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
def list_sources(request):
        domains = models.domain.list_all_sources()
        return [domain.to_dict() for domain in domains]


@view_config(
    route_name='api.get_source_by_id',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
@handle_view_exception(NoResultFound, 404, "Source is not found.")
def get_source_by_id(request):
    source_id = request.matchdict.get('source_id')
    source = models.domain.get_source_by_source_id(int(source_id))
    return source.to_dict()


@view_config(
    route_name='api.list_topics_by_source_id',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
def list_topics_by_source_id(request):
    source_id = request.matchdict.get('source_id')
    topics = models.topic.list_topics_by_source_id(int(source_id))
    if len(topics) == 0:
        raise exception_response(404, detail="Topics are not found")
    return [topic.to_dict() for topic in topics]


@view_config(
    route_name='api.get_latest_topic_by_source_id',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
def get_latest_topic_by_source_id(request):
    source_id = request.matchdict.get('source_id')
    latest_topic = models.topic.get_latest_topic_by_source_id(int(source_id))
    if latest_topic is None:
        raise exception_response(404, detail="Latest topic is not found.")
    return latest_topic.to_dict()
