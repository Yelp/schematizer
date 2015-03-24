# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config
from sqlalchemy.orm.exc import NoResultFound

from schematizer import models
from schematizer.utils.decorators import handle_view_exception


@view_config(
    route_name='api.get_topic_by_topic_name',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
@handle_view_exception(NoResultFound, 404, "Topic is not found.")
def get_topic_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    topic = models.topic.get_topic_by_topic_name(topic_name)
    return topic.to_dict()


@view_config(
    route_name='api.list_schemas_by_topic_name',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
@handle_view_exception(NoResultFound, 404, "Topic_name is not found.")
def list_schemas_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    topic_id = models.topic.get_topic_by_topic_name(topic_name).id
    schemas = models.avro_schema.list_schemas_by_topic_id(topic_id)
    if len(schemas) == 0:
        raise exception_response(404, detail="Schemas is not found.")
    return [schema.to_dict() for schema in schemas]


@view_config(
    route_name='api.get_latest_schema_by_topic_name',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
@handle_view_exception(NoResultFound, 404, "Topic_name is not found.")
def get_latest_schema_by_topic_name(request):
    topic_name = request.matchdict.get('topic_name')
    topic_id = models.topic.get_topic_by_topic_name(topic_name).id
    schema = models.avro_schema.get_latest_schema_by_topic_id(topic_id)
    if schema is None:
        raise exception_response(404, detail="Latest schema is not found.")
    return schema.to_dict()
