# -*- coding: utf-8 -*-
from pyramid.httpexceptions import exception_response
from pyramid.view import view_config
from sqlalchemy.orm.exc import NoResultFound

from schematizer import models
from schematizer.utils.decorators import handle_view_exception


@view_config(
    route_name='api.get_latest_schema_by_namespace_and_source',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
@handle_view_exception(
    NoResultFound,
    404,
    "Namespace and source is not found."
)
def get_latest_schema_by_namespace_and_source(request):
    namespace = request.params['namespace']
    source_name = request.params['source']
    source = models.domain.get_source_by_namespace_and_source_name(
        namespace,
        source_name
    )
    latest_topic = models.topic.get_latest_topic_by_source_id(source.id)
    if latest_topic is None:
        raise exception_response(404, detail="Latest topics is not found.")

    latest_schema = models.avro_schema.get_latest_schema_by_topic_id(latest_topic.id)
    if latest_schema is None:
        raise exception_response(404, detail="Latest schema is not found.")
    return latest_schema.to_dict()


@view_config(
    route_name='api.get_schema_by_id',
    request_method='GET',
    renderer='json'
)
@handle_view_exception(Exception, 500, None)
@handle_view_exception(NoResultFound, 404, "Schema is not found.")
def get_schema_by_id(request):
    schema_id = request.matchdict.get('schema_id')
    schema = models.avro_schema.get_schema_by_schema_id(int(schema_id))
    return schema.to_dict()


@view_config(
    route_name='api.register_avro_schema',
    request_method='POST',
    renderer='json'
)
def register_avro_schema(request):
    # TODO: DATAPIPE-97.
    # This is blocked by DATAPIPE-45.
    raise exception_response(501, detail="Not implemented.")


@view_config(
    route_name='api.register_avro_schema_from_mysql_statements',
    request_method='POST',
    renderer='json'
)
def register_avro_schema_from_mysql_statements(request):
    # TODO: DATAPIPE-97.
    # This is blocked by DATAPIPE-45.
    raise exception_response(501, detail="Not implemented.")
