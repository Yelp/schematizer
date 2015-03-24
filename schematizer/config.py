# -*- coding: utf-8 -*-
import logging


log = logging.getLogger('schematizer.config')


def routes(config):
    """Add routes to the configuration."""
    config.add_route(
        'api.list_namespaces',
        '/v1/namespaces'
    )
    config.add_route(
        'api.list_sources_by_namespace',
        '/v1/namespaces/{namespace}/sources'
    )
    config.add_route(
        'api.list_sources',
        '/v1/sources'
    )
    config.add_route(
        'api.get_source_by_id',
        '/v1/sources/{source_id}'
    )
    config.add_route(
        'api.list_topics_by_source_id',
        '/v1/sources/{source_id}/topics'
    )
    config.add_route(
        'api.get_latest_topic_by_source_id',
        '/v1/sources/{source_id}/topics/latest'
    )
    config.add_route(
        'api.get_topic_by_topic_name',
        '/v1/topics/{topic_name}'
    )
    config.add_route(
        'api.list_schemas_by_topic_name',
        '/v1/topics/{topic_name}/schemas'
    )
    config.add_route(
        'api.get_latest_schema_by_topic_name',
        '/v1/topics/{topic_name}/schemas/latest'
    )
    config.add_route(
        'api.get_latest_schema_by_namespace_and_source',
        '/v1/schemas/latest'
    )
    config.add_route(
        'api.register_avro_schema_from_mysql_statements',
        '/v1/schemas/mysql'
    )
    config.add_route(
        'api.register_avro_schema',
        '/v1/schemas/avro'
    )
    config.add_route(
        'api.get_schema_by_id',
        '/v1/schemas/{schema_id}'
    )
    config.add_route(
        'api.is_avro_schema_compatible',
        '/v1/compatibility/schemas/avro'
    )
    config.add_route(
        'api.is_mysql_schema_compatible',
        '/v1/compatibility/schemas/mysql'
    )
