# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import logging


log = logging.getLogger('schematizer')


def routes(config):
    """Add routes to the configuration."""
    config.add_route(
        'api.v1.list_namespaces',
        '/v1/namespaces'
    )
    config.add_route(
        'api.v1.list_sources_by_namespace',
        '/v1/namespaces/{namespace}/sources'
    )
    config.add_route(
        'api.v1.list_refreshes_by_namespace',
        '/v1/namespaces/{namespace}/refreshes'
    )
    config.add_route(
        'api.v1.list_sources',
        '/v1/sources'
    )
    config.add_route(
        'api.v1.get_source_by_id',
        '/v1/sources/{source_id}'
    )
    config.add_route(
        'api.v1.update_category',
        '/v1/sources/{source_id}/category',
        request_method="POST"
    )
    config.add_route(
        'api.v1.delete_category',
        '/v1/sources/{source_id}/category',
        request_method="DELETE"
    )
    config.add_route(
        'api.v1.list_topics_by_source_id',
        '/v1/sources/{source_id}/topics'
    )
    config.add_route(
        'api.v1.get_latest_topic_by_source_id',
        '/v1/sources/{source_id}/topics/latest'
    )
    config.add_route(
        'api.v1.create_refresh',
        '/v1/sources/{source_id}/refreshes',
        request_method="POST"
    )
    config.add_route(
        'api.v1.list_refreshes_by_source_id',
        '/v1/sources/{source_id}/refreshes',
        request_method="GET"
    )
    config.add_route(
        'api.v1.get_topic_by_topic_name',
        '/v1/topics/{topic_name}'
    )
    config.add_route(
        'api.v1.list_schemas_by_topic_name',
        '/v1/topics/{topic_name}/schemas'
    )
    config.add_route(
        'api.v1.get_latest_schema_by_topic_name',
        '/v1/topics/{topic_name}/schemas/latest'
    )
    config.add_route(
        'api.v1.get_topics_by_criteria',
        '/v1/topics'
    )
    # The REST URI matches in order, so please consider the
    # potential conflicts when you make changes.
    config.add_route(
        'api.v1.register_schema_from_mysql_stmts',
        '/v1/schemas/mysql'
    )
    config.add_route(
        'api.v1.register_schema',
        '/v1/schemas/avro'
    )
    config.add_route(
        'api.v1.get_schema_by_id',
        '/v1/schemas/{schema_id}'
    )
    config.add_route(
        'api.v1.get_schema_elements_by_schema_id',
        '/v1/schemas/{schema_id}/elements'
    )
    config.add_route(
        'api.v1.is_avro_schema_compatible',
        '/v1/compatibility/schemas/avro'
    )
    config.add_route(
        'api.v1.is_mysql_schema_compatible',
        '/v1/compatibility/schemas/mysql'
    )
    config.add_route(
        'api.v1.create_note',
        '/v1/notes'
    )
    config.add_route(
        'api.v1.update_note',
        '/v1/notes/{note_id}'
    )
    config.add_route(
        'api.v1.list_categories',
        '/v1/categories'
    )
    config.add_route(
        'doctool.index',
        '/'
    )
    config.add_route(
        'api.v1.get_refreshes_by_criteria',
        '/v1/refreshes'
    )
    config.add_route(
        'api.v1.get_refresh_by_id',
        '/v1/refreshes/{refresh_id}'
    )
    config.add_route(
        'api.v1.update_refresh',
        '/v1/refreshes/{refresh_id}/status'
    )

    config.add_route(
        'api.v1.get_data_targets',
        '/v1/data_targets',
        request_method="GET"
    )
    config.add_route(
        'api.v1.create_data_target',
        '/v1/data_targets',
        request_method="POST"
    )
    config.add_route(
        'api.v1.get_data_target_by_id',
        '/v1/data_targets/{data_target_id}',
        request_method="GET"
    )
    config.add_route(
        'api.v1.get_consumer_groups_by_data_target_id',
        '/v1/data_targets/{data_target_id}/consumer_groups',
        request_method="GET"
    )
    config.add_route(
        'api.v1.create_consumer_group',
        '/v1/data_targets/{data_target_id}/consumer_groups',
        request_method="POST"
    )
    config.add_route(
        'api.v1.get_topics_by_data_target_id',
        '/v1/data_targets/{data_target_id}/topics',
        request_method="GET"
    )

    config.add_route(
        'api.v1.get_consumer_groups',
        '/v1/consumer_groups',
        request_method="GET"
    )
    config.add_route(
        'api.v1.get_consumer_group_by_id',
        '/v1/consumer_groups/{consumer_group_id}',
        request_method="GET"
    )
    config.add_route(
        'api.v1.get_data_sources_by_consumer_group_id',
        '/v1/consumer_groups/{consumer_group_id}/data_sources',
        request_method="GET"
    )
    config.add_route(
        'api.v1.create_consumer_group_data_source',
        '/v1/consumer_groups/{consumer_group_id}/data_sources',
        request_method="POST"
    )

    config.add_route(
        'api.v1.get_schema_migration',
        '/v1/schema_migration',
        request_method="GET"
    )
    # Serve the documentation tool from /web/
    config.add_static_view(name='partials', path='static/html/partials')
    config.add_static_view(name='static', path='static/')
