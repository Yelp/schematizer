# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from pyramid import httpexceptions

from schematizer import models
from schematizer.helpers.formatting import _format_datetime
from schematizer_testing import utils
from tests.models.testing_db import DBTestCase


class ApiTestBase(DBTestCase):

    @pytest.yield_fixture
    def mock_request(self):
        with mock.patch('pyramid.request.Request', autospec=True) as mock_req:
            yield mock_req

    def get_expected_namespace_resp(self, namespace_id):
        namespace = utils.get_entity_by_id(models.Namespace, namespace_id)
        return {
            'namespace_id': namespace.id,
            'name': namespace.name,
            'created_at': _format_datetime(namespace.created_at),
            'updated_at': _format_datetime(namespace.updated_at)
        }

    def get_expected_src_resp(self, source_id):
        src = utils.get_entity_by_id(models.Source, source_id)
        return {
            'source_id': src.id,
            'namespace': self.get_expected_namespace_resp(src.namespace.id),
            'name': src.name,
            'owner_email': src.owner_email,
            'created_at': _format_datetime(src.created_at),
            'updated_at': _format_datetime(src.updated_at)
        }

    def get_expected_topic_resp(self, topic_id):
        topic = utils.get_entity_by_id(models.Topic, topic_id)
        return {
            'topic_id': topic.id,
            'name': topic.name,
            'source': self.get_expected_src_resp(topic.source_id),
            'contains_pii': False,
            'cluster_type': topic.cluster_type,
            'primary_keys': topic.primary_keys,
            'created_at': _format_datetime(topic.created_at),
            'updated_at': _format_datetime(topic.updated_at),
        }

    def get_expected_schema_resp(self, schema_id, **overrides):
        avro_schema = utils.get_entity_by_id(models.AvroSchema, schema_id)
        expected = {
            'schema_id': avro_schema.id,
            'schema': avro_schema.avro_schema,
            'topic': self.get_expected_topic_resp(avro_schema.topic_id),
            'status': models.AvroSchemaStatus.READ_AND_WRITE,
            'primary_keys': [],
            'created_at': _format_datetime(avro_schema.created_at),
            'updated_at': _format_datetime(avro_schema.updated_at)
        }
        if overrides:
            expected.update(overrides)
        return expected

    def get_expected_src_refresh_resp(self, src_refresh_id, **overrides):
        src_refresh = utils.get_entity_by_id(models.Refresh, src_refresh_id)
        expected = {
            'refresh_id': src_refresh.id,
            'source': self.get_expected_src_resp(src_refresh.source_id),
            'status': models.RefreshStatus(src_refresh.status).name,
            'offset': src_refresh.offset,
            'batch_size': src_refresh.batch_size,
            'priority': models.Priority(src_refresh.priority).name,
            'created_at': _format_datetime(src_refresh.created_at),
            'updated_at': _format_datetime(src_refresh.updated_at)
        }
        if src_refresh.avg_rows_per_second_cap is not None:
            expected[
                'avg_rows_per_second_cap'
            ] = src_refresh.avg_rows_per_second_cap
        if overrides:
            expected.update(overrides)
        return expected

    def get_expected_data_target_resp(self, data_target_id, **overrides):
        data_target = utils.get_entity_by_id(models.DataTarget, data_target_id)
        expected = {
            'data_target_id': data_target.id,
            'target_type': data_target.target_type,
            'destination': data_target.destination,
            'created_at': _format_datetime(data_target.created_at),
            'updated_at': _format_datetime(data_target.updated_at)
        }
        if overrides:
            expected.update(overrides)
        return expected

    def get_expected_consumer_group_resp(self, consumer_group_id, **overrides):
        group = utils.get_entity_by_id(models.ConsumerGroup, consumer_group_id)
        expected = {
            'consumer_group_id': group.id,
            'group_name': group.group_name,
            'data_target': self.get_expected_data_target_resp(
                group.data_target.id
            ),
            'created_at': _format_datetime(group.created_at),
            'updated_at': _format_datetime(group.updated_at)
        }
        if overrides:
            expected.update(overrides)
        return expected

    def get_expected_consumer_group_data_src_resp(
        self,
        consumer_group_data_source_id,
        **overrides
    ):
        data_source = utils.get_entity_by_id(
            models.ConsumerGroupDataSource,
            consumer_group_data_source_id
        )
        expected = {
            'consumer_group_data_source_id': data_source.id,
            'consumer_group_id': data_source.consumer_group.id,
            'data_source_type': data_source.data_source_type,
            'data_source_id': data_source.data_source_id,
            'created_at': _format_datetime(data_source.created_at),
            'updated_at': _format_datetime(data_source.updated_at)
        }
        if overrides:
            expected.update(overrides)
        return expected

    @classmethod
    def get_http_exception(cls, http_status_code):
        return httpexceptions.status_map[http_status_code]
