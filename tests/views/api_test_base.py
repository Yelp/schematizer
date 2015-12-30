# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from pyramid import httpexceptions

from schematizer import models
from schematizer.models.database import session
from tests.models.testing_db import DBTestCase


class ApiTestBase(DBTestCase):

    @pytest.yield_fixture
    def mock_request(self):
        with mock.patch('pyramid.request.Request', autospec=True) as mock_req:
            yield mock_req

    def get_expected_namespace_resp(self, namespace_id):
        namespace = self._get_entity_by_id(models.Namespace, namespace_id)
        return {
            'namespace_id': namespace.id,
            'name': namespace.name,
            'created_at': namespace.created_at.isoformat(),
            'updated_at': namespace.updated_at.isoformat()
        }

    def get_expected_src_resp(self, source_id):
        src = self._get_entity_by_id(models.Source, source_id)
        return {
            'source_id': src.id,
            'namespace': self.get_expected_namespace_resp(src.namespace.id),
            'name': src.name,
            'owner_email': src.owner_email,
            'created_at': src.created_at.isoformat(),
            'updated_at': src.updated_at.isoformat()
        }

    def get_expected_topic_resp(self, topic_id):
        topic = self._get_entity_by_id(models.Topic, topic_id)
        return {
            'topic_id': topic.id,
            'name': topic.name,
            'source': self.get_expected_src_resp(topic.source_id),
            'contains_pii': False,
            'created_at': topic.created_at.isoformat(),
            'updated_at': topic.updated_at.isoformat(),
        }

    def get_expected_schema_resp(self, schema_id, **overrides):
        avro_schema = self._get_entity_by_id(models.AvroSchema, schema_id)
        expected = {
            'schema_id': avro_schema.id,
            'schema': avro_schema.avro_schema,
            'topic': self.get_expected_topic_resp(avro_schema.topic_id),
            'status': models.AvroSchemaStatus.READ_AND_WRITE,
            'primary_keys': [],
            'created_at': avro_schema.created_at.isoformat(),
            'updated_at': avro_schema.updated_at.isoformat()
        }
        if overrides:
            expected.update(overrides)
        return expected

    def get_expected_src_refresh_resp(self, src_refresh_id, **overrides):
        src_refresh = self._get_entity_by_id(models.Refresh, src_refresh_id)
        expected = {
            'refresh_id': src_refresh.id,
            'source': self.get_expected_src_resp(src_refresh.source_id),
            'status': models.RefreshStatus(src_refresh.status).name,
            'offset': src_refresh.offset,
            'batch_size': src_refresh.batch_size,
            'priority': models.Priority(src_refresh.priority).name,
            'created_at': src_refresh.created_at.isoformat(),
            'updated_at': src_refresh.updated_at.isoformat()
        }
        if overrides:
            expected.update(overrides)
        return expected

    def _get_entity_by_id(self, entity_cls, entity_id):
        return session.query(entity_cls).filter(
            getattr(entity_cls, 'id') == entity_id
        ).one()

    @classmethod
    def get_http_exception(cls, http_status_code):
        return httpexceptions.status_map[http_status_code]
