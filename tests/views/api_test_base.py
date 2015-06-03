# -*- coding: utf-8 -*-
import mock
import pytest
from pyramid import httpexceptions

from testing import factories


class TestApiBase(object):

    test_view_module = None

    @property
    def namespace(self):
        return factories.fake_namespace

    @property
    def namespace_response(self):
        return factories.fake_namespace

    @property
    def namespaces(self):
        return [self.namespace]

    @property
    def namespaces_response(self):
        return [self.namespace_response]

    @property
    def source(self):
        return factories.DomainFactory.create(
            factories.fake_namespace,
            factories.fake_source,
        )

    @property
    def source_response(self):
        return {
            'source_id': None,
            'namespace': factories.fake_namespace,
            'source': factories.fake_source,
            'source_owner_email': factories.fake_owner_email,
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @property
    def sources(self):
        return [self.source]

    @property
    def sources_response(self):
        return [self.source_response]

    @property
    def topic(self):
        return factories.TopicFactory.create(
            factories.fake_topic_name,
            self.source,
        )

    @property
    def topic_response(self):
        return {
            'topic_id': None,
            'name': factories.fake_topic_name,
            'source': self.source_response,
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @property
    def topics(self):
        return [self.topic]

    @property
    def topics_response(self):
        return [self.topic_response]

    @property
    def schema(self):
        return factories.AvroSchemaFactory.create(
            factories.fake_avro_schema,
            self.topic
        )

    @property
    def schema_response(self):
        return {
            'schema_id': None,
            'schema': factories.fake_avro_schema,
            'topic': self.topic_response,
            'status': 'RW',
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @property
    def schemas(self):
        return [self.schema]

    @property
    def schemas_response(self):
        return [self.schema_response]

    @pytest.yield_fixture
    def mock_request(self):
        with mock.patch('pyramid.request.Request', autospec=True) as mock_repo:
            yield mock_repo

    @pytest.yield_fixture
    def mock_repo(self):
        with mock.patch(
            self.test_view_module + '.schema_repository',
            autospec=True
        ) as mock_repo:
            yield mock_repo

    @pytest.yield_fixture
    def mock_create_sql_table_from_mysql_stmts(self):
        patch_name = (self.test_view_module +
                      '.view_common.sql_handler'
                      '.create_sql_table_from_sql_stmts')
        with mock.patch(patch_name) as mock_func:
            yield mock_func

    @classmethod
    def get_mock_dict(cls, dict_value):
        mock_dict = mock.MagicMock(spec=dict)
        mock_dict.get.side_effect = lambda k: dict_value.get(k)
        mock_dict.__getitem__.side_effect = lambda k: dict_value[k]
        return mock_dict

    @classmethod
    def get_http_exception(cls, http_status_code):
        return httpexceptions.status_map[http_status_code]
