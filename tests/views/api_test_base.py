# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import mock
import pytest
from pyramid import httpexceptions

from schematizer import models
from schematizer.models import Note
from schematizer.models import Priority
from schematizer.models import RefreshStatus
from schematizer.models import SourceCategory
from schematizer.models.avro_schema import AvroSchemaStatus
from schematizer.models.database import session
from testing import factories
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


class TestApiBase(DBTestCase):

    test_view_module = None

    @property
    def namespace_name(self):
        return factories.fake_namespace

    @property
    def namespace_response(self):
        return {
            'namespace_id': factories.fake_default_id,
            'name': factories.fake_namespace,
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @property
    def namespaces(self):
        return [self.namespace_name]

    @property
    def namespaces_response(self):
        return [self.namespace]

    @property
    def source_name(self):
        return factories.fake_source

    @property
    def namespace(self):
        return factories.NamespaceFactory.create(
            factories.fake_namespace,
            fake_id=factories.fake_default_id
        )

    @property
    def source(self):
        return factories.SourceFactory.create(
            factories.fake_source,
            self.namespace,
            fake_id=factories.fake_default_id
        )

    @property
    def source_response(self):
        return {
            'source_id': factories.fake_default_id,
            'namespace': self.namespace_response,
            'name': factories.fake_source,
            'owner_email': factories.fake_owner_email,
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
            fake_id=factories.fake_default_id
        )

    @property
    def topic_response(self):
        return {
            'topic_id': factories.fake_default_id,
            'name': factories.fake_topic_name,
            'source': self.source_response,
            'contains_pii': False,
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat(),
        }

    @property
    def topics(self):
        return [self.topic]

    @property
    def topics_response(self):
        return [self.topic_response]

    @pytest.fixture
    def yelp_namespace(self):
        return factories.create_namespace('yelp')

    @pytest.fixture
    def biz_source(self, yelp_namespace):
        return factories.create_source(
            namespace_name=yelp_namespace.name,
            source_name='biz',
            owner_email='test-dev@yelp.com'
        )

    @pytest.fixture
    def biz_topic(self, biz_source):
        return factories.create_topic(
            topic_name='yelp.biz.1',
            namespace_name=biz_source.namespace.name,
            source_name=biz_source.name
        )

    @pytest.fixture
    def yelp_namespace_response(self, yelp_namespace):
        return {
            'namespace_id': yelp_namespace.id,
            'name': yelp_namespace.name,
            'created_at': yelp_namespace.created_at.isoformat(),
            'updated_at': yelp_namespace.updated_at.isoformat()
        }

    @pytest.fixture
    def biz_source_response(self, biz_source, yelp_namespace_response):
        return {
            'source_id': biz_source.id,
            'namespace': yelp_namespace_response,
            'name': biz_source.name,
            'owner_email': biz_source.owner_email,
            'created_at': biz_source.created_at.isoformat(),
            'updated_at': biz_source.updated_at.isoformat()
        }

    @pytest.fixture
    def biz_topic_response(self, biz_topic, biz_source_response):
        return {
            'topic_id': biz_topic.id,
            'name': biz_topic.name,
            'source': biz_source_response,
            'contains_pii': False,
            'created_at': biz_topic.created_at.isoformat(),
            'updated_at': biz_topic.updated_at.isoformat(),
        }

    @property
    def schema(self):
        return factories.AvroSchemaFactory.create(
            factories.fake_avro_schema,
            self.topic,
            fake_id=factories.fake_default_id
        )

    @property
    def schema_response(self):
        return {
            'schema_id': factories.fake_default_id,
            'schema': factories.fake_avro_schema,
            'topic': self.topic_response,
            'status': AvroSchemaStatus.READ_AND_WRITE,
            'primary_keys': [],
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @property
    def schemas(self):
        return [self.schema]

    @property
    def schemas_response(self):
        return [self.schema_response]

    @property
    def key(self):
        return "schema element key"

    @property
    def element_type(self):
        return "schema element type"

    @property
    def element_doc(self):
        return "schema element doc"

    @property
    def schema_elements(self):
        return [
            factories.AvroSchemaElementFactory.create(
                self.schema,
                self.key,
                self.element_type,
                self.element_doc,
                fake_id=factories.fake_default_id
            )
        ]

    @property
    def schema_elements_response(self):
        return [
            {
                'id': factories.fake_default_id,
                'schema_id': factories.fake_default_id,
                'element_type': self.element_type,
                'key': self.key,
                'doc': self.element_doc,
                'created_at': factories.fake_created_at.isoformat(),
                'updated_at': factories.fake_updated_at.isoformat()
            }
        ]

    @property
    def note_note(self):
        return 'This is a note'

    @property
    def note_reference_type(self):
        return 'schema'

    @property
    def note_last_updated_by(self):
        return 'user@yelp.com'

    @property
    def create_note_request(self):
        return {
            'reference_id': factories.fake_default_id,
            'reference_type': self.note_reference_type,
            'note': self.note_note,
            'last_updated_by': self.note_last_updated_by
        }

    @property
    def update_note_request(self):
        return {
            'note': self.note_note,
            'last_updated_by': self.note_last_updated_by
        }

    @property
    def note(self):
        return Note(
            id=factories.fake_default_id,
            reference_id=factories.fake_default_id,
            reference_type=self.note_reference_type,
            note=self.note_note,
            last_updated_by=self.note_last_updated_by,
            created_at=factories.fake_created_at,
            updated_at=factories.fake_updated_at
        )

    @property
    def note_response(self):
        return {
            'id': factories.fake_default_id,
            'reference_id': factories.fake_default_id,
            'reference_type': self.note_reference_type,
            'note': self.note_note,
            'last_updated_by': self.note_last_updated_by,
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @pytest.fixture
    def refresh_source(self, yelp_namespace):
        return factories.create_source(
            namespace_name=yelp_namespace.name,
            source_name='test_src',
            owner_email='test-dev@yelp.com'
        )

    @pytest.fixture
    def refresh_source_response(self, refresh_source, yelp_namespace_response):
        return {
            'source_id': refresh_source.id,
            'namespace': yelp_namespace_response,
            'name': refresh_source.name,
            'owner_email': refresh_source.owner_email,
            'created_at': refresh_source.created_at.isoformat(),
            'updated_at': refresh_source.updated_at.isoformat()
        }

    @pytest.fixture
    def refresh(self, refresh_source):
        return factories.create_refresh(
            source_id=refresh_source.id,
            offset=factories.fake_offset,
            batch_size=factories.fake_batch_size,
            priority=factories.fake_priority,
            filter_condition=factories.fake_filter_condition,
        )

    @pytest.fixture
    def refresh_response(self, refresh, refresh_source_response):
        return {
            'refresh_id': refresh.id,
            'source': refresh_source_response,
            'status': RefreshStatus(refresh.status).name,
            'offset': refresh.offset,
            'batch_size': refresh.batch_size,
            'priority': Priority(refresh.priority).name,
            'filter_condition': refresh.filter_condition,
            'created_at': refresh.created_at.isoformat(),
            'updated_at': refresh.updated_at.isoformat()
        }

    @pytest.fixture
    def update_refresh_request(self):
        return {
            'status': factories.fake_status,
            'offset': factories.fake_updated_offset
        }

    @pytest.fixture
    def updated_refresh_response(self, refresh_response):
        refresh_response['status'] = factories.fake_status
        refresh_response['offset'] = factories.fake_updated_offset
        return refresh_response

    @pytest.fixture
    def create_refresh_request(self):
        return {
            'offset': factories.fake_offset,
            'batch_size': factories.fake_batch_size,
            'priority': factories.fake_priority,
            'filter_condition': factories.fake_filter_condition
        }

    @pytest.fixture
    def refresh_response_list(self, refresh_response):
        return [refresh_response]

    @property
    def category(self):
        return 'Business Info'

    @property
    def new_category(self):
        return 'Deals'

    @property
    def source_category(self):
        return SourceCategory(
            id=factories.fake_default_id,
            source_id=factories.fake_default_id,
            category=self.category,
            created_at=factories.fake_created_at,
            updated_at=factories.fake_updated_at
        )

    @property
    def category_request(self):
        return {'category': self.category}

    @property
    def source_category_response(self):
        return {
            'source_id': factories.fake_default_id,
            'category': self.category,
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @property
    def categories(self):
        return [self.category, self.new_category]

    @pytest.yield_fixture
    def mock_request(self):
        with mock.patch('pyramid.request.Request', autospec=True) as mock_req:
            yield mock_req

    @pytest.yield_fixture
    def mock_repo(self):
        with mock.patch(
            self.test_view_module + '.schema_repository',
            autospec=True
        ) as mock_repo:
            yield mock_repo

    @pytest.yield_fixture
    def mock_doc_tool(self):
        with mock.patch(
            self.test_view_module + '.doc_tool',
            autospec=True
        ) as mock_doc_tool:
            yield mock_doc_tool

    @pytest.yield_fixture
    def mock_schema(self):
        with mock.patch(
            'schematizer.models.AvroSchema.note',
            new_callable=mock.PropertyMock
        ) as mock_schema:
            mock_schema.return_value = None
            yield mock_schema

    @pytest.yield_fixture
    def mock_schema_element(self):
        with mock.patch(
            'schematizer.models.AvroSchemaElement.note',
            new_callable=mock.PropertyMock
        ) as mock_schema_element:
            mock_schema_element.return_value = None
            yield mock_schema_element

    @classmethod
    def get_mock_dict(cls, dict_value):
        mock_dict = mock.MagicMock(spec=dict)
        mock_dict.get.side_effect = lambda k: dict_value.get(k)
        mock_dict.__getitem__.side_effect = lambda k: dict_value[k]
        return mock_dict

    @classmethod
    def get_http_exception(cls, http_status_code):
        return httpexceptions.status_map[http_status_code]
