# -*- coding: utf-8 -*-
import mock
import pytest
from pyramid import httpexceptions

from schematizer.models import Note
from schematizer.models import RefreshStatus
from schematizer.models import SourceCategory
from schematizer.models.avro_schema import AvroSchemaStatus
from testing import factories
from tests.models.testing_db import DBTestCase


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

    @property
    def refresh_source_id(self):
        return factories.fake_default_id

    @property
    def refresh_status(self):
        return RefreshStatus.NOT_STARTED

    @property
    def updated_refresh_status(self):
        return RefreshStatus.SUCCESS

    @property
    def refresh_offset(self):
        return factories.fake_offset

    @property
    def refresh_batch_size(self):
        return factories.fake_batch_size

    @property
    def refresh_priority(self):
        return factories.fake_priority

    @property
    def refresh_where(self):
        return factories.fake_where

    @property
    def register_refresh_request(self):
        return {
            'status': self.refresh_status,
            'offset': self.refresh_offset,
            'batch_size': self.refresh_batch_size,
            'priority': self.refresh_priority,
            'where': self.refresh_where
        }

    @property
    def update_refresh_request(self):
        return {
            'status': self.updated_refresh_status
        }

    @property
    def refresh(self):
        return factories.RefreshFactory.create(
            fake_id=factories.fake_default_id,
            source=self.source,
            status=self.refresh_status,
            offset=self.refresh_offset,
            batch_size=self.refresh_batch_size,
            priority=self.refresh_priority,
            where=self.refresh_where,
        )

    @property
    def refresh_response(self):
        return {
            'refresh_id': factories.fake_default_id,
            'source': self.source_response,
            'status': self.refresh_status,
            'offset': self.refresh_offset,
            'batch_size': self.refresh_batch_size,
            'priority': self.refresh_priority,
            'where': self.refresh_where,
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

    @property
    def refresh_list(self):
        return [self.refresh]

    @property
    def refresh_response_list(self):
        return [self.refresh_response]

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
