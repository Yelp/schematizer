# -*- coding: utf-8 -*-
import mock
import pytest
from pyramid import httpexceptions

from schematizer.models import Note
from testing import factories


class TestApiBase(object):

    test_view_module = None

    @property
    def namespace_name(self):
        return factories.fake_namespace

    @property
    def namespace_response(self):
        return {
            'namespace_id': None,
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
        return factories.NamespaceFactory.create(factories.fake_namespace)

    @property
    def source(self):
        return factories.SourceFactory.create(
            factories.fake_source,
            self.namespace
        )

    @property
    def source_response(self):
        return {
            'source_id': None,
            'namespace': self.namespace_response,
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
            'primary_keys': [],
            'note': None,
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
    def schema_elements(self):
        return [
            factories.AvroSchemaElementFactory.create(
                self.schema,
                self.key,
                self.element_type,
            )
        ]

    @property
    def schema_elements_response(self):
        return [
            {
                'id': None,
                'schema_id': None,
                'element_type': self.element_type,
                'key': self.key,
                'doc': None,
                'note': None,
                'created_at': factories.fake_created_at.isoformat(),
                'updated_at': factories.fake_updated_at.isoformat()
            }
        ]

    @property
    def create_note_request(self):
        return {
            'reference_id': None,
            'reference_type': 'schema',
            'note': 'This is a note',
            'last_updated_by': 'user@yelp.com'
        }

    @property
    def update_note_request(self):
        return {
            'note': 'This is a note',
            'last_updated_by': 'user@yelp.com'
        }

    @property
    def note(self):
        return Note(
            reference_type='schema',
            note='This is a note',
            last_updated_by='user@yelp.com',
            created_at=factories.fake_created_at,
            updated_at=factories.fake_updated_at
        )

    @property
    def note_response(self):
        return {
            'id': None,
            'reference_id': None,
            'reference_type': 'schema',
            'note': 'This is a note',
            'last_updated_by': 'user@yelp.com',
            'created_at': factories.fake_created_at.isoformat(),
            'updated_at': factories.fake_updated_at.isoformat()
        }

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
