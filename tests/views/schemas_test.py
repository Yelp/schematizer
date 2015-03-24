# -*- coding: utf-8 -*-
import unittest
from cached_property import cached_property
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPServerError
from sqlalchemy.orm.exc import NoResultFound

from schematizer.views.schemas import get_latest_schema_by_namespace_and_source
from schematizer.views.schemas import get_schema_by_id
from tests.models.avro_schema_test import AvroSchemaFactory
from tests.models.domain_test import DomainFactory
from tests.models.topic_test import TopicFactory


class TestGetSchemaByID(unittest.TestCase):

    @cached_property
    def default_schema(self):
        return AvroSchemaFactory.create_avro_schema_object(1)

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'schema_id': '1'}
        return dummy_request

    @patch(
        'schematizer.models.avro_schema.get_schema_by_schema_id',
        side_effect=NoResultFound()
    )
    def test_none_existing_schema(
            self,
            get_schema_by_schema_id_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            get_schema_by_id,
            self.request_mock
        )

    @patch(
        'schematizer.models.avro_schema.get_schema_by_schema_id',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            get_schema_by_schema_id_mock
    ):
        self.assertRaises(
            HTTPServerError,
            get_schema_by_id,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.avro_schema.get_schema_by_schema_id',
            return_value=self.default_schema
        ):
            schema = get_schema_by_id(
                self.request_mock
            )
            self.assertEqual(schema, self.default_schema.to_dict())


class TestGetLatestSchemaByNamespaceAndSource(unittest.TestCase):

    @cached_property
    def default_source(self):
        return DomainFactory.create_domain_object()

    @cached_property
    def default_topic(self):
        return TopicFactory.create_topic_object(self.default_source.id)

    @cached_property
    def default_schema(self):
        return AvroSchemaFactory.create_avro_schema_object(
            self.default_topic.id
        )

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.params = {
            'namespace': 'yelp',
            'source': 'business'
        }
        return dummy_request

    @patch(
        'schematizer.models.domain.get_source_by_namespace_and_source_name',
        side_effect=NoResultFound()
    )
    def test_none_existing_namespace_and_source(
            self,
            get_source_by_namespace_and_source_name_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            get_latest_schema_by_namespace_and_source,
            self.request_mock
        )

    @patch(
        'schematizer.models.domain.get_source_by_namespace_and_source_name',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            get_source_by_namespace_and_source_name_mock
    ):
        self.assertRaises(
            HTTPServerError,
            get_latest_schema_by_namespace_and_source,
            self.request_mock
        )

    @patch(
        'schematizer.models.topic.get_latest_topic_by_source_id',
        return_value=None
    )
    def test_none_existing_topic(
            self,
            get_latest_topic_by_source_id_mock
    ):
        with patch(
            'schematizer.models.domain.'
            'get_source_by_namespace_and_source_name',
            return_value=self.default_source
        ):
            self.assertRaises(
                HTTPNotFound,
                get_latest_schema_by_namespace_and_source,
                self.request_mock
            )

    @patch(
        'schematizer.models.avro_schema.'
        'get_latest_schema_by_topic_id',
        return_value=None
    )
    def test_none_existing_schema(self, get_latest_schema_by_topic_id_mock):
        with patch(
            'schematizer.models.domain.'
            'get_source_by_namespace_and_source_name',
            return_value=self.default_source
        ):
            with patch(
                'schematizer.models.topic.'
                'get_latest_topic_by_source_id',
                return_value=self.default_topic
            ):
                self.assertRaises(
                    HTTPNotFound,
                    get_latest_schema_by_namespace_and_source,
                    self.request_mock
                )

    def test_happy_case(self):
        with patch(
            'schematizer.models.domain.'
            'get_source_by_namespace_and_source_name',
            return_value=self.default_source
        ):
            with patch(
                'schematizer.models.topic.'
                'get_latest_topic_by_source_id',
                return_value=self.default_topic
            ):
                with patch(
                    'schematizer.models.avro_schema.'
                    'get_latest_schema_by_topic_id',
                    return_value=self.default_schema
                ):
                    schema = get_latest_schema_by_namespace_and_source(
                        self.request_mock
                    )
                    self.assertEqual(schema, self.default_schema.to_dict())
