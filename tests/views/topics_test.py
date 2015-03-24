# -*- coding: utf-8 -*-
import unittest
from cached_property import cached_property
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPServerError
from sqlalchemy.orm.exc import NoResultFound

from tests.models.avro_schema_test import AvroSchemaFactory
from tests.models.topic_test import TopicFactory
from schematizer.views.topics import get_latest_schema_by_topic_name
from schematizer.views.topics import get_topic_by_topic_name
from schematizer.views.topics import list_schemas_by_topic_name


class TestGetTopicByTopicName(unittest.TestCase):

    @cached_property
    def default_topic(self):
        return TopicFactory.create_topic_object(1)

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'topic_name': 'yelp.business.v1'}
        return dummy_request

    @patch(
        'schematizer.models.topic.get_topic_by_topic_name',
        side_effect=NoResultFound()
    )
    def test_none_existing_topic_name(
            self,
            get_topic_by_topic_name_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            get_topic_by_topic_name,
            self.request_mock
        )

    @patch(
        'schematizer.models.topic.get_topic_by_topic_name',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            get_topic_by_topic_name_mock
    ):
        self.assertRaises(
            HTTPServerError,
            get_topic_by_topic_name,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.topic.get_topic_by_topic_name',
            return_value=self.default_topic
        ):
            topic = get_topic_by_topic_name(
                self.request_mock
            )
            self.assertEqual(topic, self.default_topic.to_dict())


class TestListSchemasByTopicName(unittest.TestCase):

    @cached_property
    def default_topic(self):
        return TopicFactory.create_topic_object(1)

    @cached_property
    def default_schemas(self):
        return [
            AvroSchemaFactory.create_avro_schema_object(
                self.default_topic.id
            )
        ]

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'topic_name': 'yelp.business.v1'}
        return dummy_request

    @patch(
        'schematizer.models.topic.get_topic_by_topic_name',
        side_effect=NoResultFound()
    )
    def test_none_existing_topic_name(
            self,
            get_topic_by_topic_name_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            list_schemas_by_topic_name,
            self.request_mock
        )

    @patch(
        'schematizer.models.topic.get_topic_by_topic_name',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            get_topic_by_topic_name_mock
    ):
        self.assertRaises(
            HTTPServerError,
            list_schemas_by_topic_name,
            self.request_mock
        )

    @patch(
        'schematizer.models.avro_schema.list_schemas_by_topic_id',
        return_value=[]
    )
    def test_none_existing_schemas(
            self,
            list_schemas_by_topic_id_mock
    ):
        with patch(
            'schematizer.models.topic.get_topic_by_topic_name',
            return_value=self.default_topic
        ):
            self.assertRaises(
                HTTPNotFound,
                list_schemas_by_topic_name,
                self.request_mock
            )

    def test_happy_case(self):
        with patch(
            'schematizer.models.topic.get_topic_by_topic_name',
            return_value=self.default_topic
        ):
            with patch(
                'schematizer.models.avro_schema.list_schemas_by_topic_id',
                return_value=self.default_schemas
            ):
                schemas = list_schemas_by_topic_name(
                    self.request_mock
                )
                self.assertEqual(
                    schemas,
                    [schema.to_dict() for schema in self.default_schemas]
                )


class TestGetLatestSchemaByTopicName(unittest.TestCase):

    @cached_property
    def default_topic(self):
        return TopicFactory.create_topic_object(1)

    @cached_property
    def default_schema(self):
        return AvroSchemaFactory.create_avro_schema_object(
            self.default_topic.id
        )

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'topic_name': 'yelp.business.v1'}
        return dummy_request

    @patch(
        'schematizer.models.topic.get_topic_by_topic_name',
        side_effect=NoResultFound()
    )
    def test_none_existing_topic_name(
            self,
            get_topic_by_topic_name_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            get_latest_schema_by_topic_name,
            self.request_mock
        )

    @patch(
        'schematizer.models.topic.get_topic_by_topic_name',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            get_topic_by_topic_name_mock
    ):
        self.assertRaises(
            HTTPServerError,
            get_latest_schema_by_topic_name,
            self.request_mock
        )

    @patch(
        'schematizer.models.avro_schema.get_latest_schema_by_topic_id',
        return_value=None
    )
    def test_none_existing_schema(
            self,
            get_latest_topic_by_source_id_mock
    ):
        with patch(
            'schematizer.models.topic.get_topic_by_topic_name',
            return_value=self.default_topic
        ):
            self.assertRaises(
                HTTPNotFound,
                get_latest_schema_by_topic_name,
                self.request_mock
            )

    def test_happy_case(self):
        with patch(
            'schematizer.models.topic.get_topic_by_topic_name',
            return_value=self.default_topic
        ):
            with patch(
                'schematizer.models.avro_schema.get_latest_schema_by_topic_id',
                return_value=self.default_schema
            ):
                schema = get_latest_schema_by_topic_name(
                    self.request_mock
                )
                self.assertEqual(schema, self.default_schema.to_dict())
