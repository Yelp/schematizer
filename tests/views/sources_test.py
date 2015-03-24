# -*- coding: utf-8 -*-
import unittest
from cached_property import cached_property
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound
from pyramid.httpexceptions import HTTPServerError
from sqlalchemy.orm.exc import NoResultFound

from schematizer.views.sources import get_latest_topic_by_source_id
from schematizer.views.sources import get_source_by_id
from schematizer.views.sources import list_sources
from schematizer.views.sources import list_topics_by_source_id
from tests.models.domain_test import DomainFactory
from tests.models.topic_test import TopicFactory


class TestListSources(unittest.TestCase):

    @cached_property
    def default_sources(self):
        return [DomainFactory.create_domain_object()]

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        return dummy_request

    @patch(
        'schematizer.models.domain.list_all_sources',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            list_sources_mock
    ):
        self.assertRaises(
            HTTPServerError,
            list_sources,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.domain.list_all_sources',
            return_value=self.default_sources
        ):
            sources = list_sources(
                self.request_mock
            )
            self.assertEqual(
                sources,
                [source.to_dict() for source in self.default_sources]
            )


class TestGetSourceByID(unittest.TestCase):

    @cached_property
    def default_source(self):
        return DomainFactory.create_domain_object()

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'source_id': '1'}
        return dummy_request

    @patch(
        'schematizer.models.domain.get_source_by_source_id',
        side_effect=NoResultFound()
    )
    def test_none_existing_source_id(
            self,
            get_source_by_source_id_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            get_source_by_id,
            self.request_mock
        )

    @patch(
        'schematizer.models.domain.get_source_by_source_id',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            list_sources_by_namespace_mock
    ):
        self.assertRaises(
            HTTPServerError,
            get_source_by_id,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.domain.get_source_by_source_id',
            return_value=self.default_source
        ):
            source = get_source_by_id(
                self.request_mock
            )
            self.assertEqual(source, self.default_source.to_dict())


class TestListTopicsBySourceID(unittest.TestCase):

    @cached_property
    def default_topics(self):
        return [TopicFactory.create_topic_object(1)]

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'source_id': '1'}
        return dummy_request

    @patch(
        'schematizer.models.topic.list_topics_by_source_id',
        return_value=[]
    )
    def test_list_topics_by_source_id_with_none_existing_source_id(
            self,
            list_topics_by_source_id_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            list_topics_by_source_id,
            self.request_mock
        )

    @patch(
        'schematizer.models.topic.list_topics_by_source_id',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            list_topics_by_source_id_mock
    ):
        self.assertRaises(
            HTTPServerError,
            list_topics_by_source_id,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.topic.list_topics_by_source_id',
            return_value=self.default_topics
        ):
            topics = list_topics_by_source_id(
                self.request_mock
            )
            self.assertEqual(
                topics,
                [topic.to_dict() for topic in self.default_topics]
            )


class TestGetLatestTopicBySourceID(unittest.TestCase):

    @cached_property
    def default_topic(self):
        return TopicFactory.create_topic_object(1)

    @cached_property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'source_id': '1'}
        return dummy_request

    @patch(
        'schematizer.models.topic.get_latest_topic_by_source_id',
        return_value=None
    )
    def test_none_existing_source_id(
            self,
            get_latest_topic_by_source_id_mock
    ):
        self.assertRaises(
            HTTPNotFound,
            get_latest_topic_by_source_id,
            self.request_mock
        )

    @patch(
        'schematizer.models.topic.get_latest_topic_by_source_id',
        side_effect=Exception()
    )
    def test_unknown_exception(
            self,
            get_latest_topic_by_source_id_mock
    ):
        self.assertRaises(
            HTTPServerError,
            list_topics_by_source_id,
            self.request_mock
        )

    def test_happy_case(self):
        with patch(
            'schematizer.models.topic.get_latest_topic_by_source_id',
            return_value=self.default_topic
        ):
            topic = get_latest_topic_by_source_id(
                self.request_mock
            )
            self.assertEqual(topic, self.default_topic.to_dict())
