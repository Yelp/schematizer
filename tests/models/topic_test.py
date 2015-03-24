# -*- coding: utf-8 -*-
from datetime import datetime
import pytest

from schematizer import models
from schematizer.models.database import session
from schematizer.models.topic import Topic
from tests.models.domain_test import DomainFactory
from tests.models.testing_db import DBTestCase


class TopicFactory(object):

    @classmethod
    def create_topic_db_object(
            cls,
            domain_id,
            topic='yelp.business.v1',
            created_at=datetime.now(),
            updated_at=datetime.now()
    ):
        topic = Topic(
            topic=topic,
            domain_id=domain_id,
            created_at=created_at,
            updated_at=updated_at
        )
        session.add(topic)
        session.flush()
        return topic

    @classmethod
    def create_topic_object(
            cls,
            domain_id,
            topic='yelp.business.v1',
            created_at=datetime.now(),
            updated_at=datetime.now()
    ):
        return Topic(
            topic=topic,
            domain_id=domain_id,
            created_at=created_at,
            updated_at=updated_at
        )


class TestTopicModel(DBTestCase):

    @pytest.fixture
    def default_domain(self):
        return DomainFactory.create_domain_db_object()

    @pytest.fixture
    def default_topic(self, default_domain):
        return TopicFactory.create_topic_db_object(default_domain.id)

    def test_list_topics_by_source_id(self, default_domain, default_topic):
        topic_list = models.topic.list_topics_by_source_id(
            default_domain.id
        )
        assert [default_topic] == topic_list

    def test_get_latest_topic_by_source_id(self, default_domain):
        # Test get_latest_topic_by_source_id happy case.
        TopicFactory.create_topic_db_object(
            default_domain.id,
            topic='yelp.business.v1',
            created_at=datetime.min
        )
        latest_topic = TopicFactory.create_topic_db_object(
            default_domain.id,
            topic='yelp.business.v2'
        )
        topic = models.topic.get_latest_topic_by_source_id(
            default_domain.id
        )
        assert latest_topic == topic

        # Test get_latest_topic_by_source_id with none existing source_id.
        topic = models.topic.get_latest_topic_by_source_id(
            1111
        )
        assert None == topic

    def test_get_topic_by_topic_name(self, default_topic):
        topic = models.topic.get_topic_by_topic_name(
            default_topic.topic
        )
        assert default_topic == topic
