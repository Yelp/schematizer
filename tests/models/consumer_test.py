# -*- coding: utf-8 -*-
import pytest

from testing import factories
from tests.models.testing_db import DBTestCase


class TestConsumerGroupModel(DBTestCase):

    @classmethod
    def fake_job_name(self):
        return "test job name"

    @pytest.fixture
    def domain(self):
        return factories.DomainFactory.create_in_db(
            factories.fake_namespace,
            factories.fake_source
        )

    @pytest.fixture
    def topic(self, domain):
        return factories.TopicFactory.create_in_db(
            factories.fake_topic_name,
            domain
        )

    @pytest.fixture
    def fake_data_target(self):
        return factories.DataTargetFactory.create_in_db(
            "target",
            "destination"
        )

    @pytest.fixture
    def fake_consumer_group(self, fake_data_target):
        return factories.ConsumerGroupFactory.create_in_db(
            "group_name",
            "group_type",
            fake_data_target
        )

    @pytest.fixture
    def fake_schema(self, topic):
        return factories.AvroSchemaFactory.create_in_db(
            "test schema",
            topic
        )

    @pytest.fixture
    def fake_consumer(self, fake_schema, fake_consumer_group):
        return factories.ConsumerFactory.create_in_db(
            self.fake_job_name,
            fake_schema,
            fake_consumer_group
        )

    def test_create_consumer(self, fake_consumer, fake_schema):
        assert fake_consumer.schema_id == fake_schema.id
        assert fake_consumer.created_at == factories.fake_created_at
        assert fake_consumer.job_name == self.fake_job_name
