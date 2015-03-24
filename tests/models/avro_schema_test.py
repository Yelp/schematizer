# -*- coding: utf-8 -*-
from datetime import datetime
import pytest

from schematizer import models
from schematizer.models.avro_schema import AvroSchema
from schematizer.models.database import session
from tests.models.domain_test import DomainFactory
from tests.models.testing_db import DBTestCase
from tests.models.topic_test import TopicFactory


class AvroSchemaFactory(object):

    @classmethod
    def create_avro_schema_db_object(
            cls,
            topic_id,
            avro_schema='test schema',
            base_schema_id=None,
            status='RW',
            created_at=datetime.now(),
            updated_at=datetime.now()
    ):
        avro_schema = AvroSchema(
            topic_id=topic_id,
            avro_schema=avro_schema,
            base_schema_id=base_schema_id,
            status=status,
            created_at=created_at,
            updated_at=updated_at
        )
        session.add(avro_schema)
        session.flush()
        return avro_schema

    @classmethod
    def create_avro_schema_object(
            cls,
            topic_id,
            avro_schema='test schema',
            base_schema_id=None,
            status='RW',
            created_at=datetime.now(),
            updated_at=datetime.now()
    ):
        return AvroSchema(
            topic_id=topic_id,
            avro_schema=avro_schema,
            base_schema_id=base_schema_id,
            status=status,
            created_at=created_at,
            updated_at=updated_at
        )


class TestAvroSchemaModel(DBTestCase):

    @pytest.fixture
    def default_domain(self):
        return DomainFactory.create_domain_db_object()

    @pytest.fixture
    def default_topic(self, default_domain):
        return TopicFactory.create_topic_db_object(default_domain.id)

    @pytest.fixture
    def default_avro_schema(self, default_topic):
        return AvroSchemaFactory.create_avro_schema_db_object(default_topic.id)

    def test_get_schema_by_schema_id(self, default_avro_schema):
        avro_schema = models.avro_schema.get_schema_by_schema_id(
            default_avro_schema.id
        )
        assert default_avro_schema == avro_schema

    def test_list_schemas_by_topic_id(
            self,
            default_topic,
            default_avro_schema
    ):
        avro_schemas = models.avro_schema.list_schemas_by_topic_id(
            default_topic.id
        )
        assert [default_avro_schema] == avro_schemas

    def test_get_latest_schema_by_topic_id(self, default_topic):
        # Test get_latest_schema_by_topic_id happy case.
        AvroSchemaFactory.create_avro_schema_db_object(
            default_topic.id,
            avro_schema='test schema2',
            created_at=datetime.min
        )
        latest_avro_schema = AvroSchemaFactory.create_avro_schema_db_object(
            default_topic.id,
            avro_schema='test schema2'
        )
        avro_schema = models.avro_schema.get_latest_schema_by_topic_id(
            default_topic.id
        )
        assert latest_avro_schema == avro_schema

        # Test get_latest_schema_by_topic_id with none existing topic_id.
        avro_schema = models.avro_schema.get_latest_schema_by_topic_id(
            1111
        )
        assert None == avro_schema
