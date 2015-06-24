# -*- coding: utf-8 -*-
from datetime import datetime

from schematizer import models
from schematizer.models.database import session


fake_namespace = 'yelp'
fake_source = 'business'
fake_owner_email = 'business@yelp.com'
fake_topic_name = 'yelp.business.v1'
fake_avro_schema = '{"name": "business"}'
fake_created_at = datetime(2015, 1, 1, 17, 0, 0)
fake_updated_at = datetime(2015, 1, 1, 17, 0, 1)
fake_base_schema_id = 10
fake_mysql_create_stmts = ['create table foo']
fake_mysql_alter_stmts = ['create table foo',
                          'alter table foo',
                          'create table foo']


class DomainFactory(object):

    @classmethod
    def create(
        cls,
        namespace,
        source,
        owner_email=fake_owner_email,
        created_at=fake_created_at,
        updated_at=fake_updated_at
    ):
        return models.Domain(
            namespace=namespace,
            source=source,
            owner_email=owner_email,
            created_at=created_at,
            updated_at=updated_at
        )

    @classmethod
    def create_in_db(cls, namespace, source):
        domain = cls.create(namespace, source)
        session.add(domain)
        session.flush()
        return domain

    @classmethod
    def delete_topics(cls, domain_id):
        topics = session.query(
            models.Topic
        ).filter(
            models.Topic.domain_id == domain_id
        ).all()
        for topic in topics:
            session.delete(topic)
        session.flush()


class TopicFactory(object):

    @classmethod
    def create(
        cls,
        topic_name,
        domain,
        created_at=fake_created_at,
        updated_at=fake_updated_at
    ):
        return models.Topic(
            name=topic_name,
            domain_id=domain.id,
            created_at=created_at,
            updated_at=updated_at,
            domain=domain
        )

    @classmethod
    def create_in_db(cls, topic_name, domain):
        topic = cls.create(topic_name, domain)
        session.add(topic)
        session.flush()
        return topic

    @classmethod
    def delete_avro_schemas(cls, topic_id):
        avro_schemas = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.topic_id == topic_id
        ).all()
        for avro_schema in avro_schemas:
            session.delete(avro_schema)
        session.flush()


class AvroSchemaFactory(object):

    @classmethod
    def create(
        cls,
        avro_schema,
        topic,
        base_schema_id=None,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        created_at=fake_created_at,
        updated_at=fake_updated_at
    ):
        return models.AvroSchema(
            topic_id=topic.id,
            avro_schema=avro_schema,
            base_schema_id=base_schema_id,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            topic=topic
        )

    @classmethod
    def create_in_db(
        cls,
        avro_schema,
        topic,
        base_schema_id=None,
        status=models.AvroSchemaStatus.READ_AND_WRITE
    ):
        avro_schema = cls.create(avro_schema, topic, base_schema_id, status)
        session.add(avro_schema)
        session.flush()
        return avro_schema

    @classmethod
    def delete(cls, avro_schema_id):
        avro_schema = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == avro_schema_id
        ).first()
        if avro_schema:
            session.delete(avro_schema)
        session.flush()


class ConsumerGroupFactory(object):

    @classmethod
    def create(
        cls,
        group_name,
        group_type,
        data_target,
    ):
        return models.ConsumerGroup(
            group_name=group_name,
            group_type=group_type,
            data_target_id=data_target.id
        )

    @classmethod
    def create_in_db(
        cls,
        group_name,
        group_type,
        data_target,
    ):
        consumer_group = cls.create(group_name, group_type, data_target)
        session.add(consumer_group)
        session.flush()
        return consumer_group


class ConsumerGroupDataSourcesFactory(object):

    @classmethod
    def create(
        cls,
        consumer_group,
        data_source_type,
        data_source_id
    ):
        return models.ConsumerGroupDataSources(
            consumer_group_id=consumer_group.id,
            data_source_type=data_source_type,
            data_source_id=data_source_id
        )

    @classmethod
    def create_in_db(
        cls,
        consumer_group,
        data_source_type,
        data_source_id
    ):
        consumer_group_data_sources = cls.create(
            consumer_group,
            data_source_type,
            data_source_id
        )
        session.add(consumer_group_data_sources)
        session.flush()
        return consumer_group_data_sources


class DataTargetFactory(object):

    @classmethod
    def create(cls, target_type, destination):
        return models.DataTarget(
            target_type=target_type,
            destination=destination
        )

    @classmethod
    def create_in_db(cls, target_type, destination):
        data_target = cls.create(target_type, destination)
        session.add(data_target)
        session.flush()
        return data_target
