# -*- coding: utf-8 -*-
from datetime import datetime

from schematizer import models
from schematizer.models.database import session


fake_namespace = 'yelp'
fake_source = 'business'
fake_owner_email = 'business@yelp.com'
fake_user_email = 'user@yelp.com'
fake_topic_name = 'yelp.business.v1'
fake_avro_schema = '{"name": "business"}'
fake_created_at = datetime(2015, 1, 1, 17, 0, 0)
fake_updated_at = datetime(2015, 1, 1, 17, 0, 1)
fake_base_schema_id = 10
fake_consumer_email = 'consumer@yelp.com'
fake_frequency = 500
fake_mysql_create_stmts = ['create table foo']
fake_mysql_alter_stmts = ['create table foo',
                          'alter table foo',
                          'create table foo']


def create_namespace(namespace_name):
    namespace = models.Namespace(name=namespace_name)
    session.add(namespace)
    session.flush()
    return namespace


def get_or_create_namespace(namespace_name):
    namespace = session.query(
        models.Namespace
    ).filter(
        models.Namespace.name == namespace_name
    ).first()
    return namespace or create_namespace(namespace_name)


def create_source(namespace_name, source_name, owner_email=fake_owner_email):
    namespace = get_or_create_namespace(namespace_name)
    source = models.Source(
        namespace_id=namespace.id,
        name=source_name,
        owner_email=owner_email
    )
    session.add(source)
    session.flush()
    return source


def get_or_create_source(
    namespace_name,
    source_name,
    owner_email=fake_owner_email
):
    source = session.query(
        models.Source
    ).join(
        models.Namespace
    ).filter(
        models.Namespace.name == namespace_name,
        models.Source.name == source_name
    ).first()
    return source or create_source(
        namespace_name,
        source_name,
        owner_email=owner_email
    )


def create_topic(topic_name, namespace=fake_namespace, source=fake_source):
    source = get_or_create_source(namespace, source)
    topic = models.Topic(name=topic_name, source_id=source.id)
    session.add(topic)
    session.flush()
    return topic


def get_or_create_topic(
        topic_name,
        namespace=fake_namespace,
        source=fake_source
):
    topic = session.query(
        models.Topic
    ).filter(
        models.Topic.name == topic_name
    ).first()
    return topic or create_topic(
        topic_name,
        namespace=namespace,
        source=source
    )


def create_avro_schema(
        schema_json,
        schema_elements,
        topic_name=fake_topic_name,
        namespace=fake_namespace,
        source=fake_source,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    topic = get_or_create_topic(topic_name, namespace=namespace, source=source)

    avro_schema = models.AvroSchema(
        avro_schema_json=schema_json,
        topic_id=topic.id,
        status=status,
        base_schema_id=base_schema_id
    )
    session.add(avro_schema)
    session.flush()

    for schema_element in schema_elements:
        schema_element.avro_schema_id = avro_schema.id
        session.add(schema_element)
    session.flush()

    return avro_schema


def create_note(
    note_type,
    reference_id,
    note_text,
    last_updated_by=fake_user_email
):
    note = models.Note(
        note_type=note_type,
        reference_id=reference_id,
        note=note_text,
        last_updated_by=last_updated_by
    )
    session.add(note)
    session.flush
    return note


class NamespaceFactory(object):

    @classmethod
    def create(
        cls,
        name,
        created_at=fake_created_at,
        updated_at=fake_updated_at
    ):
        return models.Namespace(
            name=name,
            created_at=created_at,
            updated_at=updated_at
        )

    @classmethod
    def create_in_db(cls, name):
        namespace = cls.create(name)
        session.add(namespace)
        session.flush()
        return namespace


class SourceFactory(object):

    @classmethod
    def create(
        cls,
        name,
        namespace,
        owner_email=fake_owner_email,
        created_at=fake_created_at,
        updated_at=fake_updated_at
    ):
        return models.Source(
            name=name,
            namespace_id=namespace.id,
            owner_email=owner_email,
            created_at=created_at,
            updated_at=updated_at,
            namespace=namespace
        )

    @classmethod
    def create_in_db(cls, name, namespace):
        source = cls.create(name, namespace)
        session.add(source)
        session.flush()
        return source

    @classmethod
    def delete_topics(cls, source_id):
        topics = session.query(
            models.Topic
        ).filter(
            models.Topic.source_id == source_id
        ).all()
        for topic in topics:
            session.delete(topic)
        session.flush()


class TopicFactory(object):

    @classmethod
    def create(
        cls,
        topic_name,
        source,
        created_at=fake_created_at,
        updated_at=fake_updated_at
    ):
        return models.Topic(
            name=topic_name,
            source_id=source.id,
            created_at=created_at,
            updated_at=updated_at,
            source=source
        )

    @classmethod
    def create_in_db(cls, topic_name, source):
        topic = cls.create(topic_name, source)
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


class ConsumerFactory(object):

    @classmethod
    def create(cls, job_name, schema, consumer_group):
        return models.Consumer(
            email=fake_consumer_email,
            job_name=job_name,
            expected_frequency=fake_frequency,
            schema_id=schema.id,
            last_used_at=None,
            created_at=fake_created_at,
            updated_at=fake_updated_at,
            consumer_group_id=consumer_group.id
        )

    @classmethod
    def create_in_db(cls, job_name, schema, consumer_group):
        consumer = cls.create(job_name, schema, consumer_group)
        session.add(consumer)
        session.flush()
        return consumer


class ConsumerGroupFactory(object):

    @classmethod
    def create(cls, group_name, group_type, data_target):
        return models.ConsumerGroup(
            group_name=group_name,
            group_type=group_type,
            data_target_id=data_target.id
        )

    @classmethod
    def create_in_db(cls, group_name, group_type, data_target):
        consumer_group = cls.create(group_name, group_type, data_target)
        session.add(consumer_group)
        session.flush()
        return consumer_group


class ConsumerGroupDataSourceFactory(object):

    @classmethod
    def create(
        cls,
        consumer_group,
        data_source_type,
        data_source_id
    ):
        return models.ConsumerGroupDataSource(
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
        consumer_group_data_source = cls.create(
            consumer_group,
            data_source_type,
            data_source_id
        )
        session.add(consumer_group_data_source)
        session.flush()
        return consumer_group_data_source


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
