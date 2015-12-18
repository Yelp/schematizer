# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

from schematizer import models
from schematizer.models.database import session


fake_default_id = 1
fake_namespace = 'yelp'
fake_transformed_namespace = 'yelp_transformed'
fake_source = 'business'
fake_owner_email = 'business@yelp.com'
fake_topic_name = 'yelp.business.v1'
fake_transformed_topic_name = 'yelp.business.v1.transformed'
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
fake_contains_pii = False
fake_restricted_name = 'invalid|namespace'
fake_numeric_name = '12345'
fake_offset = 0
fake_updated_offset = 500
fake_batch_size = 100
fake_priority = 'MEDIUM'
fake_priority_value = 50
fake_status = 'SUCCESS'
fake_status_value = 3
fake_filter_condition = 'user=test_user'


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


def create_source(namespace_name, source_name, owner_email='src@test.com'):
    namespace = get_or_create_namespace(namespace_name)
    source = models.Source(
        namespace_id=namespace.id,
        name=source_name,
        owner_email=owner_email
    )
    session.add(source)
    session.flush()
    return source


def get_or_create_source(namespace_name, source_name, owner_email=None):
    owner_email = owner_email or 'src@test.com'
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


def create_topic(topic_name, namespace_name, source_name, **overrides):
    """Create a topic with specified topic name in the Topic table.  For topic
    attributes to override, see :class:schematizer.models.topic.Topic.
    """
    source = get_or_create_source(namespace_name, source_name)
    params = {
        'name': topic_name,
        'source_id': source.id,
        'contains_pii': False
    }
    params.update(overrides)
    topic = models.Topic(**params)
    session.add(topic)
    session.flush()
    return topic


def get_or_create_topic(topic_name, namespace_name=None, source_name=None):
    """Get the topic of specified topic name. If it doesn't exist, create it
    in the specified namespace name and source name.
    """
    topic = session.query(
        models.Topic
    ).filter(
        models.Topic.name == topic_name
    ).first()
    return topic or create_topic(
        topic_name,
        namespace_name=namespace_name,
        source_name=source_name
    )


def create_avro_schema(
        schema_json,
        schema_elements,
        topic_name=None,
        namespace=None,
        source=None,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    topic = get_or_create_topic(
        topic_name,
        namespace_name=namespace,
        source_name=source
    )

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


def create_note(reference_type, reference_id, note_text, last_updated_by):
    note = models.Note(
        reference_type=reference_type,
        reference_id=reference_id,
        note=note_text,
        last_updated_by=last_updated_by
    )
    session.add(note)
    session.flush()
    return note


def create_refresh(
        source_id,
        offset,
        batch_size,
        priority,
        filter_condition
):
    priority_value = None if not priority else models.Priority[priority].value
    refresh = models.Refresh(
        source_id=source_id,
        offset=offset,
        batch_size=batch_size,
        priority=priority_value,
        filter_condition=filter_condition
    )
    session.add(refresh)
    session.flush()
    return refresh


def create_source_category(source_id, category):
    source_category = models.SourceCategory(
        source_id=source_id,
        category=category
    )
    session.add(source_category)
    session.flush()
    return source_category


class NamespaceFactory(object):

    @classmethod
    def create(
        cls,
        name,
        created_at=fake_created_at,
        updated_at=fake_updated_at,
        fake_id=None
    ):
        namespace = models.Namespace(
            name=name,
            created_at=created_at,
            updated_at=updated_at
        )
        if fake_id:
            namespace.id = fake_id
        return namespace

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
        updated_at=fake_updated_at,
        fake_id=None
    ):
        source = models.Source(
            name=name,
            namespace_id=namespace.id,
            owner_email=owner_email,
            created_at=created_at,
            updated_at=updated_at,
            namespace=namespace,
        )
        if fake_id:
            source.id = fake_id
        return source

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
        contains_pii=fake_contains_pii,
        created_at=fake_created_at,
        updated_at=fake_updated_at,
        fake_id=None
    ):
        topic = models.Topic(
            name=topic_name,
            source_id=source.id,
            created_at=created_at,
            updated_at=updated_at,
            source=source,
            contains_pii=contains_pii,
        )
        if fake_id:
            topic.id = fake_id
        return topic

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
        updated_at=fake_updated_at,
        fake_id=None
    ):
        avro_schema = models.AvroSchema(
            topic_id=topic.id,
            avro_schema=avro_schema,
            base_schema_id=base_schema_id,
            status=status,
            created_at=created_at,
            updated_at=updated_at,
            topic=topic
        )
        if fake_id:
            avro_schema.id = fake_id
        return avro_schema

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


class AvroSchemaElementFactory(object):

    @classmethod
    def create(
        cls,
        avro_schema,
        key,
        element_type,
        doc=None,
        note=None,
        created_at=fake_created_at,
        updated_at=fake_updated_at,
        fake_id=None
    ):
        avro_schema_element = models.AvroSchemaElement(
            avro_schema_id=avro_schema.id,
            key=key,
            element_type=element_type,
            doc=doc,
            created_at=created_at,
            updated_at=updated_at
        )
        if fake_id:
            avro_schema_element.id = fake_id
        return avro_schema_element
