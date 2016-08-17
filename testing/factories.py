# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

from schematizer import models
from schematizer.models.avro_schema import AvroSchema
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
fake_offset = 0
fake_updated_offset = 500
fake_batch_size = 100
fake_priority = 'MEDIUM'
fake_priority_value = 50
fake_status = 'SUCCESS'
fake_status_value = 3
fake_filter_condition = 'user=test_user'


def create_namespace(namespace_name):
    return models.Namespace.create(session, name=namespace_name)


def get_or_create_namespace(namespace_name):
    namespace = session.query(
        models.Namespace
    ).filter(
        models.Namespace.name == namespace_name
    ).first()
    return namespace or create_namespace(namespace_name)


def create_source(namespace_name, source_name, owner_email=None):
    owner_email = owner_email or 'src@test.com'
    namespace = get_or_create_namespace(namespace_name)
    return models.Source.create(
        session,
        namespace_id=namespace.id,
        name=source_name,
        owner_email=owner_email
    )


def get_or_create_source(namespace_name, source_name, owner_email=None):
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
    return models.Topic.create(session, **params)


def get_or_create_topic(
    topic_name,
    namespace_name=None,
    source_name=None,
):
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
        schema_elements=None,
        topic_name=None,
        namespace=None,
        source=None,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None,
        created_at=None
):
    topic = get_or_create_topic(
        topic_name,
        namespace_name=namespace,
        source_name=source
    )

    avro_schema = models.AvroSchema.create(
        session,
        avro_schema_json=schema_json,
        topic_id=topic.id,
        status=status,
        base_schema_id=base_schema_id,
        created_at=created_at
    )
    schema_elements = (
        schema_elements or
        AvroSchema.create_schema_elements_from_json(schema_json)
    )
    for schema_element in schema_elements:
        schema_element.avro_schema_id = avro_schema.id
        session.add(schema_element)
    session.flush()

    return avro_schema


def create_note(reference_type, reference_id, note_text, last_updated_by):
    return models.Note.create(
        session,
        reference_type=reference_type,
        reference_id=reference_id,
        note=note_text,
        last_updated_by=last_updated_by
    )


def create_refresh(
        source_id,
        offset=0,
        batch_size=100,
        priority=None,
        filter_condition=None,
        avg_rows_per_second_cap=200
):
    priority_value = None if not priority else models.Priority[priority].value
    return models.Refresh.create(
        session,
        source_id=source_id,
        offset=offset,
        batch_size=batch_size,
        priority=priority_value,
        filter_condition=filter_condition,
        avg_rows_per_second_cap=avg_rows_per_second_cap
    )


def create_source_category(source_id, category):
    return models.SourceCategory.create(
        session,
        source_id=source_id,
        category=category
    )


def create_data_target(target_type, destination):
    return models.DataTarget.create(
        session,
        target_type=target_type,
        destination=destination
    )


def create_consumer_group(group_name, data_target):
    return models.ConsumerGroup.create(
        session,
        group_name=group_name,
        data_target_id=data_target.id
    )


def create_consumer_group_data_source(
    consumer_group,
    data_src_type,
    data_src_id
):
    return models.ConsumerGroupDataSource.create(
        session,
        consumer_group_id=consumer_group.id,
        data_source_type=data_src_type,
        data_source_id=data_src_id
    )


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
