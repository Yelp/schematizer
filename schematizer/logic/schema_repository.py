# -*- coding: utf-8 -*-
import uuid

import simplejson
from sqlalchemy import exc
from sqlalchemy.orm import exc as orm_exc

from schematizer import models
from schematizer.components.converters.converter_base import BaseConverter
from schematizer.logic.schema_resolution import SchemaCompatibilityValidator
from schematizer.models.database import session


def is_backward_compatible(old_schema_json, new_schema_json):
    """Whether the data serialized using specified old_schema_json can be
    deserialized using specified new_schema_json.
    """
    return SchemaCompatibilityValidator.is_backward_compatible(
        old_schema_json,
        new_schema_json
    )


def is_forward_compatible(old_schema_json, new_schema_json):
    """Whether the data serialized using specified new_schema_json can be
    deserialized using specified old_schema_json.
    """
    return SchemaCompatibilityValidator.is_backward_compatible(
        new_schema_json,
        old_schema_json
    )


def is_full_compatible(old_schema_json, new_schema_json):
    """Whether the data serialized using specified old_schema_json can be
    deserialized using specified new_schema_json, and vice versa.
    """
    return (is_backward_compatible(old_schema_json, new_schema_json)
            and is_forward_compatible(old_schema_json, new_schema_json))


class EntityNotFoundException(Exception):
    pass


class IncompatibleSchemaException(Exception):
    pass


def load_converters():
    __import__('schematizer.components.converters', fromlist=['converters'])
    _converters = dict()
    for cls in BaseConverter.__subclasses__():
        _converters[(cls.source_type, cls.target_type)] = cls
    return _converters


converters = load_converters()


def convert_schema(source_type, target_type, source_schema):
    """Convert the source type schema to the target type schema. The
    source_type and target_type are the SchemaKindEnum.
    """
    converter = converters.get((source_type, target_type))
    if not converter:
        raise Exception("Unable to find converter to convert from {0} to {1}."
                        .format(source_type, target_type))
    return converter().convert(source_schema)


def create_avro_schema_from_avro_json(
        avro_schema_json,
        namespace_name,
        source_name,
        source_email_owner,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    """Add an Avro schema of given schema json object into schema store.
    The steps from checking compatibility to create new topic should be atomic.
    """
    namespace = _get_namespace_or_create(namespace_name)
    _lock_namespace(namespace)
    source = _get_source_or_create(
        namespace.id,
        source_name,
        source_email_owner
    )
    _lock_source(source)

    topic = get_latest_topic_of_source_id(source.id)

    # Lock topic and its schemas so that no other transaction can add new
    # schema to the topic or change schema status.
    _lock_topic_and_schemas(topic)

    if (not topic or not is_schema_compatible_in_topic(
            avro_schema_json,
            topic.name
    )):
        # Note that creating duplicate topic names will throw a sqlalchemy
        # IntegrityError exception. When it occurs, it indicates the uuid
        # is generating the same value (rarely) and we'd like to know it.
        topic_name = _construct_topic_name(namespace_name, source_name)
        topic = _create_topic(topic_name, source.id)

    # Do not create the schema if it is the same as the latest one
    latest_schema = get_latest_schema_by_topic_id(topic.id)
    if (latest_schema
            and latest_schema.avro_schema_json == avro_schema_json
            and latest_schema.base_schema_id == base_schema_id):
        return latest_schema

    avro_schema = _create_avro_schema(
        avro_schema_json,
        topic.id,
        status,
        base_schema_id
    )
    return avro_schema


def _get_namespace_or_create(namespace_name):
    try:
        return session.query(
            models.Namespace
        ).filter(
            models.Namespace.name == namespace_name
        ).one()
    except orm_exc.NoResultFound:
        return _create_namespace_if_not_exist(namespace_name)


def _get_source_or_create(namespace_id, source_name, owner_email):
    try:
        return session.query(
            models.Source
        ).filter(
            models.Source.namespace_id == namespace_id,
            models.Source.name == source_name
        ).one()
    except orm_exc.NoResultFound:
        return _create_source_if_not_exist(
            namespace_id,
            source_name,
            owner_email
        )


def _create_namespace_if_not_exist(namespace_name):
    try:
        # Create a savepoint before trying to create new namespace so that
        # in the case which the IntegrityError occurs, the session will
        # rollback to savepoint. Upon exiting the nested Context, commit/
        # rollback is automatically issued and no need to add it explicitly
        with session.begin_nested():
            new_namespace = models.Namespace(name=namespace_name)
            session.add(new_namespace)
    except exc.IntegrityError:
        # Ignore this error due to trying to create a duplicate namespace
        new_namespace = get_namespace_by_name(namespace_name)
    return new_namespace


def _create_source_if_not_exist(namespace_id, source_name, owner_email):
    try:
        # Create a savepoint before trying to create new source so that
        # in the case which the IntegrityError occurs, the session will
        # rollback to savepoint. Upon exiting the nested Context, commit/
        # rollback is automatically issued and no need to add it explicitly
        with session.begin_nested():
            new_source = models.Source(
                namespace_id=namespace_id,
                name=source_name,
                owner_email=owner_email
            )
            session.add(new_source)
    except exc.IntegrityError:
        # Ignore this error due to trying to create a duplicate source
        new_source = _get_source_by_namespace_id_and_src_name(
            namespace_id,
            source_name
        )
    return new_source


def _get_source_by_namespace_id_and_src_name(namespace_id, source):
    session.query(
        models.Source
    ).filter(
        models.Source.namespace_id == namespace_id,
        models.Source.name == source
    ).first()


def _lock_namespace(namespace):
    session.query(
        models.Namespace
    ).filter(
        models.Namespace.id == namespace.id
    ).with_for_update()


def _lock_source(source):
    session.query(
        models.Source
    ).filter(
        models.Source.id == source.id
    ).with_for_update()


def _lock_topic_and_schemas(topic):
    if not topic:
        return
    session.query(
        models.Topic
    ).filter(
        models.Topic.id == topic.id
    ).with_for_update()
    session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic.id
    ).with_for_update()


def get_latest_topic_of_namespace_source(namespace_name, source_name):
    return session.query(
        models.Topic
    ).join(
        models.Source,
        models.Namespace
    ).filter(
        models.Namespace.id == models.Source.namespace_id,
        models.Source.id == models.Topic.source_id,
        models.Namespace.name == namespace_name,
        models.Source.name == source_name
    ).order_by(
        models.Topic.id.desc()
    ).first()


def is_schema_compatible_in_topic(target_schema, topic_name):
    """Check whether given schema is a valid Avro schema and compatible
    with existing schemas in the specified topic. Note that target_schema
    is the avro json object.
    """
    enabled_schemas = get_schemas_by_topic_name(topic_name)
    for enabled_schema in enabled_schemas:
        schema_json = simplejson.loads(enabled_schema.avro_schema)
        if not is_full_compatible(schema_json, target_schema):
            return False
    return True


def _construct_topic_name(namespace, source):
    return '.'.join((namespace, source, uuid.uuid4().hex))


def _create_topic(topic_name, source_id):
    """Create a topic named `topic_name` in the given source.
    It returns a newly created topic. If a topic with the same
    name already exists, an exception is thrown
    """
    topic = models.Topic(name=topic_name, source_id=source_id)
    session.add(topic)
    session.flush()
    return topic


def get_topic_by_name(topic_name):
    """Get topic of specified topic name. It returns None if the specified
    topic is not found.
    """
    return session.query(
        models.Topic
    ).filter(
        models.Topic.name == topic_name
    ).first()


def get_namespace_by_name(namespace):
    return session.query(
        models.Namespace
    ).filter(
        models.Namespace.name == namespace
    ).first()


def get_source_by_fullname(namespace_name, source_name):
    return session.query(
        models.Source
    ).join(
        models.Namespace
    ).filter(
        models.Namespace.name == namespace_name,
        models.Source.name == source_name
    ).first()


def _create_avro_schema(
        avro_schema_json,
        topic_id,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    avro_schema = models.AvroSchema(
        avro_schema_json=avro_schema_json,
        topic_id=topic_id,
        status=status,
        base_schema_id=base_schema_id
    )
    session.add(avro_schema)

    # TODO[clin|DATAPIPE-224]: create schema elements of new Avro schema

    session.flush()
    return avro_schema


def get_schema_by_id(schema_id):
    """Get the Avro schema of specified id. It returns None if not found.
    """
    return session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.id == schema_id
    ).first()


def get_latest_schema_by_topic_id(topic_id):
    """Get the latest enabled (Read-Write or Read-Only) schema of given topic.
    It returns None if no such schema can be found.
    """
    return session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic_id,
        models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
    ).order_by(
        models.AvroSchema.id.desc()
    ).first()


def get_latest_schema_by_topic_name(topic_name):
    """Get the latest enabled (Read-Write or Read-Only) schema of given topic.
    It returns None if no such schema can be found.
    """
    return session.query(
        models.AvroSchema
    ).join(
        models.Topic
    ).filter(
        models.AvroSchema.topic_id == models.Topic.id,
        models.Topic.name == topic_name,
        models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
    ).order_by(
        models.AvroSchema.id.desc()
    ).first()


def is_schema_compatible(target_schema, namespace, source):
    """Check whether given schema is a valid Avro schema. It then determines
    the topic of given Avro schema belongs to and checks the compatibility
    against the existing schemas in this topic. Note that given target_schema
    is expected as Avro json object.
    """
    topic = get_latest_topic_of_namespace_source(namespace, source)
    if not topic:
        return True
    return is_schema_compatible_in_topic(target_schema, topic.name)


def get_schemas_by_topic_name(topic_name, include_disabled=False):
    qry = session.query(
        models.AvroSchema
    ).join(
        models.Topic
    ).filter(
        models.Topic.id == models.AvroSchema.topic_id,
        models.Topic.name == topic_name
    )
    if not include_disabled:
        qry = qry.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )
    return qry.order_by(models.AvroSchema.id).all()


def get_schemas_by_topic_id(topic_id, include_disabled=False):
    """Get all the Avro schemas of specified topic. Default it excludes
    disabled schemas. Set `include_disabled` to True to include disabled ones.
    """
    qry = session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic_id
    )
    if not include_disabled:
        qry = qry.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )
    return qry.order_by(models.AvroSchema.id).all()


def mark_schema_disabled(schema_id):
    """Disable the Avro schema of specified id.
    """
    _update_schema_status(schema_id, models.AvroSchemaStatus.DISABLED)


def mark_schema_readonly(schema_id):
    """Mark the Avro schema of specified id as read-only.
    """
    _update_schema_status(schema_id, models.AvroSchemaStatus.READ_ONLY)


def _update_schema_status(schema_id, status):
    session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.id == schema_id
    ).update(
        {'status': status}
    )
    session.flush()


def get_sources():
    return session.query(models.Source).order_by(models.Source.id).all()


def get_namespaces():
    result = session.query(models.Namespace.name).distinct().all()
    return [namespace for (namespace,) in result]


def get_sources_by_namespace(namespace_name):
    return session.query(
        models.Source
    ).join(
        models.Namespace
    ).filter(
        models.Source.namespace_id == models.Namespace.id,
        models.Namespace.name == namespace_name
    ).order_by(
        models.Source.id
    ).all()


def get_topics_by_source_id(source_id):
    return session.query(
        models.Topic
    ).filter(
        models.Topic.source_id == source_id
    ).order_by(
        models.Topic.id
    ).all()


def get_namespace_by_id(namespace_id):
    return session.query(
        models.Namespace
    ).filter(
        models.Namespace.id == namespace_id
    ).first()


def get_source_by_id(source_id):
    return session.query(
        models.Source
    ).filter(
        models.Source.id == source_id
    ).first()


def get_latest_topic_of_source_id(source_id):
    return session.query(
        models.Topic
    ).filter(
        models.Topic.source_id == source_id
    ).order_by(
        models.Topic.id.desc()
    ).first()
