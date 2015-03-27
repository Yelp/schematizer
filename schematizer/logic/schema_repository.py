# -*- coding: utf-8 -*-
import uuid

import simplejson
from sqlalchemy import exc

from schematizer import models
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


converters_module = __import__(
    'schematizer.components.converters',
    fromlist=['converters']
)
# TODO[clin|...] setup converter from config
mysql_converter = getattr(converters_module, 'MySqlConverter')


def create_avro_schema_from_mysql(
        sqls,
        namespace,
        source,
        domain_email_owner,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    """Add an Avro schema generated from given MySQL statement(s) into
    the schema store.
    """
    return create_avro_schema_from_avro_json(
        mysql_converter.convert_to_avro(sqls),
        namespace,
        source,
        domain_email_owner,
        status,
        base_schema_id
    )


def create_avro_schema_from_avro_json(
        avro_schema_json,
        namespace,
        source,
        domain_email_owner,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    """Add an Avro schema of given schema json object into schema store.

    The steps from checking compatibility to create new topic should be
    atomic or idempotent.
    """
    # TODO[clin|...] Add necessary lock table to ensure atomic operation
    topic = get_latest_topic_of_domain(namespace, source)
    if (not topic or not is_schema_compatible_in_topic(
            avro_schema_json,
            topic.topic
    )):
        topic_name = _construct_topic_name(namespace, source)
        try:
            topic = create_topic(
                topic_name,
                namespace,
                source,
                domain_email_owner
            )
        except exc.IntegrityError:
            # topic_name already exists; simply use the existing one
            topic = get_topic_by_name(topic_name)
    avro_schema = _create_avro_schema(
        avro_schema_json,
        topic.id,
        status,
        base_schema_id
    )
    return avro_schema


def get_latest_topic_of_domain(namespace, source):
    """Get the latest topic of given namespace and source. The latest one is
    the one created most recently. It returns None if no such topic exists.
    """
    return session.query(
        models.Topic
    ).join(
        models.Domain
    ).filter(
        models.Domain.id == models.Topic.domain_id,
        models.Domain.namespace == namespace,
        models.Domain.source == source
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


def create_topic(topic_name, namespace, source, domain_owner_email):
    """Create a topic named `topic_name` in the domain of given namespace
    and source. It returns newly created topic. If a topic with same name
    already exists, an exception is thrown.
    """
    try:
        domain = (get_domain_by_fullname(namespace, source)
                  or create_domain(namespace, source, domain_owner_email))
    except exc.IntegrityError:
        # Ignore this error due to trying to create a duplicate domain
        # (same namespace and source). Simply get the existing one.
        domain = get_domain_by_fullname(namespace, source)

    topic = models.Topic(topic=topic_name, domain_id=domain.id)
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
        models.Topic.topic == topic_name
    ).first()


def get_domain_by_fullname(namespace, source):
    """Get the domain object of specified namespace and source. It returns
    None if no such domain exists.
    """
    return session.query(
        models.Domain
    ).filter(
        models.Domain.namespace == namespace,
        models.Domain.source == source
    ).first()


def create_domain(namespace, source, owner_email):
    """Create a domain of specified namespace and source. The `owner_email` is
    the email of whoever owns this domain. If a domain with same namespace and
    source already exists in the table, an IntegrityError is thrown.
    """
    domain = models.Domain(
        namespace=namespace,
        source=source,
        owner_email=owner_email
    )
    session.add(domain)
    session.flush()
    return domain


def _create_avro_schema(
        avro_schema_json,
        topic_id,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    avro_schema = models.AvroSchema(
        avro_schema=simplejson.dumps(avro_schema_json),
        topic_id=topic_id,
        status=status,
        base_schema_id=base_schema_id
    )
    session.add(avro_schema)
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


def validate_schema(target_schema, namespace, source):
    """Check whether given schema is a valid Avro schema. It then determines
    the topic of given Avro schema belongs to and checks the compatibility
    against the existing schemas in this topic. Note that given target_schema
    is expected as Avro json object.
    """
    topic = get_latest_topic_of_domain(namespace, source)
    if not topic:
        return True
    return is_schema_compatible_in_topic(target_schema, topic.topic)


def get_schemas_by_topic_name(topic_name, include_disabled=False):
    qry = session.query(
        models.AvroSchema
    ).join(
        models.Topic
    ).filter(
        models.Topic.id == models.AvroSchema.topic_id,
        models.Topic.topic == topic_name
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
    session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.id == schema_id
    ).update(
        {'status': models.AvroSchemaStatus.DISABLED}
    )
    session.flush()


def mark_schema_readonly(schema_id):
    """Mark the Avro schema of specified id as read-only.
    """
    session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.id == schema_id
    ).update(
        {'status': models.AvroSchemaStatus.READ_ONLY}
    )
    session.flush()


def get_domains():
    return session.query(models.Domain).order_by(models.Domain.id).all()


def get_namespaces():
    """Return a list of namespace strings"""
    result = session.query(models.Domain.namespace).distinct().all()
    return [namespace for (namespace,) in result]


def get_domains_by_namespace(namespace):
    return session.query(
        models.Domain
    ).filter(
        models.Domain.namespace == namespace
    ).order_by(
        models.Domain.id
    ).all()


def get_topics_by_domain_id(domain_id):
    return session.query(
        models.Topic
    ).filter(
        models.Topic.domain_id == domain_id
    ).order_by(
        models.Topic.id
    ).all()
