# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import uuid

import simplejson
from sqlalchemy import desc
from sqlalchemy import exc
from sqlalchemy.orm import exc as orm_exc

from schematizer import models
from schematizer.components.converters.converter_base import BaseConverter
from schematizer.logic import exceptions as sch_exc
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
    return (is_backward_compatible(old_schema_json, new_schema_json) and
            is_forward_compatible(old_schema_json, new_schema_json))


def _load_converters():
    __import__(
        'schematizer.components.converters',
        fromlist=[str('converters')]
    )
    _converters = dict()
    for cls in BaseConverter.__subclasses__():
        _converters[(cls.source_type, cls.target_type)] = cls
    return _converters


converters = _load_converters()


def convert_schema(source_type, target_type, source_schema):
    """Convert the source type schema to the target type schema. The
    source_type and target_type are the SchemaKindEnum.
    """
    converter = converters.get((source_type, target_type))
    if not converter:
        raise Exception("Unable to find converter to convert from {0} to {1}."
                        .format(source_type, target_type))
    return converter().convert(source_schema)


def register_avro_schema_from_avro_json(
        avro_schema_json,
        namespace_name,
        source_name,
        source_email_owner,
        contains_pii,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None
):
    """Add an Avro schema of given schema json object into schema store.
    The steps from checking compatibility to create new topic should be atomic.

    :param avro_schema_json: JSON representation of Avro schema
    :param namespace: namespace string
    :param source: source name string
    :param domain_owner_email: email of the schema owner
    :param status: AvroStatusEnum: RW/R/Disabled
    :param base_schema_id: Id of the Avro schema from which the new schema is
    derived from
    :return: New created AvroSchema object.
    """
    is_valid, error = models.AvroSchema.verify_avro_schema(avro_schema_json)
    if not is_valid:
        raise ValueError("Invalid Avro schema JSON. Value: {0}. Error: {1}"
                         .format(avro_schema_json, error))

    namespace = _get_namespace_or_create(namespace_name)
    _lock_namespace(namespace)

    source = _get_source_or_create(
        namespace.id,
        source_name,
        source_email_owner
    )
    _lock_source(source)

    topic_candidates = _get_topic_candidates(
        source_id=source.id,
        base_schema_id=base_schema_id,
        contains_pii=contains_pii,
        limit=None if base_schema_id else 1
    )

    for topic in topic_candidates:
        _lock_topic_and_schemas(topic)
        latest_schema = get_latest_schema_by_topic_id(topic.id)
        if _is_same_schema(
            schema=latest_schema,
            avro_schema_json=avro_schema_json,
            base_schema_id=base_schema_id
        ):
            return latest_schema

    most_recent_topic = topic_candidates[0] if topic_candidates else None
    if not _is_topic_compatible(
        topic=most_recent_topic,
        avro_schema_json=avro_schema_json,
        contains_pii=contains_pii
    ):
        most_recent_topic = _create_topic_for_source(
            namespace_name=namespace_name,
            source=source,
            contains_pii=contains_pii
        )
    return _create_avro_schema(
        avro_schema_json=avro_schema_json,
        topic_id=most_recent_topic.id,
        status=status,
        base_schema_id=base_schema_id
    )


def _is_same_schema(schema, avro_schema_json, base_schema_id):
    return (schema and
            schema.avro_schema_json == avro_schema_json and
            schema.base_schema_id == base_schema_id)


def _is_topic_compatible(topic, avro_schema_json, contains_pii):
    return (topic and
            topic.contains_pii == contains_pii and
            is_schema_compatible_in_topic(avro_schema_json, topic.name) and
            _is_pkey_identical(avro_schema_json, topic.name))


def _create_topic_for_source(namespace_name, source, contains_pii):
    # Note that creating duplicate topic names will throw a sqlalchemy
    # IntegrityError exception. When it occurs, it indicates the uuid
    # is generating the same value (rarely) and we'd like to know it.
    topic_name = _construct_topic_name(namespace_name, source.name)
    return _create_topic(topic_name, source.id, contains_pii)


def _construct_topic_name(namespace, source):
    return '.'.join((namespace, source, uuid.uuid4().hex))


def _create_topic(topic_name, source_id, contains_pii):
    """Create a topic named `topic_name` in the given source.
    It returns a newly created topic. If a topic with the same
    name already exists, an exception is thrown
    """
    topic = models.Topic(
        name=topic_name,
        source_id=source_id,
        contains_pii=contains_pii
    )
    session.add(topic)
    session.flush()
    return topic


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
    return session.query(
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
    source = get_source_by_fullname(namespace_name, source_name)
    if not source:
        raise sch_exc.EntityNotFoundException(
            "Cannot find namespace {0} source {1}.".format(
                namespace_name,
                source_name
            )
        )
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


def _get_topic_candidates(
        source_id,
        base_schema_id,
        contains_pii,
        limit=None,
        enabled_schemas_only=True
):
    """ Get topic candidate(s) for the given args, in order of creation (newest
    first).

    :param int source_id: The source_id of the topic(s)
    :param int|None base_schema_id: The base_schema_id of the schema(s) in the
        topic(s). Note that this may be None, as is the case for any schemas
        not derived from other schemas.
    :param bool contains_pii: Limit to topics which either do or do not
        contain PII. Defaults to None, which will not apply any filter.
    :param int|None limit: Provide a limit to the number of topics returned.
    :param bool enabled_schemas_only: Set to True to limit results to schemas
        which have not been disabled
    :rtype: [schematizer.models.Topic]
    """
    query = session.query(
        models.Topic
    ).join(
        models.AvroSchema
    ).filter(
        models.Topic.source_id == source_id,
        models.Topic._contains_pii == int(contains_pii),
        models.AvroSchema.base_schema_id == base_schema_id
    )
    if enabled_schemas_only:
        query = query.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )
    query = query.order_by(
        models.Topic.id.desc()
    )
    if limit:
        query = query.limit(limit)
    return query.all()


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


def _is_pkey_identical(new_schema_json, topic_name):
    """Check whether given schema has not mutated any primary key.
    """
    old_schema_json = get_latest_schema_by_topic_name(
        topic_name
    ).avro_schema_json
    old_pkey_set = set(
        (old_field['name'], old_field['pkey'])
        for old_field in old_schema_json.get('fields', [])
        if old_field.get('pkey')
    )
    new_pkey_set = set(
        (new_field['name'], new_field['pkey'])
        for new_field in new_schema_json.get('fields', [])
        if new_field.get('pkey')
    )
    return old_pkey_set == new_pkey_set


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
    avro_schema_elements = models.AvroSchema.create_schema_elements_from_json(
        avro_schema_json
    )

    avro_schema = models.AvroSchema(
        avro_schema_json=avro_schema_json,
        topic_id=topic_id,
        status=status,
        base_schema_id=base_schema_id
    )
    session.add(avro_schema)
    session.flush()

    for avro_schema_element in avro_schema_elements:
        avro_schema_element.avro_schema_id = avro_schema.id
        session.add(avro_schema_element)

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


def get_schemas_created_after(created_after):
    """Get the Avro schemas created after the specified creation_date.

    Args:
        creation_date(datetime): get schemas created after given utc
            datetime (inclusive).
    Returns:
        (list[:class:schematizer.models.AvroSchema]): List of avro
            schemas created after (inclusive) the specified creation
            date.
    """
    return session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.created_at >= created_after
    ).all()


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
    topic = get_topic_by_name(topic_name)
    if not topic:
        raise sch_exc.EntityNotFoundException(
            "Cannot find topic {0}.".format(topic_name)
        )

    return session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic.id,
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
    topic = get_topic_by_name(topic_name)
    if not topic:
        raise sch_exc.EntityNotFoundException(
            'Cannot find topic {0}.'.format(topic_name)
        )

    qry = session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.topic_id == topic.id
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


def get_schemas_by_criteria(namespace_name, source_name=None):
    """Get all avro schemas of specified namespace, optionally filtering
    by source name.
    """
    qry = session.query(
        models.AvroSchema
    ).join(
        models.Topic,
        models.Source,
        models.Namespace
    ).filter(
        models.AvroSchema.topic_id == models.Topic.id,
        models.Topic.source_id == models.Source.id,
        models.Source.namespace_id == models.Namespace.id,
        models.Namespace.name == namespace_name
    )
    if source_name:
        qry = qry.filter(
            models.Source.name == source_name
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
    return session.query(models.Namespace).order_by(models.Namespace.id).all()


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


def list_refreshes_by_source_id(source_id):
    return session.query(
        models.Refresh
    ).filter(
        models.Refresh.source_id == source_id
    ).order_by(
        models.Refresh.id
    ).all()


def create_refresh(
        source_id,
        offset,
        batch_size,
        priority,
        filter_condition,
        avg_rows_per_second_cap
):
    priority_value = None if not priority else models.Priority[priority].value
    refresh = models.Refresh(
        source_id=source_id,
        offset=offset,
        batch_size=batch_size,
        priority=priority_value,
        filter_condition=filter_condition,
        avg_rows_per_second_cap=avg_rows_per_second_cap
    )
    session.add(refresh)
    session.flush()
    return refresh


def get_schema_element_by_id(schema_id):
    return session.query(
        models.AvroSchemaElement
    ).filter(
        models.AvroSchemaElement.id == schema_id
    ).first()


def get_schema_elements_by_schema_id(schema_id):
    return session.query(
        models.AvroSchemaElement
    ).filter(
        models.AvroSchemaElement.avro_schema_id == schema_id
    ).order_by(
        models.AvroSchemaElement.id
    ).all()


def get_topics_by_criteria(
    namespace=None,
    source=None,
    created_after=None,
    count=None,
    min_id=None
):
    """Get all the topics that match given filter criteria.

    Args:
        namespace(Optional[str]): get topics of given namespace if specified
        source(Optional[str]): get topics of given source name if specified
        created_after(Optional[datetime]): get topics created after given utc
            datetime (inclusive) if specified.
        count(Optional[int]): number of topics to return in this query
        min_id(Optional[int]): limits results to those with an id greater than
            or equal to given id.
    Returns:
        (list[:class:schematizer.models.Topic]): List of topic models sorted
        by their ids.
    """
    qry = session.query(models.Topic)
    if namespace or source:
        qry = qry.join(models.Source).filter(
            models.Source.id == models.Topic.source_id
        )
    if namespace:
        qry = qry.join(models.Namespace).filter(
            models.Namespace.name == namespace,
            models.Namespace.id == models.Source.namespace_id,
        )
    if source:
        qry = qry.filter(models.Source.name == source)
    if created_after:
        qry = qry.filter(models.Topic.created_at >= created_after)
    if min_id:
        qry = qry.filter(models.Topic.id >= min_id)
    qry = qry.order_by(models.Topic.id)
    if count:
        qry = qry.limit(count)
    return qry.all()


def get_refreshes_by_criteria(
    namespace=None,
    source_name=None,
    status=None,
    created_after=None
):
    """Get all the refreshes that match the given filter criteria.

    Args:
        namespace(Optional[str]): get refreshes of given namespace
            if specified.
        source_name(Optional[str]): get refreshes of given source
            if specified.
        status(Optional[int]): get refreshes of given status
            if specified.
        created_after(Optional[datetime]): get refreshes created
            after given utc datetime (inclusive) if specified.
    """
    qry = session.query(models.Refresh)
    if namespace:
        qry = qry.join(models.Source).filter(
            models.Source.id == models.Refresh.source_id
        )
        qry = qry.join(models.Namespace).filter(
            models.Namespace.name == namespace,
            models.Namespace.id == models.Source.namespace_id
        )
    if source_name:
        qry = qry.join(models.Source).filter(
            models.Source.id == models.Refresh.source_id,
            models.Source.name == source_name
        )
    if status:
        status = models.RefreshStatus[status].value
        qry = qry.filter(models.Refresh.status == status)
    if created_after:
        qry = qry.filter(models.Refresh.created_at >= created_after)
    return qry.order_by(
        desc(models.Refresh.priority)
    ).order_by(
        models.Refresh.id
    ).all()


def get_refresh_by_id(refresh_id):
    return session.query(
        models.Refresh
    ).filter(
        models.Refresh.id == refresh_id
    ).first()


def update_refresh(refresh_id, status, offset):
    status_value = models.RefreshStatus[status].value
    return session.query(
        models.Refresh
    ).filter(
        models.Refresh.id == refresh_id
    ).update(
        {
            models.Refresh.status: status_value,
            models.Refresh.offset: offset
        }
    )
