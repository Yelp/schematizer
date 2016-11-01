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
from schematizer.environment_configs import FORCE_AVOID_INTERNAL_PACKAGES
from schematizer.logic import exceptions as sch_exc
from schematizer.logic import meta_attribute_mappers as meta_attr_logic
from schematizer.logic.schema_resolution import SchemaCompatibilityValidator
from schematizer.models.database import session
from schematizer.models.schema_meta_attribute_mapping import (
    SchemaMetaAttributeMapping)

try:
    # TODO(DATAPIPE-1506|abrar): Currently we have
    # force_avoid_internal_packages as a means of simulating an absence
    # of a yelp's internal package. And all references
    # of force_avoid_internal_packages have to be removed from
    # schematizer after we have completely ready for open source.
    if FORCE_AVOID_INTERNAL_PACKAGES:
        raise ImportError
    from yelp_conn.mysqldb import IntegrityError
except ImportError:
    from sqlalchemy.exc import IntegrityError


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
        cluster_type,
        status=models.AvroSchemaStatus.READ_AND_WRITE,
        base_schema_id=None,
        docs_required=True
):
    """Add an Avro schema of given schema json object into schema store.
    The steps from checking compatibility to create new topic should be atomic.

    :param avro_schema_json: JSON representation of Avro schema
    :param namespace: namespace string
    :param source: source name string
    :param domain_owner_email: email of the schema owner
    :param cluster_type: Type of kafka cluster Ex: datapipe, scribe, etc.
        See http://y/datapipe_cluster_types for more info on cluster_types.
    :param status: AvroStatusEnum: RW/R/Disabled
    :param base_schema_id: Id of the Avro schema from which the new schema is
        derived from
    :param docs_required: whether to-be-registered schema must contain doc
        strings
    :return: New created AvroSchema object.
    """

    source_email_owner = _strip_if_not_none(source_email_owner)
    source_name = _strip_if_not_none(source_name)

    _assert_non_empty_email(source_email_owner)
    _assert_non_empty_src_name(source_name)

    is_valid, error = models.AvroSchema.verify_avro_schema(avro_schema_json)
    if not is_valid:
        raise ValueError("Invalid Avro schema JSON. Value: {0}. Error: {1}"
                         .format(avro_schema_json, error))

    if docs_required:
        models.AvroSchema.verify_avro_schema_has_docs(
            avro_schema_json
        )

    namespace = _get_namespace_or_create(namespace_name)
    _lock_namespace(namespace)

    source = _get_source_or_create(
        namespace.id,
        source_name.strip(),
        source_email_owner.strip()
    )
    _lock_source(source)

    topic_candidates = _get_topic_candidates(
        source_id=source.id,
        base_schema_id=base_schema_id,
        contains_pii=contains_pii,
        cluster_type=cluster_type,
        limit=None if base_schema_id else 1
    )

    required_meta_attr_ids = meta_attr_logic.get_meta_attributes_by_source(
        source.id
    )
    for topic in topic_candidates:
        _lock_topic_and_schemas(topic)
        latest_schema = get_latest_schema_by_topic_id(topic.id)
        if _is_same_schema(
            schema=latest_schema,
            avro_schema_json=avro_schema_json,
            base_schema_id=base_schema_id,
            required_meta_attr_ids=required_meta_attr_ids
        ):
            return latest_schema

    most_recent_topic = topic_candidates[0] if topic_candidates else None
    if not _is_candidate_topic_compatible(
        topic=most_recent_topic,
        avro_schema_json=avro_schema_json,
        contains_pii=contains_pii
    ):
        most_recent_topic = _create_topic_for_source(
            namespace_name=namespace_name,
            source=source,
            contains_pii=contains_pii,
            cluster_type=cluster_type
        )
    return _create_avro_schema(
        avro_schema_json=avro_schema_json,
        source_id=source.id,
        topic_id=most_recent_topic.id,
        status=status,
        base_schema_id=base_schema_id
    )


def _strip_if_not_none(original_str):
    if not original_str:
        return original_str
    return original_str.strip()


def _is_same_schema(
    schema,
    avro_schema_json,
    base_schema_id,
    required_meta_attr_ids
):
    return (schema and
            schema.avro_schema_json == avro_schema_json and
            schema.base_schema_id == base_schema_id and
            _are_meta_attributes_same(schema.id, required_meta_attr_ids))


def _are_meta_attributes_same(schema_id, required_meta_attr_ids):
    return (set(get_meta_attributes_by_schema_id(schema_id)) ==
            set(required_meta_attr_ids))


def _is_candidate_topic_compatible(topic, avro_schema_json, contains_pii):
    return (topic and
            topic.contains_pii == contains_pii and
            is_schema_compatible_in_topic(avro_schema_json, topic) and
            _is_pkey_identical(avro_schema_json, topic.name))


def _create_topic_for_source(
    namespace_name,
    source,
    contains_pii,
    cluster_type
):
    # Note that creating duplicate topic names will throw a sqlalchemy
    # IntegrityError exception. When it occurs, it indicates the uuid
    # is generating the same value (rarely) and we'd like to know it.
    # Per SEC-5079, sqlalchemy IntegrityError now is replaced with yelp-conn
    # IntegrityError.
    topic_name = _construct_topic_name(namespace_name, source.name)
    return _create_topic(topic_name, source.id, contains_pii, cluster_type)


def _construct_topic_name(namespace, source):
    return '__'.join((namespace, source, uuid.uuid4().hex))


def _create_topic(topic_name, source_id, contains_pii, cluster_type):
    """Create a topic named `topic_name` in the given source.
    It returns a newly created topic. If a topic with the same
    name already exists, an exception is thrown
    """
    topic = models.Topic(
        name=topic_name,
        source_id=source_id,
        contains_pii=contains_pii,
        cluster_type=cluster_type
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
    except (IntegrityError, exc.IntegrityError):
        # Ignore this error due to trying to create a duplicate namespace
        # TODO [clin|DATAPIPE-1471] see if there is a way to only handle one
        # exception or the other.
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
    except (IntegrityError, exc.IntegrityError):
        # Ignore this error due to trying to create a duplicate source
        # TODO [clin|DATAPIPE-1471] see if there is a way to only handle one
        # exception or the other.
        new_source = _get_source_by_namespace_id_and_src_name(
            namespace_id,
            source_name
        )
    return new_source


def _assert_non_empty_email(email):
    if not email or email.strip() == "":
        raise ValueError("Source owner email must be non-empty.")


def _assert_non_empty_src_name(name):
    if not name or name.strip() == "":
        raise ValueError("Source name must be non-empty.")


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
    cluster_type,
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
    :param string cluster_type : Limit to topics of same cluster type.
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
        models.Topic.cluster_type == cluster_type,
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


def is_schema_compatible_in_topic(target_schema, topic):
    """Check whether given schema is a valid Avro schema and compatible
    with existing schemas in the specified topic. Note that target_schema
    is the avro json object.
    """
    required_meta_attr_ids = meta_attr_logic.get_meta_attributes_by_source(
        topic.source_id
    )
    enabled_schemas = get_schemas_by_topic_name(topic.name)
    for enabled_schema in enabled_schemas:
        schema_json = simplejson.loads(enabled_schema.avro_schema)
        if (not is_full_compatible(schema_json, target_schema) or
            not _are_meta_attributes_same(
                enabled_schema.id,
                required_meta_attr_ids
        )):
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
    source_id,
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
    _add_meta_attribute_mappings(avro_schema.id, source_id)
    return avro_schema


def get_schema_by_id(schema_id):
    """Get the Avro schema of specified id. It returns None if not found.
    """
    return session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.id == schema_id
    ).first()


def get_schemas_created_after(
    created_after,
    page_info=None,
    include_disabled=False
):
    # TODO [clin|DATAPIPE-1430] as part of the clean up, merge this function
    # into `get_schemas_by_criteira`.
    """ Get the Avro schemas (excluding disabled schemas) created after the
    specified created_after timestamp and with id greater than or equal to
    the min_id. Limits the returned schemas to count. Default it excludes
    disabled schemas.

    Args:
        created_after(datetime): get schemas created after given utc
            datetime (inclusive).
        page_info(Optional[:class:schematizer.models.tuples.PageInfo]):
            limits the schemas to count and those with an id greater than or
            equal to min_id.
        include_disabled(Optional[bool]): set it to True to include disabled
            schemas. Default it excludes disabled ones.
    Returns:
        (list[:class:schematizer.models.AvroSchema]): List of avro
            schemas created after (inclusive) the specified creation
            date.
    """
    qry = session.query(
        models.AvroSchema
    ).filter(
        models.AvroSchema.created_at >= created_after,
    )
    if not include_disabled:
        qry = qry.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )
    if page_info and page_info.min_id:
        qry = qry.filter(
            models.AvroSchema.id >= page_info.min_id
        )
    qry = qry.order_by(models.AvroSchema.id)
    if page_info and page_info.count:
        qry = qry.limit(page_info.count)
    return qry.all()


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
    return is_schema_compatible_in_topic(target_schema, topic)


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


def get_topics_by_source_id(source_id):
    return session.query(
        models.Topic
    ).filter(
        models.Topic.source_id == source_id
    ).order_by(
        models.Topic.id
    ).all()


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
    refresh = models.Refresh(
        source_id=source_id,
        offset=offset,
        batch_size=batch_size,
        priority=priority,
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


def get_meta_attributes_by_schema_id(schema_id):
    """Logic Method to list the schema_ids of all meta attributes registered to
    the specified schema id. Invalid schema id will raise an
    EntityNotFoundError exception"""
    models.AvroSchema.get_by_id(schema_id)
    mappings = session.query(
        SchemaMetaAttributeMapping
    ).filter(
        SchemaMetaAttributeMapping.schema_id == schema_id
    ).all()
    return [m.meta_attr_schema_id for m in mappings]


def _add_meta_attribute_mappings(schema_id, source_id):
    mappings = []
    for meta_attr_schema_id in meta_attr_logic.get_meta_attributes_by_source(
        source_id
    ):
        new_mapping = SchemaMetaAttributeMapping(
            schema_id=schema_id,
            meta_attr_schema_id=meta_attr_schema_id
        )
        session.add(new_mapping)
        mappings.append(new_mapping)
    session.flush()
    return mappings


def get_topics_by_criteria(
    namespace=None,
    source=None,
    created_after=None,
    page_info=None
):
    """Get all the topics that match given criteria, including namespace,
    source, and/or topic created timestamp.

    This function supports pagination, i.e. caller can specify miniumum topic
    id and page size to get single chunk of topics.

    Args:
        namespace(Optional[str]): get topics of given namespace if specified
        source(Optional[str]): get topics of given source name if specified
        created_after(Optional[datetime]): get topics created after given utc
            datetime (inclusive) if specified.
        page_info(Optional[:class:schematizer.models.page_info.PageInfo]):
            limits the topics to count and those with id greater than or
            equal to min_id.

    Returns:
        (list[:class:schematizer.models.Topic]): List of topics sorted by
        their ids.
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
    if created_after is not None:
        qry = qry.filter(models.Topic.created_at >= created_after)

    min_id = page_info.min_id if page_info else 0
    qry = qry.filter(models.Topic.id >= min_id)

    qry = qry.order_by(models.Topic.id)
    if page_info and page_info.count:
        qry = qry.limit(page_info.count)
    return qry.all()


def get_schemas_by_criteria(
    namespace_name=None,
    source_name=None,
    created_after=None,
    include_disabled=False,
    page_info=None
):
    """Get avro schemas that match the specified criteria, including namespace,
    source, schema created timestamp, and/or schema status.

    This function supports pagination, i.e. caller can specify minimum schema
    id and page size to get single chunk of schemas.

    Args:
        namespace(Optional[str]): get schemas of given namespace if specified
        source(Optional[str]): get schemas of given source name if specified
        created_after(Optional[datetime]): get schemas created after given utc
            datetime (inclusive) if specified
        included_disabled(Optional[bool]): whether to include disabled schemas
        page_info(Optional[:class:schematizer.models.page_info.PageInfo]):
            limits the topics to count and those with id greater than or
            equal to min_id.

    Returns:
        (list[:class:schematizer.models.AvroSchema]): List of avro schemas
        sorted by their ids.
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
        qry = qry.filter(models.Source.name == source_name)
    if created_after is not None:
        qry = qry.filter(models.AvroSchema.created_at >= created_after)

    if not include_disabled:
        qry = qry.filter(
            models.AvroSchema.status != models.AvroSchemaStatus.DISABLED
        )

    min_id = page_info.min_id if page_info else 0
    qry = qry.filter(models.AvroSchema.id >= min_id)

    qry = qry.order_by(models.AvroSchema.id)
    if page_info and page_info.count:
        qry = qry.limit(page_info.count)

    return qry.all()


def get_refreshes_by_criteria(
    namespace=None,
    source_name=None,
    status=None,
    created_after=None,
    updated_after=None
):
    """Get all the refreshes that match the given filter criteria.

    Args:
        namespace(Optional[str]): get refreshes of given namespace
            if specified.
        source_name(Optional[str]): get refreshes of given source
            if specified.
        status(Optional[str]): get refreshes of given status
            if specified.
        created_after(Optional[datetime]): get refreshes created
            after given utc datetime (inclusive) if specified.
        updated_after(Optional[datetime]): get refreshes updated
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
        qry = qry.filter(models.Refresh.status == status)
    if created_after:
        qry = qry.filter(models.Refresh.created_at >= created_after)
    if updated_after:
        qry = qry.filter(models.Refresh.updated_at >= updated_after)
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
    return session.query(
        models.Refresh
    ).filter(
        models.Refresh.id == refresh_id
    ).update(
        {
            models.Refresh.status: status,
            models.Refresh.offset: offset
        }
    )
