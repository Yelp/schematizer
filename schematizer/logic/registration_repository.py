# -*- coding: utf-8 -*-
"""
This module contains the logic that manages producers, consumers, and related
data, such as data sources and data targets.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import and_
from sqlalchemy import exc
from sqlalchemy import or_

from schematizer import models
from schematizer.environment_configs import FORCE_AVOID_INTERNAL_PACKAGES
from schematizer.logic.validators import verify_entity_exists
from schematizer.logic.validators import verify_truthy_value
from schematizer.models.consumer_group_data_source \
    import DataSourceTypeEnum as SrcType
from schematizer.models.database import session

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


def create_data_target(name, target_type, destination):
    """Create a new data target of specified target type and destination.

    Args:
        name (string): name to uniquely identify a data target
        target_type (string): string describing the type of the data target
        destination (string): the actual location of the data target, such as
            the url of the cluster.

    Returns:
        :class:models.data_target.DataTarget: the created data target.

    Raises:
        ValueError: if given target type or destination is empty.
    """
    verify_truthy_value(name, "data target name")
    verify_truthy_value(target_type, "data target type")
    verify_truthy_value(destination, "destination of data target")

    return models.DataTarget.create(
        session,
        name=name,
        target_type=target_type,
        destination=destination
    )


def create_consumer_group(group_name, data_target_id):
    """Add a new consumer group that associates to given data target.

    Args:
        group_name (string): consumer group name. It should be unique among all
            the consumer groups.
        data_target_id (id): id of the data target which this consumer group
            associates to

    Returns:
        :class:models.consumer_group.ConsumerGroup: the created consumer group.

    Raises:
        ValueError: if group name is empty or already exists.
        :class:schematizer.models.exceptions.EntityNotFoundError: if specified
            data target id is not found.

    """
    verify_truthy_value(group_name, "consumer group name")
    verify_entity_exists(session, models.DataTarget, data_target_id)

    try:
        return models.ConsumerGroup.create(
            session,
            group_name=group_name,
            data_target_id=data_target_id
        )
    except (IntegrityError, exc.IntegrityError):
        # TODO [clin|DATAPIPE-1471] see if there is a way to only handle one
        # exception or the other.
        raise ValueError(
            "Consumer group {} already exists.".format(group_name)
        )


def get_consumer_groups_by_data_target_id(data_target_id):
    """Get the list of consumer groups that associate to the given data target.

    Args:
        data_target_id (int): Id of the data target

    Returns:
        List[:class: schematizer.models.consumer_group.ConsumerGroup]: List of
            consumer group objects.

    Raises:
        :class:schematizer.models.exceptions.EntityNotFoundError: if specified
            data target id is not found.
    """
    groups = session.query(models.ConsumerGroup).filter(
        models.ConsumerGroup.data_target_id == data_target_id
    ).all()
    if not groups:
        verify_entity_exists(session, models.DataTarget, data_target_id)
    return groups


def register_consumer_group_data_source(
    consumer_group_id,
    data_source_type,
    data_source_id
):
    """Assign the specified data source to the given consumer group.

    Args:
        consumer_group_id (int): :class:models.consumer_group.ConsumerGroup
            object Id.
        data_src_type (
            :class:models.consumer_group_data_source.DataSourceTypeEnum) data
            source type
        data_src_id (int): Id of the specified data source type

    Returns:
        :class:models.consumer_group_data_source.ConsumerGroupDataSource:
            the newly created object.

    Raises:
        ValueError: if given data source type is invalid
        :class:schematizer.models.exceptions.EntityNotFoundError: if specified
            data source id is not found or consumer group id is not found.
    """
    verify_entity_exists(session, models.ConsumerGroup, consumer_group_id)
    _verify_data_source_exists(data_source_type, data_source_id)

    return models.ConsumerGroupDataSource.create(
        session,
        consumer_group_id=consumer_group_id,
        data_source_type=data_source_type,
        data_source_id=data_source_id
    )


def _verify_data_source_exists(data_src_type, data_src_id):
    """Check if the data source object of specified data source type and id
    exists.

    Args:
        data_src_type (
            :class:models.consumer_group_data_source.DataSourceTypeEnum) data
            source type
        data_src_id (int): Id of the specified data source type

    Returns:
        bool: returns True if the object exists.

    Raises:
        ValueError: if given data source type is invalid
        :class:schematizer.models.exceptions.EntityNotFoundError: if specified
            data source id is not found.
    """
    data_src_type_to_cls_map = {
        models.DataSourceTypeEnum.NAMESPACE: models.Namespace,
        models.DataSourceTypeEnum.SOURCE: models.Source
    }

    data_src_cls = data_src_type_to_cls_map.get(data_src_type)
    if not data_src_cls:
        raise ValueError(
            "Invalid data source type {}. It should be one of {}."
            .format(data_src_type, models.DataSourceTypeEnum.__name__)
        )

    verify_entity_exists(session, data_src_cls, data_src_id)


def get_data_sources_by_consumer_group_id(consumer_group_id):
    """Get all the data sources that associate to the given consumer group.

    Args:
        consumer_group_id (int): Id of the consumer group.

    Returns:
        List[:class:schematizer.models.consumer_group_data_source
        .ConsumerGroupDataSource]: List of data sources associated to the
        given data target.

    Raises:
        :class:schematizer.models.exceptions.EntityNotFoundError: if specified
            consumer group id is not found.
    """
    data_srcs = session.query(models.ConsumerGroupDataSource).filter(
        models.ConsumerGroupDataSource.consumer_group_id == consumer_group_id
    ).all()

    if not data_srcs:
        verify_entity_exists(session, models.ConsumerGroup, consumer_group_id)

    return data_srcs


def _filter_consumer_group_data_src_by_namespace(namespace_id):
    return and_(
        models.ConsumerGroupDataSource.data_source_id == namespace_id,
        models.ConsumerGroupDataSource.data_source_type == SrcType.NAMESPACE
    )


def _filter_consumer_group_data_src_by_source(src_id):
    return and_(
        models.ConsumerGroupDataSource.data_source_id == src_id,
        models.ConsumerGroupDataSource.data_source_type == SrcType.SOURCE
    )


def _filter_consumer_group_data_src_by_schema(schema_id):
    return and_(
        models.ConsumerGroupDataSource.data_source_id == schema_id,
        models.ConsumerGroupDataSource.data_source_type == SrcType.SCHEMA
    )


def get_data_targets_by_schema_id(schema_id):
    """Get the data targets of the corresponding schema id.
    Since the the data source in ConsumerGroupDataSource can be a
    schema, namespace or source, this function first uses the schema_id to
    grab corresponding namespace, source id and use them to find
    the corresponding consumer group ids. It finally uses these consumer group
    ids to fetch the data targets.

    Returns:
        A list of unique data targets
    """
    avro_schema = models.AvroSchema.get_by_id(schema_id)
    src_id = avro_schema.topic.source.id
    namespace_id = avro_schema.topic.source.namespace_id

    results = session.query(
        models.ConsumerGroupDataSource.consumer_group_id
    ).filter(
        or_(
            _filter_consumer_group_data_src_by_namespace(namespace_id),
            _filter_consumer_group_data_src_by_source(src_id),
            _filter_consumer_group_data_src_by_schema(schema_id)
        )
    ).all()

    consumer_group_ids = set([id[0] for id in results])

    data_targets = session.query(
        models.DataTarget
    ).join(
        models.ConsumerGroup
    ).filter(
        models.DataTarget.id == models.ConsumerGroup.data_target_id,
        models.ConsumerGroup.id.in_(consumer_group_ids)
    ).all()

    return data_targets


def get_data_sources_by_data_target_id(data_target_id):
    """Get all the data sources that associate to the given data target.

    Args:
        data_target_id (int): Id of the data target.

    Returns:
        List[:class:schematizer.models.consumer_group_data_source
        .ConsumerGroupDataSource]: List of data sources associated to the
        given data target.

    Raises:
        :class:schematizer.models.exceptions.EntityNotFoundError: if specified
            data target id is not found.
    """
    data_srcs = session.query(models.ConsumerGroupDataSource).join(
        models.ConsumerGroup
    ).filter(
        models.ConsumerGroup.data_target_id == data_target_id,
        models.ConsumerGroup.id == (models.ConsumerGroupDataSource
                                    .consumer_group_id)
    ).all()

    if not data_srcs:
        verify_entity_exists(session, models.DataTarget, data_target_id)

    return data_srcs


def get_topics_by_data_target_id(data_target_id, created_after=None):
    """Get all the topics that associate to the given data target, and
    optionally filtered by topic creation timestamp.

    A data target may be associated to multiple consumer groups, and each
    consumer group may have multiple data sources (which could be a namespace
    or a source).  This function returns all the topics under all the data
    sources that link to the given data target.

    Args:
        data_target_id (int): data target id
        created_after(Optional[datetime]): get topics created after given utc
            datetime (inclusive) if specified.

    Returns:
        List[:class:schematizer.models.topic.Topic]: List of topic models
        sorted by their ids.
    """
    data_srcs = get_data_sources_by_data_target_id(data_target_id)
    source_ids = {
        data_src.data_source_id for data_src in data_srcs
        if data_src.data_source_type == models.DataSourceTypeEnum.SOURCE
    }

    namespace_ids = {
        data_src.data_source_id for data_src in data_srcs
        if data_src.data_source_type == models.DataSourceTypeEnum.NAMESPACE
    }
    if namespace_ids:
        sources = session.query(models.Source).filter(
            models.Source.namespace_id.in_(namespace_ids)
        ).all()
        source_ids.update(source.id for source in sources)

    if not source_ids:
        return []

    qry = session.query(models.Topic).join(models.Source).filter(
        models.Source.id.in_(source_ids),
        models.Source.id == models.Topic.source_id
    )
    if created_after:
        qry = qry.filter(models.Topic.created_at >= created_after)
    return qry.order_by(models.Topic.id).all()
