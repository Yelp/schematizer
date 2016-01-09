# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer import models
from schematizer.models.database import session


def create_data_target(target_type, destination):
    """Create a new data target of specified target type and destination.

    Args:
        target_type (string): string describing the type of the data target
        destination (string): the actual location of the data target, such as
            the url of the cluster.

    Returns:
        :class:models.data_target.DataTarget: the created data target.

    Raises:
        ValueError: if given target type or destination is empty.
    """
    if not target_type:
        raise ValueError("data target type cannot be empty.")
    if not destination:
        raise ValueError("destination of data target cannot be empty.")

    data_target = models.DataTarget(
        target_type=target_type,
        destination=destination
    )
    session.add(data_target)
    session.flush()
    return data_target


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
        ValueError: if group name is empty
        :class:schematizer.models.exceptions.EntityNotFoundError: if specified
            data target id is not found.

    """
    if not group_name:
        raise ValueError("consumer group name cannot be empty.")

    # Check if the data target id exists by trying to get the object. If it
    # doesn't exist, the EntityNotFoundException will be thrown.
    models.DataTarget.get_by_id(session, data_target_id)

    consumer_group = models.ConsumerGroup(
        group_name=group_name,
        data_target_id=data_target_id
    )
    session.add(consumer_group)
    session.flush()
    return consumer_group


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
        models.DataTarget.get_by_id(session, data_target_id)
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
    # Verify if the consumer group exists by trying to retrieving it
    models.ConsumerGroup.get_by_id(session, consumer_group_id)

    # Verify if the data source exists
    _validate_data_source_exists(data_source_type, data_source_id)

    consumer_grp_data_src = models.ConsumerGroupDataSource(
        consumer_group_id=consumer_group_id,
        data_source_type=data_source_type,
        data_source_id=data_source_id
    )
    session.add(consumer_grp_data_src)
    session.flush()
    return consumer_grp_data_src


def _validate_data_source_exists(data_src_type, data_src_id):
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
    data_model_map = {
        models.DataSourceTypeEnum.NAMESPACE: models.Namespace,
        models.DataSourceTypeEnum.SOURCE: models.Source
    }

    data_model = data_model_map.get(data_src_type)
    if not data_model:
        raise ValueError(
            "Invalid data source type {}. It should be one of {}."
            .format(data_src_type, models.DataSourceTypeEnum.__name__)
        )

    # Check if the data target id exists by trying to get the object. If it
    # doesn't exist, the EntityNotFoundException will be thrown.
    data_model.get_by_id(session, data_src_id)


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
        # Check if the data_target_id exists and throw exception if not
        models.DataTarget.get_by_id(session, data_target_id)

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
