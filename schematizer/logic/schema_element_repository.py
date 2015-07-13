# -*- coding: utf-8 -*-
from schematizer import models
from schematizer.logic import exceptions as sch_exc
from schematizer.models.database import session


def get_element_chains_by_schema_id(schema_id):
    """Build the element chain for each schema element in the given schema.
    The elements in the same element chain represent the same entity, such
    as same field, same column, etc, from the previous version schemas of
    the same source.

    Note that if a schema that is newer than the given schema exists, the
    element chains will not include the elements from the newer schema.

    :param schema_id: the Avro schema Id.
    :return: List of element chains ([[schematizer.models.AvroSchemaElement]].
    Each element chain is a list of schema elements sorted by their timestamp
    reversely: the list starts with the most recent schema element, i.e. the
    element of given schema.
    """
    avro_schema = models.AvroSchema.get_by_id(schema_id)
    if not avro_schema:
        raise sch_exc.EntityNotFoundException(
            "Cannot find Avro schema id {0}.".format(schema_id)
        )

    identity_to_element_chain_map = _initialize_schema_element_chains(
        avro_schema.avro_schema_elements
    )
    elements = _get_schema_elements_by_source(
        avro_schema.topic.source_id,
        no_later_than_schema_id=schema_id
    )

    start_index = 0
    for i, element in enumerate(elements):
        if element.avro_schema_id != elements[start_index].avro_schema_id:
            _update_schema_element_chains(
                identity_to_element_chain_map,
                elements[start_index:i]
            )
            start_index = i
    _update_schema_element_chains(
        identity_to_element_chain_map,
        elements[start_index:]
    )
    return identity_to_element_chain_map.values()


def _initialize_schema_element_chains(elements):
    if not elements:
        return {}
    return dict((_get_schema_element_identity(e), [e]) for e in elements)


def _get_schema_element_identity(element):
    """The identity of an element is the value used to see if two schema
    elements represent the same entity (field, column, etc.).
    """
    return element.key


def _get_schema_elements_by_source(source_id, no_later_than_schema_id=None):
    """Get all the schema elements of the schemas that belong to specified
    source. If `no_later_than_schema_id` is specified, only the elements of
    the schemas that are earlier than the specified schema id will be returned.

    :param source_id:
    :param no_later_than_schema_id:
    :return: A list of schema elements sorted by the creation timestamp of
    their enclosed schema reversely, i.e. the list starts with the elements of
    the latest schema.
    """
    qry = session.query(models.AvroSchemaElement).join(
        models.AvroSchema,
        models.Topic,
    ).filter(
        models.AvroSchemaElement.avro_schema_id == models.AvroSchema.id,
        models.AvroSchema.topic_id == models.Topic.id,
        models.Topic.source_id == source_id
    )
    if no_later_than_schema_id:
        qry = qry.filter(models.AvroSchema.id < no_later_than_schema_id)
    qry = qry.order_by(models.AvroSchema.id.desc())

    return qry.all()


def _update_schema_element_chains(identity_to_element_chain_map, elements):
    """If the identity of an element in the given elements is the same as
    the key of the identity_to_element_chain_map, the element is added to
    the corresponding element chain.

    Note that if the element with same identity appear in two schemas and
    one of the schemas is not the immediate next version of the other, this
    element will still be added to the chain.
    """
    for element in elements:
        identity = _get_schema_element_identity(element)
        chain = identity_to_element_chain_map.get(identity)
        if chain:
            chain.append(element)
