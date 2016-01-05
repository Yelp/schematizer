# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


def assert_equal_namespace(actual, expected):
    assert actual.id == expected.id
    assert actual.name == expected.name
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at


def assert_equal_source(actual, expected):
    assert actual.id == expected.id
    assert_equal_namespace(actual.namespace, expected.namespace)
    assert actual.name == expected.name
    assert actual.owner_email == expected.owner_email
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at


def assert_equal_topic(actual, expected):
    assert actual.id == expected.id
    assert actual.name == expected.name
    assert_equal_source(actual.source, expected.source)
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at


def assert_equal_avro_schema(actual, expected):
    assert actual.id == expected.id
    assert actual.avro_schema == expected.avro_schema
    assert_equal_topic(actual.topic, expected.topic)
    assert actual.base_schema_id == expected.base_schema_id
    assert actual.status == expected.status
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at
    assert_equal_entities(
        actual.avro_schema_elements,
        expected.avro_schema_elements,
        assert_equal_avro_schema_element
    )


def assert_equal_avro_schema_element(actual, expected):
    assert actual.id == expected.id
    assert actual.avro_schema_id == expected.avro_schema_id
    assert actual.key == expected.key
    assert actual.element_type == expected.element_type
    assert actual.doc == expected.doc
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at


def assert_equal_data_target(actual, expected):
    assert actual.id == expected.id
    assert actual.target_type == expected.target_type
    assert actual.destination == expected.destination
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at


def assert_equal_consumer_group(actual, expected):
    assert actual.id == expected.id
    assert actual.group_name == expected.group_name
    assert_equal_data_target(actual.data_target, expected.data_target)
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at


def assert_equal_consumer_group_data_source(actual, expected):
    assert actual.id == expected.id
    assert_equal_consumer_group(actual.consumer_group, expected.consumer_group)
    assert actual.data_source_type == expected.data_source_type
    assert actual.data_source_id == expected.data_source_id
    assert actual.created_at == expected.created_at
    assert actual.updated_at == expected.updated_at


def assert_equal_entities(expected_entities, actual_entities, assert_func):
    assert len(actual_entities) == len(expected_entities)
    for expected, actual in zip(expected_entities, actual_entities):
        assert_func(expected, actual)
