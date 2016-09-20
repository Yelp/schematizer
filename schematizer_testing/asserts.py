# -*- coding: utf-8 -*-
"""
This module contains the helper functions for testing assertions.

The equality assertion functions are used instead of overriding the __eq__
function of each data model because the data model may be mutable. Also it may
be easier to see which one fails when asserting each value separately.
"""
from __future__ import absolute_import
from __future__ import unicode_literals


def assert_equal_namespace(actual, expected):
    attrs = ('id', 'name', 'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)


def assert_equal_source(actual, expected):
    attrs = ('id', 'name', 'owner_email', 'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)
    assert_equal_namespace(actual.namespace, expected.namespace)


def assert_equal_topic(actual, expected):
    attrs = ('id', 'name', 'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)
    assert_equal_source(actual.source, expected.source)


def assert_equal_avro_schema(actual, expected):
    attrs = ('id', 'avro_schema', 'base_schema_id', 'status',
             'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)
    assert_equal_topic(actual.topic, expected.topic)
    assert_equal_entity_list(
        actual_list=actual.avro_schema_elements,
        expected_list=expected.avro_schema_elements,
        assert_func=assert_equal_avro_schema_element
    )


def assert_equal_avro_schema_element(actual, expected):
    attrs = ('id', 'avro_schema_id', 'key', 'element_type', 'doc',
             'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)


def assert_equal_refresh(actual, expected):
    attrs = ('id', 'source_id', 'status', 'offset', 'batch_size', 'priority',
             'filter_condition', 'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)


def assert_equal_data_target(actual, expected):
    attrs = ('id', 'target_type', 'destination', 'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)


def assert_equal_consumer_group(actual, expected):
    attrs = ('id', 'group_name', 'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)
    assert_equal_data_target(actual.data_target, expected.data_target)


def assert_equal_consumer_group_data_source(actual, expected):
    attrs = ('id', 'data_source_type', 'data_source_id',
             'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)
    assert_equal_consumer_group(actual.consumer_group, expected.consumer_group)


def assert_equal_entity_list(actual_list, expected_list, assert_func):
    assert len(actual_list) == len(expected_list)
    for expected, actual in zip(expected_list, actual_list):
        assert_func(actual, expected)


def assert_equal_entity_set(actual_set, expected_set, assert_func, id_attr):
    actual_id_to_obj_map = {getattr(o, id_attr): o for o in actual_set}

    for actual_id, actual in actual_id_to_obj_map.iteritems():
        expected = next(
            o for o in expected_set if actual_id == getattr(o, id_attr)
        )
        assert_func(actual, expected)

    err_msg = 'Expected id {} is missing in actual result.'
    for expected in expected_set:
        # only need to ensure all the expected entities can be found in the
        # actual entity set.
        expected_id = getattr(expected, id_attr)
        assert expected_id in actual_id_to_obj_map, err_msg.format(expected_id)


def assert_equal_meta_attribute_mapping(actual, expected):
    attrs = ('id', 'entity_type', 'entity_id', 'meta_attr_schema_id',
             'created_at', 'updated_at')
    _assert_equal_multi_attrs(actual, expected, *attrs)


def _assert_equal_multi_attrs(expected_entity, actual_entity, *attrs):
    for attr in attrs:
        assert getattr(actual_entity, attr) == getattr(expected_entity, attr)
