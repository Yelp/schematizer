# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
import simplejson

from schematizer import models
from schematizer.models.avro_schema import AvroSchema
from schematizer_testing import factories
from tests.models.base_model_test import GetAllModelTestBase
from tests.models.testing_db import DBTestCase


class TestGetAllAvroSchemas(GetAllModelTestBase):

    def create_avro_schema(self, avro_schema_no):
        my_topic = factories.get_or_create_topic(
            topic_name='my_topic',
            namespace_name='foo',
            source_name='bar'
        )
        type_name = "fixed_type_{}".format(avro_schema_no)
        return factories.create_avro_schema(
            schema_json={
                "type": "fixed",
                "size": 16,
                "name": type_name
            },
            topic_name=my_topic.name
        )

    entity_model = AvroSchema
    create_entity_func = create_avro_schema
    assert_func_name = 'assert_equal_avro_schema'


class TestAvroSchemaModel(DBTestCase):

    @property
    def schema_id(self):
        return 1

    @property
    def element_id(self):
        return 100

    def _create_element(self, doc, key, element_type, element_id=None):
        return models.AvroSchemaElement(
            id=element_id or self.element_id,
            key=key,
            avro_schema_id=self.schema_id,
            element_type=element_type,
            doc=doc
        )

    @pytest.fixture
    def enum_schema_json(self):
        return {"name": "color", "type": "enum", "symbols": ["red"]}

    @pytest.fixture
    def enum_schema_json_with_doc(self, enum_schema_json):
        schema_json = dict(enum_schema_json)
        schema_json.update({
            models.AvroSchema.DOC_ATTR: "object color",
            models.AvroSchema.ELEMENT_ID_ATTR: 101
        })
        return schema_json

    @pytest.fixture
    def enum_schema_element(self):
        return self._create_element(
            doc="object color",
            key="color",
            element_type="enum",
            element_id=101
        )

    def test_enum_avro_schema_with_doc(
        self,
        enum_schema_json,
        enum_schema_element,
        enum_schema_json_with_doc
    ):
        schema_model = models.AvroSchema(
            id=self.schema_id,
            avro_schema=simplejson.dumps(enum_schema_json),
            avro_schema_elements=[enum_schema_element]
        )
        expected = enum_schema_json_with_doc
        assert expected == schema_model.avro_schema_with_doc

    @pytest.fixture
    def array_schema_json(self):
        return {"type": "array", "items": "string"}

    @pytest.fixture
    def array_schema_json_with_doc(self, array_schema_json):
        schema_json = dict(array_schema_json)
        schema_json.update({
            models.AvroSchema.DOC_ATTR: "string array",
            models.AvroSchema.ELEMENT_ID_ATTR: 102
        })
        return schema_json

    @pytest.fixture
    def array_schema_element(self):
        return self._create_element(
            doc="string array",
            key="array",
            element_type="array",
            element_id=102
        )

    def test_array_avro_schema_with_doc(
        self,
        array_schema_json,
        array_schema_element,
        array_schema_json_with_doc
    ):
        schema_model = models.AvroSchema(
            id=self.schema_id,
            avro_schema=simplejson.dumps(array_schema_json),
            avro_schema_elements=[array_schema_element]
        )
        expected = array_schema_json_with_doc
        assert expected == schema_model.avro_schema_with_doc

    @pytest.fixture
    def inner_array_schema_json(self):
        return {"type": "array", "items": "int"}

    @pytest.fixture
    def inner_array_schema_json_with_doc(self, inner_array_schema_json):
        schema_json = dict(inner_array_schema_json)
        schema_json.update({
            models.AvroSchema.DOC_ATTR: "some array",
            models.AvroSchema.ELEMENT_ID_ATTR: 103
        })
        return schema_json

    @pytest.fixture
    def inner_array_schema_element(self):
        return self._create_element(
            doc="some array",
            key="foo_foo|bar|array",
            element_type="array",
            element_id=103
        )

    @pytest.fixture
    def inner_record_schema_json(
        self,
        inner_array_schema_json,
        enum_schema_json
    ):
        return {
            'type': 'record',
            'name': 'foo_foo',
            'namespace': '',
            'fields': [
                {"name": "bar", "type": inner_array_schema_json},
                {"name": "baz", "type": enum_schema_json}
            ]
        }

    @pytest.fixture
    def inner_record_schema_json_with_doc(
        self,
        inner_array_schema_json_with_doc,
        enum_schema_json_with_doc
    ):
        enum_schema_json_with_doc.update({'namespace': ''})
        return {
            'type': 'record',
            'name': 'foo_foo',
            'namespace': '',
            'fields': [
                {"name": "bar", "type": inner_array_schema_json_with_doc},
                {"name": "baz", "type": enum_schema_json_with_doc}
            ]
        }

    @pytest.fixture
    def nested_record_schema_json(
        self,
        inner_record_schema_json,
        array_schema_json
    ):
        return {
            "type": "record",
            "name": "foo",
            "fields": [
                {"name": "bar", "type": array_schema_json},
                {"name": "baz", "type": inner_record_schema_json}
            ]
        }

    @pytest.fixture
    def foo_record_schema_element(self):
        return self._create_element(
            doc="table foo",
            key="foo",
            element_type="record",
            element_id=104
        )

    @pytest.fixture
    def field_bar_schema_element(self):
        return self._create_element(
            doc="column bar",
            key="foo|bar",
            element_type="field",
            element_id=105
        )

    @pytest.fixture
    def nested_record_schema_json_with_doc(
        self,
        array_schema_json,
        inner_record_schema_json_with_doc
    ):
        return {
            "type": "record",
            "name": "foo",
            "fields": [
                {
                    "name": "bar",
                    "type": array_schema_json,
                    models.AvroSchema.DOC_ATTR: "column bar",
                    models.AvroSchema.ELEMENT_ID_ATTR: 105
                },
                {"name": "baz", "type": inner_record_schema_json_with_doc}
            ],
            models.AvroSchema.DOC_ATTR: "table foo",
            models.AvroSchema.ELEMENT_ID_ATTR: 104,
        }

    def test_nested_record_avro_schema_with_doc(
        self,
        inner_array_schema_element,
        enum_schema_element,
        foo_record_schema_element,
        field_bar_schema_element,
        nested_record_schema_json,
        nested_record_schema_json_with_doc
    ):
        schema_model = models.AvroSchema(
            id=self.schema_id,
            avro_schema=simplejson.dumps(nested_record_schema_json),
            avro_schema_elements=[
                inner_array_schema_element,
                enum_schema_element,
                foo_record_schema_element,
                field_bar_schema_element,
            ]
        )
        expected = nested_record_schema_json_with_doc
        assert expected == schema_model.avro_schema_with_doc

    def test_create_schema_elements_from_json_with_enum(
        self,
        enum_schema_json_with_doc,
        enum_schema_element
    ):
        actual = models.AvroSchema.create_schema_elements_from_json(
            enum_schema_json_with_doc
        )
        assert 1 == len(actual)
        actual_element = actual[0]
        assert enum_schema_element.key == actual_element.key
        assert enum_schema_element.element_type == actual_element.element_type
        assert enum_schema_element.doc == actual_element.doc

    def test_create_schema_elements_from_json_with_array(
        self,
        array_schema_json
    ):
        actual = models.AvroSchema.create_schema_elements_from_json(
            array_schema_json
        )
        assert 1 == len(actual)
        actual_element = actual[0]
        assert 'array' == actual_element.key
        assert 'array' == actual_element.element_type
        assert actual_element.doc is None

    @pytest.fixture
    def schema_elements_of_nested_record_schema(self):
        return [
            models.AvroSchemaElement(key="foo", element_type="record"),
            models.AvroSchemaElement(key="foo|bar", element_type="field"),
            models.AvroSchemaElement(
                key="foo|bar|array",
                element_type="array"
            ),
            models.AvroSchemaElement(key="foo|baz", element_type="field"),
            models.AvroSchemaElement(key="foo_foo", element_type="record"),
            models.AvroSchemaElement(key="foo_foo|bar", element_type="field"),
            models.AvroSchemaElement(key="foo_foo|baz", element_type="field"),
            models.AvroSchemaElement(
                key="foo_foo|bar|array",
                element_type="array"
            ),
            models.AvroSchemaElement(key="color", element_type="enum"),
        ]

    def test_create_schema_elements_from_json_with_nested_record(
        self,
        nested_record_schema_json,
        schema_elements_of_nested_record_schema
    ):
        actual = models.AvroSchema.create_schema_elements_from_json(
            nested_record_schema_json
        )
        assert len(schema_elements_of_nested_record_schema) == len(actual)
        for actual_element in actual:
            expected = next(
                (o for o in schema_elements_of_nested_record_schema
                 if o.key == actual_element.key),
                None
            )
            assert expected.element_type == actual_element.element_type

    @pytest.mark.parametrize("avro_schema", [
        {
            "name": "foo",
            "type": "record",
            "fields": [{"type": "int", "name": "col"}]
        },
        {"name": "foo", "type": "enum", "symbols": ["a"]},
        {"name": "foo", "type": "fixed", "size": 16},
        {"type": "array", "items": "int"},
        {"type": "map", "values": "long"},
        "int",
        ["int", "null"],
    ])
    def test_verify_avro_schema_with_valid_schema_json(self, avro_schema):
        is_valid, error = models.AvroSchema.verify_avro_schema(avro_schema)
        assert is_valid
        assert error is None

    @pytest.fixture(params=[
        {"name": "foo", "type": "record", "fields": ["bar"]},
        {"name": "", "type": "enum", "symbols": ["a"]},
        {"name": "foo", "type": "fixed"},
        {"type": "array"},
        {"values": "long"},
        "str",
        ["null", "null"],
        100
    ])
    def invalid_avro_schema(self, request):
        return request.param

    def test_verify_avro_schema_with_invalid_schema_json(
        self,
        invalid_avro_schema
    ):
        is_valid, error = models.AvroSchema.verify_avro_schema(
            invalid_avro_schema
        )
        assert not is_valid
        assert error

    @pytest.mark.parametrize("avro_schema", [
        {
            "name": "foo",
            "doc": "test_doc",
            "type": "record",
            "namespace": "test_namespace",
            "fields": [
                {"type": "int", "name": "col", "doc": "test_doc"},
                {"name": "clientHash", "doc": "test_doc",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}}
            ]
        },
        {"name": "foo", "type": "fixed", "size": 16},
        {"name": "color", "doc": "test_d", "type": "enum", "symbols": ["red"]}
    ])
    def test_verify_avro_schema_with_schema_doc(self, avro_schema):
        models.AvroSchema.verify_avro_schema_has_docs(avro_schema)

    @pytest.mark.parametrize("avro_schema_with_missing_docs", [
        {
            "name": "foo",
            "doc": "",
            "type": "record",
            "namespace": "test_namespace",
            "fields": [
                {"type": "int", "name": "col"},
                {"name": "clientHash", "doc": " ",
                 "type": {"type": "fixed", "name": "MD5", "size": 16}}
            ]
        },
        {"name": "color", "type": "enum", "symbols": ["red"]}
    ])
    def test_avro_schema_with_missing_doc_fails(
        self,
        avro_schema_with_missing_docs
    ):
        with pytest.raises(ValueError):
            models.AvroSchema.verify_avro_schema_has_docs(
                avro_schema_with_missing_docs
            )

    def test_avro_schema_with_invalid_schema_fails(
        self,
        invalid_avro_schema
    ):
        with pytest.raises(Exception):
            models.AvroSchema.verify_avro_schema_has_docs(
                invalid_avro_schema
            )
