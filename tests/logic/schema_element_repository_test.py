# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer import models
from schematizer.logic import exceptions as sch_exc
from schematizer.logic import schema_element_repository as repo
from testing import factories
from tests.models.testing_db import DBTestCase


class TestSchemaElementRepository(DBTestCase):

    @pytest.fixture
    def namespace(self):
        return factories.create_namespace(namespace_name='some_namespace')

    @pytest.fixture
    def source_one(self, namespace):
        return factories.create_source(namespace.name, source_name='some_src')

    @pytest.fixture
    def source_two(self, namespace):
        return factories.create_source(namespace.name, source_name='src_two')

    @pytest.fixture
    def topic_one(self, source_one):
        return factories.create_topic(
            topic_name='some.topic',
            namespace_name=source_one.namespace.name,
            source_name=source_one.name
        )

    @pytest.fixture
    def topic_two(self, source_one):
        return factories.create_topic(
            topic_name='topic.2',
            namespace_name=source_one.namespace.name,
            source_name=source_one.name
        )

    @pytest.fixture
    def topic_of_src_two(self, source_two):
        return factories.create_topic(
            topic_name='another.topic.name',
            namespace_name=source_two.namespace.name,
            source_name=source_two.name
        )

    @property
    def schema_with_bar_fld_json(self):
        return {
            "name": "foo",
            "namespace": "yelp",
            "type": "record",
            "fields": [{"name": "bar", "type": "int", "doc": "bar"}],
            "doc": "table foo"
        }

    @property
    def schema_with_bar_fld_elements(self):
        return [
            models.AvroSchemaElement(
                key="yelp.foo",
                element_type="record",
                doc="table foo"
            ),
            models.AvroSchemaElement(
                key="yelp.foo|bar",
                element_type="field",
                doc="bar"
            ),
        ]

    @pytest.fixture
    def schema_with_bar_fld(self, topic_one):
        return factories.create_avro_schema(
            schema_json=self.schema_with_bar_fld_json,
            schema_elements=self.schema_with_bar_fld_elements,
            topic_name=topic_one.name
        )

    @property
    def schema_with_baz_fld_json(self):
        return {
            "name": "foo",
            "namespace": "yelp",
            "type": "record",
            "fields": [{"name": "baz", "type": "int", "doc": "baz"}],
            "doc": "table foo"
        }

    @property
    def schema_with_baz_fld_elements(self):
        return [
            models.AvroSchemaElement(
                key="yelp.foo",
                element_type="record",
                doc='table foo'
            ),
            models.AvroSchemaElement(
                key="yelp.foo|baz",
                element_type="field",
                doc="baz"
            ),
        ]

    @pytest.fixture
    def schema_of_src_two(self, topic_of_src_two):
        return factories.create_avro_schema(
            schema_json=self.schema_with_bar_fld_json,
            schema_elements=self.schema_with_bar_fld_elements,
            topic_name=topic_of_src_two.name
        )

    @property
    def schema_with_no_element_json(self):
        return 'int'

    @pytest.fixture
    def schema_with_no_element(self, topic_one):
        return factories.create_avro_schema(
            schema_json=self.schema_with_no_element_json,
            schema_elements=[],
            topic_name=topic_one.name
        )

    def test_get_element_chains_by_schema_id_with_nonexistent_schema(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            repo.get_element_chains_by_schema_id(0)

    def test_get_element_chains_by_schema_id_with_no_element(
        self,
        schema_with_no_element
    ):
        actual = repo.get_element_chains_by_schema_id(
            schema_with_no_element.id
        )
        assert actual == []

    def test_get_element_chains_by_schema_id_with_one_schema(
        self,
        schema_with_bar_fld
    ):
        actual = repo.get_element_chains_by_schema_id(schema_with_bar_fld.id)
        expected = [[el] for el in schema_with_bar_fld.avro_schema_elements]
        self.assert_equal_schema_element_chains(expected, actual)

    @pytest.fixture
    def schemas_of_src_one(self, topic_one, topic_two, schema_with_bar_fld):
        """List of schemas of source one, starting with latest schema"""
        schema_with_baz_fld = factories.create_avro_schema(
            schema_json=self.schema_with_baz_fld_json,
            schema_elements=self.schema_with_baz_fld_elements,
            topic_name=topic_one.name
        )
        schema_of_topic_two = factories.create_avro_schema(
            schema_json=self.schema_with_bar_fld_json,
            schema_elements=self.schema_with_bar_fld_elements,
            topic_name=topic_two.name
        )
        return [schema_of_topic_two, schema_with_baz_fld, schema_with_bar_fld]

    def test_get_element_chains_by_schema_id_with_multi_same_src_schemas(
        self,
        schemas_of_src_one
    ):
        latest_schema, schema_with_baz_fld, oldest_schema = schemas_of_src_one
        actual = repo.get_element_chains_by_schema_id(latest_schema.id)
        expected = [[el] for el in latest_schema.avro_schema_elements]
        expected[0].append(schema_with_baz_fld.avro_schema_elements[0])
        expected[0].append(oldest_schema.avro_schema_elements[0])
        self.assert_equal_schema_element_chains(expected, actual)

    def test_get_element_chains_by_schema_id_with_newer_schema(
        self,
        schemas_of_src_one
    ):
        _, second_latest_schema, oldest_schema = schemas_of_src_one
        actual = repo.get_element_chains_by_schema_id(second_latest_schema.id)
        expected = [[el] for el in second_latest_schema.avro_schema_elements]
        expected[0].append(oldest_schema.avro_schema_elements[0])
        self.assert_equal_schema_element_chains(expected, actual)

    def assert_equal_schema_element_chains(
        self,
        expected_chains,
        actual_chains
    ):
        assert len(actual_chains) == len(expected_chains)

        key_to_chain_map = {chain[0].key: chain for chain in actual_chains}
        for expected in expected_chains:
            actual = key_to_chain_map[expected[0].key]
            self.assert_equal_schema_element_chain(expected, actual)

    def assert_equal_schema_element_chain(self, expected_chain, actual_chain):
        assert len(actual_chain) == len(expected_chain)
        for i, expected in enumerate(expected_chain):
            actual = actual_chain[i]
            self.assert_equal_avro_schema_element(expected, actual)

    def assert_equal_avro_schema_element(self, expected, actual):
        assert expected.id == actual.id
        assert expected.avro_schema_id == actual.avro_schema_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

        assert expected.key == actual.key
        assert expected.element_type == actual.element_type
        assert expected.doc == actual.doc
