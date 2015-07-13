# -*- coding: utf-8 -*-
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
    def source(self, namespace):
        return factories.create_source(namespace.name, source_name='some_src')

    @pytest.fixture
    def another_source(self, namespace):
        return factories.create_source(namespace.name, source_name='src_two')

    @pytest.fixture
    def topic_one(self, source):
        return factories.create_topic(
            topic_name='some.topic',
            namespace=source.namespace.name,
            source=source.name
        )

    @pytest.fixture
    def topic_two(self, source):
        return factories.create_topic(
            topic_name='topic.2',
            namespace=source.namespace.name,
            source=source.name
        )

    @pytest.fixture
    def topic_of_another_src(self, another_source):
        return factories.create_topic(
            topic_name='another.topic.name',
            namespace=another_source.namespace.name,
            source=another_source.name
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
            self.schema_with_bar_fld_json,
            self.schema_with_bar_fld_elements,
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
    def schema_with_baz_fld(self, topic_one):
        return factories.create_avro_schema(
            self.schema_with_baz_fld_json,
            self.schema_with_baz_fld_elements,
            topic_name=topic_one.name
        )

    @pytest.fixture
    def schema_in_topic_two(self, topic_two):
        return factories.create_avro_schema(
            self.schema_with_bar_fld_json,
            self.schema_with_bar_fld_elements,
            topic_name=topic_two.name
        )

    @pytest.fixture
    def schema_of_another_src(self, topic_of_another_src):
        return factories.create_avro_schema(
            self.schema_with_bar_fld_json,
            self.schema_with_bar_fld_elements,
            topic_name=topic_of_another_src.name
        )

    @property
    def schema_with_no_element_json(self):
        return 'int'

    @pytest.fixture
    def schema_with_no_element(self, topic_one):
        return factories.create_avro_schema(
            self.schema_with_no_element_json,
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
        assert [] == actual

    def test_get_element_chains_by_schema_id_with_one_schema(
            self,
            schema_with_bar_fld
    ):
        actual = repo.get_element_chains_by_schema_id(schema_with_bar_fld.id)
        self.assert_equal_schema_element_chains(actual, schema_with_bar_fld)

    def test_get_element_chains_by_schema_id_with_multi_same_src_schemas(
            self,
            schema_with_bar_fld,
            schema_with_baz_fld,
            schema_in_topic_two,
            schema_of_another_src
    ):
        first_schema, second_schema, third_schema = sorted(
            (schema_with_bar_fld, schema_with_baz_fld, schema_in_topic_two),
            key=lambda o: o.id
        )
        actual = repo.get_element_chains_by_schema_id(third_schema.id)
        self.assert_equal_schema_element_chains(
            actual,
            third_schema,
            second_schema,
            first_schema
        )

    def test_get_element_chains_by_schema_id_with_newer_schema(
            self,
            schema_with_bar_fld,
            schema_with_baz_fld,
            schema_in_topic_two
    ):
        first_schema, second_schema, third_schema = sorted(
            (schema_with_bar_fld, schema_with_baz_fld, schema_in_topic_two),
            key=lambda o: o.id
        )
        actual = repo.get_element_chains_by_schema_id(second_schema.id)
        self.assert_equal_schema_element_chains(
            actual,
            second_schema,
            first_schema
        )

    def assert_equal_avro_schema_element(self, expected, actual):
        assert expected.id == actual.id
        assert expected.avro_schema_id == actual.avro_schema_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

        assert expected.key == actual.key
        assert expected.element_type == actual.element_type
        assert expected.doc == actual.doc

    def assert_equal_schema_element_chains(
            self,
            actual_chains,
            *expected_schemas
    ):
        actual = dict((chain[0].key, chain) for chain in actual_chains)

        # The chain count should match the schema element count of the latest
        # expected schema (the first schema). The expected_schemas are the
        # schemas of which the schema elements are expected in the chain,
        # starting with the latest schema.
        latest_schema = expected_schemas[0]
        assert len(actual_chains) == len(latest_schema.avro_schema_elements)

        element_keys = set(o.key for o in latest_schema.avro_schema_elements)
        for expected_schema in expected_schemas:
            elements = (o for o in expected_schema.avro_schema_elements
                        if o.key in element_keys)
            for element in elements:
                chain = actual[element.key]
                actual_element = chain.pop(0)
                self.assert_equal_avro_schema_element(element, actual_element)

        # There should be no element left in chains
        for chain in actual.values():
            assert 0 == len(chain)
