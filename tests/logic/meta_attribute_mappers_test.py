# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from sqlalchemy.orm import exc as orm_exc

from schematizer.logic import meta_attribute_mappers as meta_attr_logic
from schematizer.models import EntityType
from schematizer.models import MetaAttributeMappingStore as meta_attr_model
from schematizer.models import Namespace
from schematizer.models.database import session
from testing import factories
from tests.models.testing_db import DBTestCase


class RegisterMetaAttributeBase(DBTestCase):

    def assert_equal_meta_attr_partial(self, expected, actual):
        assert expected.entity_type == actual.entity_type
        assert expected.entity_id == actual.entity_id
        assert expected.meta_attr_schema_id == actual.meta_attr_schema_id

    def assert_equal_meta_attr(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_meta_attr_partial(expected, actual)

    def _setup_meta_attribute_mapping(self, meta_attr_schema, entity_id):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_type,
            entity_id
        )

    def test_register_first_time(self, setup_test, meta_attr_schema):
        actual = self.register_logic_method(
            meta_attr_schema.id,
            self.entity.id
        )
        expected = meta_attr_model(
            entity_type=self.entity_type,
            entity_id=self.entity.id,
            meta_attr_schema_id=meta_attr_schema.id
        )
        self.assert_equal_meta_attr_partial(expected, actual)

    def test_idempotent_registration(self, setup_test, meta_attr_schema):
        self._setup_meta_attribute_mapping(meta_attr_schema, self.entity.id)
        first_result = self.register_logic_method(
            meta_attr_schema.id,
            self.entity.id
        )
        second_result = self.register_logic_method(
            meta_attr_schema.id,
            self.entity.id
        )
        expected = meta_attr_model(
            entity_type=self.entity_type,
            entity_id=self.entity.id,
            meta_attr_schema_id=meta_attr_schema.id
        )
        self.assert_equal_meta_attr_partial(expected, first_result)
        self.assert_equal_meta_attr(first_result, second_result)

    def test_delete_mapping(self, setup_test, meta_attr_schema):
        self._setup_meta_attribute_mapping(meta_attr_schema, self.entity.id)
        actual = self.delete_logic_method(
            meta_attr_schema.id,
            self.entity.id
        )
        assert actual is True
        with pytest.raises(orm_exc.NoResultFound):
            session.query(
                meta_attr_model
            ).filter(
                meta_attr_model.entity_type == self.entity_type,
                meta_attr_model.entity_id == self.entity.id,
                meta_attr_model.meta_attr_schema_id == meta_attr_schema.id
            ).one()


class TestRegisterMetaAttributeForNamespace(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, yelp_namespace):
        self.entity_type = EntityType.NAMESPACE
        self.register_logic_method = meta_attr_logic.\
            register_meta_attribute_mapping_for_namespace
        self.delete_logic_method = meta_attr_logic.\
            delete_meta_attribute_mapping_for_namespace
        self.entity = yelp_namespace


class TestRegisterMetaAttributeForSource(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_source):
        self.entity_type = EntityType.SOURCE
        self.register_logic_method = meta_attr_logic.\
            register_meta_attribute_mapping_for_source
        self.delete_logic_method = meta_attr_logic.\
            delete_meta_attribute_mapping_for_source
        self.entity = biz_source


class TestRegisterMetaAttributeForSchema(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_type = EntityType.SCHEMA
        self.register_logic_method = meta_attr_logic.\
            register_meta_attribute_mapping_for_schema
        self.delete_logic_method = \
            meta_attr_logic.delete_meta_attribute_mapping_for_schema
        self.entity = biz_schema


class GetMetaAttributeBaseTest(DBTestCase):
    """MetaAttribute Mappings are supposed to be additive. In other words, a
    Source should have all the meta attributes for itself and the namespace it
    belongs to. Similarly an AvroSchema should have all the meta attributes for
    itself and the source and namespace it belongs to.

    Below are the entity structures and the meta attribute mappings I will be
    testing with:
        NamespaceA:
          - SourceA1
            - SchemaA1X
        NamespaceB

    +----+-------------+-----------+------------------+
    | id | entity_type | entity_id | meta_attr_schema |
    +----+-------------+-----------+------------------+
    |  1 |   namespace |         A |      meta_attr_1 |
    |  2 |      source |        A1 |      meta_attr_2 |
    |  3 |      schema |       A1X |      meta_attr_3 |
    +----+-------------+-----------+------------------+
    """

    @pytest.fixture
    def test_nsp(self):
        return factories.create_namespace('yelp_meta_A')

    @pytest.fixture
    def test_src(self, test_nsp):
        return factories.create_source(
            namespace_name=test_nsp.name,
            source_name='meta_source_A_1',
            owner_email='test-meta-src@yelp.com'
        )

    @pytest.fixture
    def test_schema(
        self,
        test_nsp,
        test_src,
        meta_attr_schema_json,
        meta_attr_schema_elements
    ):
        return factories.create_avro_schema(
            meta_attr_schema_json,
            meta_attr_schema_elements,
            topic_name='.'.join([test_nsp.name, test_src.name, '1']),
            namespace=test_nsp.name,
            source=test_src.name
        )

    def _create_meta_attribute_schema(
        self,
        source_name,
        meta_attr_schema_json,
        meta_attr_schema_elements
    ):
        return factories.create_avro_schema(
            meta_attr_schema_json,
            meta_attr_schema_elements,
            topic_name='.'.join(['yelp_meta', source_name, '1']),
            namespace='yelp_meta',
            source=source_name
        )

    @pytest.fixture
    def meta_attr_1(self, meta_attr_schema_json, meta_attr_schema_elements):
        return self._create_meta_attribute_schema(
            'meta_atr_1', meta_attr_schema_json, meta_attr_schema_elements
        )

    @pytest.fixture
    def meta_attr_2(self, meta_attr_schema_json, meta_attr_schema_elements):
        return self._create_meta_attribute_schema(
            'meta_atr_2', meta_attr_schema_json, meta_attr_schema_elements
        )

    @pytest.fixture
    def meta_attr_3(self, meta_attr_schema_json, meta_attr_schema_elements):
        return self._create_meta_attribute_schema(
            'meta_atr_3', meta_attr_schema_json, meta_attr_schema_elements
        )

    @pytest.fixture
    def setup_meta_attr_mappings(
        self, meta_attr_1, meta_attr_2, meta_attr_3,
        test_nsp, test_src, test_schema
    ):
        factories.create_meta_attribute_mapping(
            meta_attr_1.id,
            EntityType.NAMESPACE,
            test_nsp.id
        )
        factories.create_meta_attribute_mapping(
            meta_attr_2.id,
            EntityType.SOURCE,
            test_src.id
        )
        factories.create_meta_attribute_mapping(
            meta_attr_3.id,
            EntityType.SCHEMA,
            test_schema.id
        )


class TestGetMetaAttributeMappings(GetMetaAttributeBaseTest):

    def test_get_mapping_by_namespace(
        self,
        test_nsp,
        meta_attr_1,
        setup_meta_attr_mappings
    ):
        actual = meta_attr_logic.get_meta_attributes_by_namespace(test_nsp)
        expected = [meta_attr_1.id]
        assert actual == expected

    def test_get_mapping_by_source(
        self,
        test_src,
        meta_attr_1,
        meta_attr_2,
        setup_meta_attr_mappings
    ):
        actual = meta_attr_logic.get_meta_attributes_by_source(test_src)
        expected = [meta_attr_1.id, meta_attr_2.id]
        assert actual == expected

    def test_get_mapping_by_schema(
        self,
        test_schema,
        meta_attr_1,
        meta_attr_2,
        meta_attr_3,
        setup_meta_attr_mappings
    ):
        actual = meta_attr_logic.get_meta_attributes_by_schema(test_schema)
        expected = [meta_attr_1.id, meta_attr_2.id, meta_attr_3.id]
        assert actual == expected

    def test_get_non_existing_mapping(self, setup_meta_attr_mappings):
        fake_nsp = Namespace(name='fake_namespace')
        actual = meta_attr_logic.get_meta_attributes_by_namespace(fake_nsp)
        expected = []
        assert actual == expected


class TestAddToMetaAttrStore(GetMetaAttributeBaseTest):

    def _get_meta_attr_mappings_as_dict(self, mappings):
        mappings_dict = {}
        for m in mappings:
            if m.schema_id in mappings_dict:
                mappings_dict.get(m.schema_id).add(m.meta_attr_schema_id)
            else:
                mappings_dict[m.schema_id] = {m.meta_attr_schema_id}
        return mappings_dict

    def test_add_unique_mappings(
        self,
        test_schema,
        meta_attr_1,
        meta_attr_2,
        meta_attr_3,
        setup_meta_attr_mappings
    ):
        actual = meta_attr_logic.add_meta_attribute_mappings(test_schema)
        expected = {
            test_schema.id: {meta_attr_1.id, meta_attr_2.id, meta_attr_3.id}
        }
        assert self._get_meta_attr_mappings_as_dict(actual) == expected

        actual_2 = meta_attr_logic.add_meta_attribute_mappings(test_schema)
        assert self._get_meta_attr_mappings_as_dict(actual_2) == expected

    def test_add_duplicate_mappings(
        self,
        test_schema,
        meta_attr_1,
        meta_attr_2,
        meta_attr_3,
        setup_meta_attr_mappings
    ):
        factories.create_meta_attribute_mapping(
            meta_attr_2.id,
            EntityType.SOURCE,
            test_schema.id
        )
        actual = meta_attr_logic.add_meta_attribute_mappings(test_schema)
        expected = {
            test_schema.id: {meta_attr_1.id, meta_attr_2.id, meta_attr_3.id}
        }
        assert self._get_meta_attr_mappings_as_dict(actual) == expected

    def test_handle_non_existing_mappings(
        self,
        biz_schema,
        setup_meta_attr_mappings
    ):
        actual = meta_attr_logic.add_meta_attribute_mappings(biz_schema)
        expected = []
        assert actual == expected
