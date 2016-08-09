# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from sqlalchemy.orm import exc as orm_exc

from schematizer.logic import meta_attribute_mappers as meta_attr_logic
from schematizer.models import AvroSchema
from schematizer.models import MetaAttributeMappingStore as meta_attr_model
from schematizer.models import Namespace
from schematizer.models import Source
from schematizer.models.database import session
from schematizer.models.exceptions import EntityNotFoundError
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
            self.entity_model.__name__,
            entity_id
        )

    def test_register_first_time(self, meta_attr_schema):
        actual = self.register_logic_method(
            meta_attr_schema.id,
            self.entity.id
        )
        expected = meta_attr_model(
            entity_type=self.entity_model.__name__,
            entity_id=self.entity.id,
            meta_attr_schema_id=meta_attr_schema.id
        )
        self.assert_equal_meta_attr_partial(expected, actual)

    def test_idempotent_registration(self, meta_attr_schema):
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
            entity_type=self.entity_model.__name__,
            entity_id=self.entity.id,
            meta_attr_schema_id=meta_attr_schema.id
        )
        self.assert_equal_meta_attr_partial(expected, first_result)
        self.assert_equal_meta_attr(first_result, second_result)

    def test_delete_mapping(self, meta_attr_schema):
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
                meta_attr_model.entity_type == self.entity_model.__name__,
                meta_attr_model.entity_id == self.entity.id,
                meta_attr_model.meta_attr_schema_id == meta_attr_schema.id
            ).one()


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForNamespace(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, yelp_namespace):
        self.entity_model = Namespace
        self.register_logic_method = meta_attr_logic.\
            register_meta_attribute_mapping_for_namespace
        self.delete_logic_method = meta_attr_logic.\
            delete_meta_attribute_mapping_for_namespace
        self.entity = yelp_namespace


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForSource(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_source):
        self.entity_model = Source
        self.register_logic_method = meta_attr_logic.\
            register_meta_attribute_mapping_for_source
        self.delete_logic_method = meta_attr_logic.\
            delete_meta_attribute_mapping_for_source
        self.entity = biz_source


@pytest.mark.usefixtures('setup_test')
class TestRegisterMetaAttributeForSchema(RegisterMetaAttributeBase):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_model = AvroSchema
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
    def dummy_nsp(self):
        return factories.create_namespace('yelp_meta_A')

    @pytest.fixture
    def dummy_src(self, dummy_nsp):
        return factories.create_source(
            namespace_name=dummy_nsp.name,
            source_name='meta_source_A_1',
            owner_email='test-meta-src@yelp.com'
        )

    @pytest.fixture
    def dummy_schema(
        self,
        dummy_nsp,
        dummy_src,
        meta_attr_schema_json,
        meta_attr_schema_elements
    ):
        return factories.create_avro_schema(
            meta_attr_schema_json,
            meta_attr_schema_elements,
            topic_name='.'.join([dummy_nsp.name, dummy_src.name, '1']),
            namespace=dummy_nsp.name,
            source=dummy_src.name
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
        self,
        meta_attr_1,
        meta_attr_2,
        meta_attr_3,
        dummy_nsp,
        dummy_src,
        dummy_schema
    ):
        factories.create_meta_attribute_mapping(
            meta_attr_1.id,
            Namespace.__name__,
            dummy_nsp.id
        )
        factories.create_meta_attribute_mapping(
            meta_attr_2.id,
            Source.__name__,
            dummy_src.id
        )
        factories.create_meta_attribute_mapping(
            meta_attr_3.id,
            AvroSchema.__name__,
            dummy_schema.id
        )


@pytest.mark.usefixtures('setup_meta_attr_mappings')
class TestGetMetaAttributeMappings(GetMetaAttributeBaseTest):

    def test_get_mapping_by_namespace(
        self,
        dummy_nsp,
        meta_attr_1
    ):
        actual = meta_attr_logic.get_meta_attributes_by_namespace(dummy_nsp.id)
        expected = [meta_attr_1.id]
        assert actual == expected

    def test_get_mapping_by_source(
        self,
        dummy_src,
        meta_attr_1,
        meta_attr_2
    ):
        actual = meta_attr_logic.get_meta_attributes_by_source(dummy_src.id)
        expected = [meta_attr_1.id, meta_attr_2.id]
        assert actual == expected

    def test_get_mapping_by_schema(
        self,
        dummy_schema,
        meta_attr_1,
        meta_attr_2,
        meta_attr_3
    ):
        actual = meta_attr_logic.get_meta_attributes_by_schema(dummy_schema.id)
        expected = [meta_attr_1.id, meta_attr_2.id, meta_attr_3.id]
        assert actual == expected

    def test_get_non_existing_mapping(self):
        fake_nsp_id = 1234
        with pytest.raises(EntityNotFoundError):
            meta_attr_logic.get_meta_attributes_by_namespace(fake_nsp_id)
