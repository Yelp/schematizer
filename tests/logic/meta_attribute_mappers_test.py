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
from schematizer_testing import factories
from schematizer_testing.asserts import assert_equal_meta_attribute_mapping
from tests.models.testing_db import DBTestCase


class RegisterAndDeleteMetaAttributeBase(DBTestCase):

    def assert_equal_meta_attr_partial(self, expected, actual):
        assert expected.entity_type == actual.entity_type
        assert expected.entity_id == actual.entity_id
        assert expected.meta_attr_schema_id == actual.meta_attr_schema_id

    def _setup_meta_attribute_mapping(self, meta_attr_schema, entity_id):
        factories.create_meta_attribute_mapping(
            meta_attr_schema.id,
            self.entity_model.__name__,
            entity_id
        )

    def test_invalid_entity_id_fails(self, meta_attr_schema):
        fake_entity_id = 0
        with pytest.raises(EntityNotFoundError):
            meta_attr_logic.register_meta_attribute_for_entity(
                self.entity_model,
                fake_entity_id,
                meta_attr_schema.id
            )
        with pytest.raises(EntityNotFoundError):
            meta_attr_logic.delete_meta_attribute_mapping_for_entity(
                self.entity_model,
                fake_entity_id,
                meta_attr_schema.id
            )

    def test_register_first_time(self, meta_attr_schema):
        actual = meta_attr_logic.register_meta_attribute_for_entity(
            self.entity_model,
            self.entity.id,
            meta_attr_schema.id
        )
        expected = meta_attr_model(
            entity_type=self.entity_model.__name__,
            entity_id=self.entity.id,
            meta_attr_schema_id=meta_attr_schema.id
        )
        self.assert_equal_meta_attr_partial(expected, actual)

    def test_idempotent_registration(self, meta_attr_schema):
        self._setup_meta_attribute_mapping(meta_attr_schema, self.entity.id)
        first_result = meta_attr_logic.register_meta_attribute_for_entity(
            self.entity_model,
            self.entity.id,
            meta_attr_schema.id
        )
        second_result = meta_attr_logic.register_meta_attribute_for_entity(
            self.entity_model,
            self.entity.id,
            meta_attr_schema.id
        )
        expected = meta_attr_model(
            entity_type=self.entity_model.__name__,
            entity_id=self.entity.id,
            meta_attr_schema_id=meta_attr_schema.id
        )
        self.assert_equal_meta_attr_partial(expected, first_result)
        assert_equal_meta_attribute_mapping(first_result, second_result)

    def test_delete_mapping(self, meta_attr_schema):
        self._setup_meta_attribute_mapping(meta_attr_schema, self.entity.id)
        actual = meta_attr_logic.delete_meta_attribute_mapping_for_entity(
            self.entity_model,
            self.entity.id,
            meta_attr_schema.id
        )
        expected = meta_attr_model(
            entity_type=self.entity_model.__name__,
            entity_id=self.entity.id,
            meta_attr_schema_id=meta_attr_schema.id
        )
        self.assert_equal_meta_attr_partial(expected, actual)
        with pytest.raises(orm_exc.NoResultFound):
            session.query(
                meta_attr_model
            ).filter(
                meta_attr_model.entity_type == self.entity_model.__name__,
                meta_attr_model.entity_id == self.entity.id,
                meta_attr_model.meta_attr_schema_id == meta_attr_schema.id
            ).one()

    def test_delete_non_existent_mapping(self, meta_attr_schema):
        with pytest.raises(EntityNotFoundError):
            meta_attr_logic.delete_meta_attribute_mapping_for_entity(
                self.entity_model,
                self.entity.id,
                meta_attr_schema.id
            )


@pytest.mark.usefixtures('setup_test')
class TestRegisterAndDeleteMetaAttributeForNamespace(
    RegisterAndDeleteMetaAttributeBase
):

    @pytest.fixture
    def setup_test(self, yelp_namespace):
        self.entity_model = Namespace
        self.entity = yelp_namespace


@pytest.mark.usefixtures('setup_test')
class TestRegisterAndDeleteMetaAttributeForSource(
    RegisterAndDeleteMetaAttributeBase
):

    @pytest.fixture
    def setup_test(self, biz_source):
        self.entity_model = Source
        self.entity = biz_source


@pytest.mark.usefixtures('setup_test')
class TestRegisterAndDeleteMetaAttributeForSchema(
    RegisterAndDeleteMetaAttributeBase
):

    @pytest.fixture
    def setup_test(self, biz_schema):
        self.entity_model = AvroSchema
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

    +----+-------------+-----------+--------------------------+
    | id | entity_type | entity_id |     meta_attr_schema     |
    +----+-------------+-----------+--------------------------+
    |  1 |   namespace |         A |      namespace_meta_attr |
    |  2 |      source |        A1 |         source_meta_attr |
    |  3 |      schema |       A1X |         schema_meta_attr |
    +----+-------------+-----------+--------------------------+
    """

    @pytest.fixture
    def dummy_namespace(self):
        return factories.create_namespace('yelp_meta_A')

    @pytest.fixture
    def dummy_src(self, dummy_namespace):
        return factories.create_source(
            namespace_name=dummy_namespace.name,
            source_name='meta_source_A_1',
            owner_email='test-meta-src@yelp.com'
        )

    @pytest.fixture
    def dummy_schema(
        self,
        dummy_namespace,
        dummy_src,
        meta_attr_schema_json,
        meta_attr_schema_elements
    ):
        return factories.create_avro_schema(
            meta_attr_schema_json,
            meta_attr_schema_elements,
            topic_name='.'.join([dummy_namespace.name, dummy_src.name, '1']),
            namespace=dummy_namespace.name,
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
    def namespace_meta_attr(
        self,
        meta_attr_schema_json,
        meta_attr_schema_elements
    ):
        return self._create_meta_attribute_schema(
            'namespace_meta_attr',
            meta_attr_schema_json,
            meta_attr_schema_elements
        )

    @pytest.fixture
    def source_meta_attr(
        self,
        meta_attr_schema_json,
        meta_attr_schema_elements
    ):
        return self._create_meta_attribute_schema(
            'source_meta_attr',
            meta_attr_schema_json,
            meta_attr_schema_elements
        )

    @pytest.fixture
    def schema_meta_attr(
        self,
        meta_attr_schema_json,
        meta_attr_schema_elements
    ):
        return self._create_meta_attribute_schema(
            'schema_meta_attr',
            meta_attr_schema_json,
            meta_attr_schema_elements
        )

    @pytest.fixture
    def namespace_meta_attr_mapping(
        self,
        namespace_meta_attr,
        dummy_namespace
    ):
        factories.create_meta_attribute_mapping(
            namespace_meta_attr.id,
            Namespace.__name__,
            dummy_namespace.id
        )

    @pytest.fixture
    def source_meta_attr_mapping(self, source_meta_attr, dummy_src):
        factories.create_meta_attribute_mapping(
            source_meta_attr.id,
            Source.__name__,
            dummy_src.id
        )

    @pytest.fixture
    def schema_meta_attr_mapping(self, schema_meta_attr, dummy_schema):
        factories.create_meta_attribute_mapping(
            schema_meta_attr.id,
            AvroSchema.__name__,
            dummy_schema.id
        )


@pytest.mark.usefixtures(
    'namespace_meta_attr_mapping',
    'source_meta_attr_mapping',
    'schema_meta_attr_mapping'
)
class TestGetMetaAttributeMappings(GetMetaAttributeBaseTest):

    def test_get_mapping_by_namespace(
        self,
        dummy_namespace,
        namespace_meta_attr
    ):
        actual = meta_attr_logic.get_meta_attributes_by_namespace(
            dummy_namespace.id
        )
        expected = [namespace_meta_attr.id]
        assert actual == expected

    def test_get_mapping_by_source(
        self,
        dummy_src,
        namespace_meta_attr,
        source_meta_attr
    ):
        actual = meta_attr_logic.get_meta_attributes_by_source(dummy_src.id)
        expected = [namespace_meta_attr.id, source_meta_attr.id]
        assert actual == expected

    def test_get_mapping_by_schema(
        self,
        dummy_schema,
        namespace_meta_attr,
        source_meta_attr,
        schema_meta_attr
    ):
        actual = meta_attr_logic.get_meta_attributes_by_schema(dummy_schema.id)
        expected = [
            namespace_meta_attr.id,
            source_meta_attr.id,
            schema_meta_attr.id
        ]
        assert actual == expected

    @pytest.mark.parametrize('getter_method', [
        meta_attr_logic.get_meta_attributes_by_namespace,
        meta_attr_logic.get_meta_attributes_by_source,
        meta_attr_logic.get_meta_attributes_by_schema
    ])
    def test_get_non_existing_mapping(self, getter_method):
        fake_id = 0
        with pytest.raises(EntityNotFoundError):
            getter_method(fake_id)
