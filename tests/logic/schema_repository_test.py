# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime
import copy

import mock
import pytest

from schematizer import models
from schematizer.components import converters
from schematizer.logic import exceptions as sch_exc
from schematizer.logic import schema_repository as schema_repo
from schematizer.models.database import session
from testing import factories
from tests.models.testing_db import DBTestCase


class TestSchemaRepository(DBTestCase):

    @property
    def namespace_name(self):
        return factories.fake_namespace

    @property
    def transformed_namespace_name(self):
        return factories.fake_transformed_namespace

    @property
    def source_name(self):
        return factories.fake_source

    @property
    def source_owner_email(self):
        return factories.fake_owner_email

    @property
    def schema_contains_pii(self):
        return True

    @pytest.fixture
    def namespace(self):
        return factories.create_namespace(self.namespace_name)

    @pytest.fixture
    def source(self, namespace):
        return factories.create_source(self.namespace_name, self.source_name)

    @property
    def topic_name(self):
        return factories.fake_topic_name

    @pytest.fixture
    def topic(self):
        return factories.create_topic(
            topic_name=self.topic_name,
            namespace_name=self.namespace_name,
            source_name=self.source_name
        )

    @property
    def transformed_topic_name(self):
        return factories.fake_transformed_topic_name

    @pytest.fixture
    def transformed_topic(self):
        return factories.create_topic(
            self.transformed_topic_name,
            self.transformed_namespace_name,
            self.source_name
        )

    @property
    def rw_schema_name(self):
        return "foo"

    @property
    def rw_schema_json(self):
        return {
            "name": self.rw_schema_name,
            "namespace": self.namespace_name,
            "type": "record",
            "fields": [{"name": "bar", "type": "int", "doc": "bar"}],
            "doc": "table foo"
        }

    def _build_elements(self, json):
        base_key = "{}.{}".format(json['namespace'], json['name'])
        avro_schema_elements = [
            models.AvroSchemaElement(
                key=base_key,
                element_type="record",
                doc=json['doc']
            )
        ]
        for field in json['fields']:
            avro_schema_elements.append(
                models.AvroSchemaElement(
                    key=models.AvroSchemaElement.compose_key(
                        base_key,
                        field['name']
                    ),
                    element_type='field',
                    doc=field['doc']
                )
            )
        return avro_schema_elements

    @property
    def rw_schema_elements(self):
        return self._build_elements(self.rw_schema_json)

    @pytest.fixture
    def rw_schema(self, topic):
        return factories.create_avro_schema(
            self.rw_schema_json,
            self.rw_schema_elements,
            topic_name=topic.name
        )

    @property
    def rw_transformed_schema_json(self):
        schema_json = copy.deepcopy(self.rw_schema_json)
        schema_json['namespace'] = self.transformed_namespace_name
        schema_json['fields'].append(
            {"name": "bar_str", "type": "string", "doc": "bar_str"}
        )
        return schema_json

    @property
    def rw_transformed_schema_elements(self):
        return self._build_elements(self.rw_transformed_schema_json)

    @pytest.fixture
    def rw_transformed_schema(self, transformed_topic, rw_schema):
        return factories.create_avro_schema(
            self.rw_transformed_schema_json,
            self.rw_transformed_schema_elements,
            topic_name=transformed_topic.name,
            base_schema_id=rw_schema.id
        )

    @property
    def rw_transformed_schema_v2_json(self):
        schema_json = copy.deepcopy(self.rw_transformed_schema_json)
        schema_json['fields'].append(
            {
                "name": "bar_double",
                "type": "double",
                "doc": "bar_double",
                "default": 0.0
            }
        )
        return schema_json

    @property
    def rw_transformed_schema_v2_elements(self):
        return self._build_elements(self.rw_transformed_schema_v2_json)

    @pytest.fixture
    def rw_transformed_v2_schema(self, transformed_topic, rw_schema):
        """Represents an upgrade to the ASTs (v2) which produces a different
        (but compatible) transformed schema from the same base
        """
        return factories.create_avro_schema(
            self.rw_transformed_schema_v2_json,
            self.rw_transformed_schema_v2_elements,
            topic_name=transformed_topic.name,
            base_schema_id=rw_schema.id
        )

    @property
    def another_rw_schema_json(self):
        return {
            "name": self.rw_schema_name,
            "namespace": self.namespace_name,
            "type": "record",
            "fields": [{"name": "baz", "type": "int", "doc": "baz"}],
            "doc": "table foo"
        }

    @property
    def another_rw_schema_elements(self):
        return self._build_elements(self.another_rw_schema_json)

    @pytest.fixture
    def another_rw_schema(self, topic):
        return factories.create_avro_schema(
            self.another_rw_schema_json,
            self.another_rw_schema_elements,
            topic_name=topic.name
        )

    @property
    def another_rw_transformed_schema_json(self):
        schema_json = copy.deepcopy(self.another_rw_schema_json)
        schema_json['namespace'] = self.transformed_namespace_name
        schema_json['fields'].append(
            {"name": "baz_str", "type": "string", "doc": "baz_str"}
        )
        return schema_json

    @property
    def another_rw_transformed_schema_elements(self):
        return self._build_elements(self.another_rw_transformed_schema_json)

    @pytest.fixture
    def another_rw_transformed_schema(
            self,
            transformed_topic,
            another_rw_schema
    ):
        return factories.create_avro_schema(
            self.another_rw_transformed_schema_json,
            self.another_rw_transformed_schema_elements,
            topic_name=transformed_topic.name,
            base_schema_id=another_rw_schema.id
        )

    @property
    def disabled_schema_json(self):
        return {
            "type": "record",
            "name": "disabled",
            "namespace": self.namespace_name,
            "fields": [],
            "doc": "I am disabled!"
        }

    @property
    def disabled_schema_elements(self):
        return self._build_elements(self.disabled_schema_json)

    @pytest.fixture
    def disabled_schema(self, topic):
        return factories.create_avro_schema(
            self.disabled_schema_json,
            self.disabled_schema_elements,
            topic_name=topic.name,
            status=models.AvroSchemaStatus.DISABLED
        )

    @pytest.yield_fixture
    def mock_compatible_func(self):
        with mock.patch(
            'schematizer.logic.schema_repository.'
            'SchemaCompatibilityValidator.is_backward_compatible'
        ) as mock_func:
            yield mock_func

    def test_registering_from_avro_json_with_new_schema(self, namespace):
        expected_base_schema_id = 100
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            self.rw_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            contains_pii=False,
            base_schema_id=expected_base_schema_id
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            base_schema_id=expected_base_schema_id,
            avro_schema_elements=self.rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)

        actual_source = session.query(models.Source).filter(
            models.Source.id == actual_schema.topic.source.id
        ).one()
        expected_source = models.Source(
            namespace_id=namespace.id,
            name=self.source_name,
            owner_email=self.source_owner_email
        )
        self.assert_equal_source_partial(expected_source, actual_source)

    @pytest.mark.usefixtures('rw_schema')
    def test_registering_from_avro_json_with_compatible_schema(
            self,
            topic,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True

        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            self.another_rw_schema_json,
            topic.source.namespace.name,
            topic.source.name,
            topic.source.owner_email,
            contains_pii=False
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.another_rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.another_rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)
        assert topic.id == actual_schema.topic_id

    def assert_new_topic_created_after_schema_register(
        self,
        topic,
        contains_pii
    ):
        actual_schema = schema_repo.register_avro_schema_from_avro_json(
            self.another_rw_schema_json,
            topic.source.namespace.name,
            topic.source.name,
            topic.source.owner_email,
            contains_pii
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.another_rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.another_rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)

        # new topic should be created
        assert topic.id != actual_schema.topic_id
        assert topic.name != actual_schema.topic.name

        # the new topic should still be under the same source
        assert topic.source_id == actual_schema.topic.source_id

    @pytest.mark.usefixtures('rw_schema')
    def test_create_schema_from_avro_json_with_incompatible_schema(
            self,
            topic,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = False
        self.assert_new_topic_created_after_schema_register(
            topic,
            contains_pii=False
        )

    @pytest.mark.usefixtures('rw_schema')
    def test_create_schema_from_avro_json_with_different_pii(
            self,
            topic,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        self.assert_new_topic_created_after_schema_register(
            topic,
            not topic.contains_pii
        )

    def _register_avro_schema(self, avro_schema):
        return schema_repo.register_avro_schema_from_avro_json(
            avro_schema.avro_schema_json,
            avro_schema.topic.source.namespace.name,
            avro_schema.topic.source.name,
            avro_schema.topic.source.owner_email,
            contains_pii=avro_schema.topic.contains_pii,
            base_schema_id=avro_schema.base_schema_id
        )

    def test_registering_from_avro_json_with_same_schema(
            self,
            rw_schema,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        actual = self._register_avro_schema(rw_schema)
        assert rw_schema.id == actual.id

    def test_registering_same_transformed_schema_is_same(
            self,
            rw_transformed_schema
    ):
        result_a1 = self._register_avro_schema(rw_transformed_schema)
        result_a2 = self._register_avro_schema(rw_transformed_schema)
        self.assert_equal_avro_schema_partial(result_a1, result_a2)

    def test_add_different_transformed_schemas_with_same_base_schema(
            self,
            rw_transformed_schema,
            another_rw_transformed_schema
    ):
        # Registering a different transformed schema should result in a
        # different schema/topic
        result_a1 = self._register_avro_schema(rw_transformed_schema)
        result_b = self._register_avro_schema(another_rw_transformed_schema)
        assert result_a1.id != result_b.id
        assert result_a1.topic.id != result_b.topic.id
        assert result_a1.base_schema_id != result_b.base_schema_id

        # Re-registering the original transformed schema will should
        # result in the original's schema/topic
        result_a2 = self._register_avro_schema(rw_transformed_schema)
        assert result_a1.id == result_a2.id
        assert result_a1.topic.id == result_a2.topic.id
        assert result_a1.base_schema_id == result_a2.base_schema_id

    def test_reregistering_compatible_transformed_schema_stays_in_topic(
            self,
            rw_transformed_schema,
            rw_transformed_v2_schema
    ):
        result_a1 = self._register_avro_schema(rw_transformed_schema)
        result_b = self._register_avro_schema(rw_transformed_v2_schema)
        assert result_a1.id != result_b.id
        assert result_a1.topic.id == result_b.topic.id
        assert result_a1.base_schema_id == result_b.base_schema_id
        result_a2 = self._register_avro_schema(rw_transformed_schema)
        assert result_a1.id != result_a2.id
        assert result_a1.topic.id == result_a2.topic.id
        assert result_a1.base_schema_id == result_a2.base_schema_id

    def test_registering_same_schema_twice(
            self,
            topic,
            rw_schema
    ):
        result_a = self._register_avro_schema(rw_schema)
        result_b = self._register_avro_schema(rw_schema)

        # new schema should be created for the same topic
        assert rw_schema.id == result_a.id
        assert topic.id == result_a.topic_id
        assert rw_schema.id == result_b.id
        assert topic.id == result_b.topic_id
        expected = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.rw_schema_elements,
            base_schema_id=rw_schema.base_schema_id
        )
        self.assert_equal_avro_schema_partial(expected, result_a)
        self.assert_equal_avro_schema_partial(expected, result_b)

    def test_registering_from_avro_json_with_diff_base_schema(
            self,
            topic,
            rw_schema,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        expected_base_schema_id = 100

        actual = schema_repo.register_avro_schema_from_avro_json(
            rw_schema.avro_schema_json,
            rw_schema.topic.source.namespace.name,
            rw_schema.topic.source.name,
            rw_schema.topic.source.owner_email,
            contains_pii=False,
            base_schema_id=expected_base_schema_id
        )

        # new schema should be created for a new topic
        assert rw_schema.id != actual.id
        assert topic.id != actual.topic_id
        expected = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.rw_schema_elements,
            base_schema_id=expected_base_schema_id
        )
        self.assert_equal_avro_schema_partial(expected, actual)

    def test_get_latest_topic_of_namespace_source(
        self,
        namespace,
        source,
        topic
    ):
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        self.assert_equal_topic(topic, actual)
        new_topic = factories.create_topic(
            topic_name='new_topic',
            namespace_name=source.namespace.name,
            source_name=source.name
        )
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        self.assert_equal_topic(new_topic, actual)

    def test_get_latest_topic_of_source_id(self, source, topic):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        self.assert_equal_topic(topic, actual)

        new_topic = factories.create_topic(
            topic_name='new_topic',
            namespace_name=source.namespace.name,
            source_name=source.name
        )
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        self.assert_equal_topic(new_topic, actual)

    def test_get_latest_topic_of_source_with_no_topic(self, namespace, source):
        factories.SourceFactory.delete_topics(source.id)
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        assert actual is None

    def test_get_latest_topic_of_source_with_nonexistent_source(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.get_latest_topic_of_namespace_source('foo', 'bar')

    def test_get_latest_topic_of_source_id_with_no_topic(self, source):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        assert actual is None

    def test_get_latest_topic_of_source_id_with_nonexistent_source(self):
        actual = schema_repo.get_latest_topic_of_source_id(0)
        assert actual is None

    @pytest.mark.usefixtures('source', 'rw_schema', 'disabled_schema')
    @pytest.mark.parametrize("is_compatible", [True, False])
    def test_is_schema_compatible_in_topic(
            self,
            topic,
            mock_compatible_func,
            is_compatible
    ):
        mock_compatible_func.return_value = is_compatible
        actual = schema_repo.is_schema_compatible_in_topic(
            self.rw_schema_json,
            topic.name
        )
        assert is_compatible == actual

    @pytest.mark.usefixtures('disabled_schema')
    def test_is_schema_compatible_in_topic_with_no_enabled_schema(self, topic):
        actual = schema_repo.is_schema_compatible_in_topic('int', topic.name)
        assert actual is True

    @pytest.mark.usefixtures('disabled_schema', 'rw_schema')
    def test_is_schema_compatible_in_topic_with_bad_topic_name(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.is_schema_compatible_in_topic('int', 'foo')

    def test_get_topic_by_name(self, topic):
        actual = schema_repo.get_topic_by_name(self.topic_name)
        self.assert_equal_topic(topic, actual)

    def test_get_topic_by_name_with_nonexistent_topic(self):
        actual = schema_repo.get_topic_by_name('foo')
        assert actual is None

    def test_get_source_by_fullname(self, source):
        actual = schema_repo.get_source_by_fullname(
            self.namespace_name,
            self.source_name
        )
        self.assert_equal_source(source, actual)

    def test_get_source_by_fullname_with_nonexistent_source(self):
        actual = schema_repo.get_source_by_fullname('foo', 'bar')
        assert actual is None

    def test_get_schema_by_id(self, rw_schema):
        actual = schema_repo.get_schema_by_id(rw_schema.id)
        self.assert_equal_avro_schema(rw_schema, actual)

    def test_get_schema_by_id_with_nonexistent_schema(self):
        actual = schema_repo.get_schema_by_id(0)
        assert actual is None

    def test_get_latest_schema_by_topic_id(self, topic, rw_schema):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        self.assert_equal_avro_schema(rw_schema, actual)

    def test_get_latest_schema_by_topic_id_with_nonexistent_topic(self):

        actual = schema_repo.get_latest_schema_by_topic_id(0)
        assert actual is None

    def test_get_latest_schema_by_topic_id_with_empty_topic(self, topic):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        assert actual is None

    @pytest.mark.usefixtures('disabled_schema')
    def test_get_latest_schema_by_topic_id_with_all_disabled_schema(
            self,
            topic
    ):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        assert actual is None

    def test_get_latest_schema_by_topic_name(self, topic, rw_schema):
        actual = schema_repo.get_latest_schema_by_topic_name(topic.name)
        self.assert_equal_avro_schema(rw_schema, actual)

    def test_get_latest_schema_by_topic_name_with_nonexistent_topic(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.get_latest_schema_by_topic_name('_bad.topic')

    @pytest.mark.usefixtures('rw_schema', 'disabled_schema')
    @pytest.mark.parametrize("is_compatible", [True, False])
    def test_is_schema_compatible(self, mock_compatible_func, is_compatible):
        mock_compatible_func.return_value = is_compatible
        target_schema = 'avro schema to be validated'
        actual = schema_repo.is_schema_compatible(
            target_schema,
            self.namespace_name,
            self.source_name
        )
        expected = mock_compatible_func.return_value
        assert expected == actual

    def test_is_schema_compatible_with_nonexistent_source(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.is_schema_compatible('avro schema', 'foo', 'bar')

    def test_get_schemas_by_topic_name(self, topic, rw_schema):
        actual = schema_repo.get_schemas_by_topic_name(topic.name)
        assert 1 == len(actual)
        self.assert_equal_avro_schema(rw_schema, actual[0])

    def test_get_schemas_by_topic_name_including_disabled(
            self,
            topic,
            rw_schema,
            disabled_schema
    ):
        actual = schema_repo.get_schemas_by_topic_name(topic.name, True)
        self.assert_equal_entities(
            expected_entities=[rw_schema, disabled_schema],
            actual_entities=actual,
            assert_func=self.assert_equal_avro_schema
        )

    def test_get_schemas_by_topic_name_with_nonexistent_topic(self):
        with pytest.raises(sch_exc.EntityNotFoundException):
            schema_repo.get_schemas_by_topic_name('foo')

    def test_get_schemas_by_topic_id(self, topic, rw_schema):
        actual = schema_repo.get_schemas_by_topic_id(topic.id)
        assert 1 == len(actual)
        self.assert_equal_avro_schema(rw_schema, actual[0])

    def test_get_schemas_by_topic_id_including_disabled(
            self,
            topic,
            rw_schema,
            disabled_schema
    ):
        actual = schema_repo.get_schemas_by_topic_id(topic.id, True)
        self.assert_equal_entities(
            expected_entities=[rw_schema, disabled_schema],
            actual_entities=actual,
            assert_func=self.assert_equal_avro_schema
        )

    def test_get_schemas_by_topic_id_with_nonexistent_topic(self):
        actual = schema_repo.get_schemas_by_topic_id(0)
        assert [] == actual

    def test_mark_schema_disabled(self, rw_schema):
        schema_repo.mark_schema_disabled(rw_schema.id)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.DISABLED == actual.status

    def test_mark_schema_disabled_with_nonexisted_schema(self, rw_schema):
        # nothing should happen
        schema_repo.mark_schema_disabled(0)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

    def test_mark_schema_readonly(self, rw_schema):
        schema_repo.mark_schema_readonly(rw_schema.id)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_ONLY == actual.status

    def test_mark_schema_readonly_with_nonexisted_schema(self, rw_schema):
        # nothing should happen
        schema_repo.mark_schema_readonly(0)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

    def test_get_sources(self, source):
        actual = schema_repo.get_sources()
        assert 1 == len(actual)
        self.assert_equal_source(source, actual[0])

    @pytest.mark.usefixtures('source')
    def test_get_namespaces(self, namespace):
        factories.SourceFactory.create('another source', namespace)
        actual = schema_repo.get_namespaces()
        assert 1 == len(actual)
        self.assert_equal_namespace(namespace, actual[0])

    def test_get_sources_by_namespace(self, source):
        namespace = factories.NamespaceFactory.create('another namespace')
        factories.SourceFactory.create('another source', namespace)
        actual = schema_repo.get_sources_by_namespace(self.namespace_name)
        assert 1 == len(actual)
        self.assert_equal_source(source, actual[0])

    @pytest.mark.usefixtures('source')
    def test_get_sources_by_namespace_with_nonexistent_namespace(self):
        actual = schema_repo.get_sources_by_namespace('foo')
        assert 0 == len(actual)

    def test_get_topics_by_source_id(self, source, topic):
        actual = schema_repo.get_topics_by_source_id(source.id)
        assert 1 == len(actual)
        self.assert_equal_topic(topic, actual[0])

    def test_available_converters(self):
        expected = {
            (models.SchemaKindEnum.MySQL, models.SchemaKindEnum.Avro):
            converters.MySQLToAvroConverter,
            (models.SchemaKindEnum.Avro, models.SchemaKindEnum.Redshift):
            converters.AvroToRedshiftConverter
        }
        for key, value in expected.iteritems():
            actual = schema_repo.converters[key]
            source_type, target_type = key
            assert source_type == actual.source_type
            assert target_type == actual.target_type
            assert value == actual

    def test_convert_schema(self):
        with mock.patch.object(
            converters.MySQLToAvroConverter,
            'convert'
        ) as mock_converter:
            schema_repo.convert_schema(
                models.SchemaKindEnum.MySQL,
                models.SchemaKindEnum.Avro,
                self.rw_schema_json
            )
            mock_converter.assert_called_once_with(self.rw_schema_json)

    def test_convert_schema_with_no_suitable_converter(self):
        with pytest.raises(Exception):
            schema_repo.convert_schema(
                mock.Mock(),
                mock.Mock(),
                self.rw_schema_json
            )

    def test_get_schema_elements_with_no_schema(self):
        actual = schema_repo.get_schema_elements_by_schema_id(1)
        assert 0 == len(actual)

    def test_get_schema_elements_by_schema_id(self, rw_schema):
        actual = schema_repo.get_schema_elements_by_schema_id(rw_schema.id)
        for i in range(len(self.rw_schema_elements)):
            self.assert_equal_avro_schema_element_partial(
                actual[i],
                self.rw_schema_elements[i]
            )

    def assert_equal_namespace(self, expected, actual):
        assert expected.id == actual.id
        assert expected.name == actual.name
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    def assert_equal_source_partial(self, expected, actual):
        assert expected.namespace_id == actual.namespace_id
        assert expected.name == actual.name
        assert expected.owner_email == actual.owner_email

    def assert_equal_source(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_source_partial(expected, actual)

    def assert_equal_topic_partial(self, expected, actual):
        assert expected.name == actual.name

    def assert_equal_topic(self, expected, actual):
        assert expected.id == actual.id
        assert expected.source_id == actual.source_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_topic_partial(expected, actual)

    def assert_equal_avro_schema_partial(self, expected, actual):
        assert expected.avro_schema == actual.avro_schema
        assert expected.base_schema_id == actual.base_schema_id
        assert expected.status == actual.status
        self.assert_equal_entities(
            expected.avro_schema_elements,
            actual.avro_schema_elements,
            self.assert_equal_avro_schema_element_partial,
            filter_key='key'
        )

    def assert_equal_avro_schema(self, expected, actual):
        assert expected.id == actual.id
        assert expected.avro_schema == actual.avro_schema
        assert expected.topic_id == actual.topic_id
        assert expected.base_schema_id == actual.base_schema_id
        assert expected.status == actual.status
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_entities(
            expected.avro_schema_elements,
            actual.avro_schema_elements,
            self.assert_equal_avro_schema_element
        )

    def assert_equal_entities(
            self,
            expected_entities,
            actual_entities,
            assert_func,
            filter_key='id',
    ):
        assert len(expected_entities) == len(actual_entities)
        for actual_elem in actual_entities:
            expected_elem = next(
                o for o in expected_entities
                if getattr(o, filter_key) == getattr(actual_elem, filter_key)
            )
            assert_func(expected_elem, actual_elem)

    def assert_equal_avro_schema_element_partial(self, expected, actual):
        assert expected.key == actual.key
        assert expected.element_type == actual.element_type
        assert expected.doc == actual.doc

    def assert_equal_avro_schema_element(self, expected, actual):
        assert expected.id == actual.id
        assert expected.avro_schema_id == actual.avro_schema_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_avro_schema_element_partial(expected, actual)


@pytest.mark.usefixtures('sorted_topics')
class TestGetTopicsByCriteira(DBTestCase):

    @pytest.fixture
    def yelp_namespace(self):
        return factories.create_namespace(namespace_name='yelp')

    @pytest.fixture
    def aux_namespace(self):
        return factories.create_namespace(namespace_name='aux')

    @pytest.fixture
    def biz_source(self, yelp_namespace):
        return factories.create_source(yelp_namespace.name, source_name='biz')

    @pytest.fixture
    def user_source(self, yelp_namespace):
        return factories.create_source(yelp_namespace.name, source_name='user')

    @pytest.fixture
    def cta_source(self, aux_namespace):
        return factories.create_source(aux_namespace.name, source_name='cta')

    @property
    def some_datetime(self):
        return datetime.datetime(2015, 3, 1, 10, 23, 5, 254)

    @pytest.fixture
    def biz_topic(self, biz_source):
        return factories.create_topic(
            topic_name='yelp.biz.topic.1',
            namespace_name=biz_source.namespace.name,
            source_name=biz_source.name,
            created_at=self.some_datetime + datetime.timedelta(seconds=3)
        )

    @pytest.fixture
    def user_topic_1(self, user_source):
        return factories.create_topic(
            topic_name='yelp.user.topic.1',
            namespace_name=user_source.namespace.name,
            source_name=user_source.name,
            created_at=self.some_datetime - datetime.timedelta(seconds=1)
        )

    @pytest.fixture
    def user_topic_2(self, user_source):
        return factories.create_topic(
            topic_name='yelp.user.topic.two',
            namespace_name=user_source.namespace.name,
            source_name=user_source.name,
            created_at=self.some_datetime + datetime.timedelta(seconds=5)
        )

    @pytest.fixture
    def cta_topic(self, cta_source):
        return factories.create_topic(
            topic_name='aux.cta.topic.1',
            namespace_name=cta_source.namespace.name,
            source_name=cta_source.name,
            created_at=self.some_datetime + datetime.timedelta(minutes=1)
        )

    @pytest.fixture
    def sorted_topics(self, user_topic_1, user_topic_2, biz_topic, cta_topic):
        return sorted(
            [user_topic_1, biz_topic, user_topic_2, cta_topic],
            key=lambda topic: topic.created_at
        )

    def test_get_topics_after_given_timestamp(self, sorted_topics):
        expected = sorted_topics[2:]
        after_dt = expected[0].created_at

        actual = schema_repo.get_topics_by_criteria(created_after=after_dt)

        self.assert_equal_topics(actual, expected)
        assert all(topic.created_at >= after_dt for topic in actual)

    def test_no_newer_topic(self, sorted_topics):
        last_topic = sorted_topics[-1]
        after_dt = last_topic.created_at + datetime.timedelta(seconds=1)
        actual = schema_repo.get_topics_by_criteria(created_after=after_dt)
        assert actual == []

    def test_get_biz_source_only(self, biz_topic, biz_source):
        actual = schema_repo.get_topics_by_criteria(source=biz_source.name)
        self.assert_equal_topics(actual, [biz_topic])

    def test_get_yelp_namespace_only(
        self,
        biz_topic,
        user_topic_1,
        user_topic_2,
        yelp_namespace
    ):
        self.assert_equal_topics(
            actual_topics=schema_repo.get_topics_by_criteria(
                namespace=yelp_namespace.name
            ),
            expected_topics=self._sort_topics_by_id(
                [user_topic_1, user_topic_2, biz_topic]
            )
        )

    def test_get_user_topics_only(
        self,
        user_topic_1,
        user_topic_2,
        user_source,
        yelp_namespace
    ):
        self.assert_equal_topics(
            actual_topics=schema_repo.get_topics_by_criteria(
                namespace=yelp_namespace.name,
                source=user_source.name
            ),
            expected_topics=self._sort_topics_by_id(
                [user_topic_1, user_topic_2]
            )
        )

    def assert_equal_topics(self, expected_topics, actual_topics):
        assert len(actual_topics) == len(expected_topics)
        for i, actual_topic in enumerate(actual_topics):
            assert actual_topic == expected_topics[i]

    def _sort_topics_by_id(self, topics):
        return sorted(topics, key=lambda topic: topic.id)
