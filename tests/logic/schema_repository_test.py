# -*- coding: utf-8 -*-
import mock
import pytest

from schematizer import models
from schematizer.components import converters
from schematizer.logic import schema_repository as schema_repo
from schematizer.models.database import session
from testing import factories
from tests.models.testing_db import DBTestCase


class TestSchemaRepository(DBTestCase):

    @property
    def namespace(self):
        return factories.fake_namespace

    @property
    def source(self):
        return factories.fake_source

    @property
    def domain_owner_email(self):
        return factories.fake_owner_email

    @pytest.fixture
    def domain(self):
        return factories.create_domain(self.namespace, self.source)

    @property
    def topic_name(self):
        return factories.fake_topic_name

    @pytest.fixture
    def topic(self):
        return factories.create_topic(
            self.topic_name,
            self.namespace,
            self.source
        )

    @property
    def rw_schema_json(self):
        return {
            "name": "foo",
            "namespace": "yelp",
            "type": "record",
            "fields": [{"name": "bar", "type": "int", "doc": "bar"}],
            "doc": "table foo"
        }

    @property
    def rw_schema_elements(self):
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
    def rw_schema(self, topic):
        return factories.create_avro_schema(
            self.rw_schema_json,
            self.rw_schema_elements,
            topic_name=topic.name
        )

    @property
    def another_rw_schema_json(self):
        return {
            "name": "foo",
            "namespace": "yelp",
            "type": "record",
            "fields": [{"name": "baz", "type": "int", "doc": "baz"}],
            "doc": "table foo"
        }

    @property
    def another_rw_schema_elements(self):
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
    def another_rw_schema(self, topic):
        return factories.create_avro_schema(
            self.another_rw_schema_json,
            self.another_rw_schema_elements,
            topic_name=topic.name
        )

    @property
    def disabled_schema_json(self):
        return {
            "type": "record",
            "name": "disabled",
            "fields": [],
            "doc": "I am disabled!"
        }

    @property
    def disabled_schema_elements(self):
        return [
            models.AvroSchemaElement(
                key="disabled",
                element_type="record",
                doc="I am disabled!"
            )
        ]

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

    def test_create_schema_from_avro_json_with_new_schema(self):
        expected_base_schema_id = 100

        actual_schema = schema_repo.create_avro_schema_from_avro_json(
            self.rw_schema_json,
            self.namespace,
            self.source,
            self.domain_owner_email,
            base_schema_id=expected_base_schema_id
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            base_schema_id=expected_base_schema_id,
            avro_schema_elements=self.rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)

        actual_domain = session.query(models.Domain).filter(
            models.Domain.id == actual_schema.topic.domain.id
        ).one()
        expected_domain = models.Domain(
            namespace=self.namespace,
            source=self.source,
            owner_email=self.domain_owner_email
        )
        self.assert_equal_domain_partial(expected_domain, actual_domain)

    @pytest.mark.usefixtures('rw_schema')
    def test_create_schema_from_avro_json_with_compatible_schema(
            self,
            topic,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True

        actual_schema = schema_repo.create_avro_schema_from_avro_json(
            self.another_rw_schema_json,
            topic.domain.namespace,
            topic.domain.source,
            topic.domain.owner_email
        )

        expected_schema = models.AvroSchema(
            avro_schema_json=self.another_rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.another_rw_schema_elements
        )
        self.assert_equal_avro_schema_partial(expected_schema, actual_schema)
        assert topic.id == actual_schema.topic_id

    @pytest.mark.usefixtures('rw_schema')
    def test_create_schema_from_avro_json_with_incompatible_schema(
            self,
            topic,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = False

        actual_schema = schema_repo.create_avro_schema_from_avro_json(
            self.another_rw_schema_json,
            topic.domain.namespace,
            topic.domain.source,
            topic.domain.owner_email
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

        # the new topic should still be under the same namespace and domain
        assert topic.domain_id == actual_schema.topic.domain_id

    def test_create_schema_from_avro_json_with_same_schema(
            self,
            rw_schema,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True

        actual = schema_repo.create_avro_schema_from_avro_json(
            self.rw_schema_json,
            rw_schema.topic.domain.namespace,
            rw_schema.topic.domain.source,
            rw_schema.topic.domain.owner_email,
        )

        assert rw_schema.id == actual.id

    def test_create_schema_from_avro_json_with_diff_base_schema(
            self,
            topic,
            rw_schema,
            mock_compatible_func
    ):
        mock_compatible_func.return_value = True
        expected_base_schema_id = 100

        actual = schema_repo.create_avro_schema_from_avro_json(
            self.rw_schema_json,
            rw_schema.topic.domain.namespace,
            rw_schema.topic.domain.source,
            rw_schema.topic.domain.owner_email,
            base_schema_id=expected_base_schema_id
        )

        # new schema should be created for the same topic
        assert rw_schema.id != actual.id
        assert topic.id == actual.topic_id
        expected = models.AvroSchema(
            avro_schema_json=self.rw_schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=self.rw_schema_elements,
            base_schema_id=expected_base_schema_id
        )
        self.assert_equal_avro_schema_partial(expected, actual)

    def test_create_schema_from_avro_json_with_missing_doc(self, topic):
        schema_json = {
            "type": "record",
            "name": "foo",
            "fields": [{"name": "bar", "type": "int"}],
            "doc": "table foo"
        }
        with pytest.raises(schema_repo.MissingDocException):
            schema_repo.create_avro_schema_from_avro_json(
                schema_json,
                topic.domain.namespace,
                topic.domain.source,
                topic.domain.owner_email
            )

    def test_create_schema_from_avro_json_with_missing_doc_but_not_required(
            self,
            topic
    ):
        schema_json = {"type": "enum", "name": "foo", "symbols": ["a"]}
        actual = schema_repo.create_avro_schema_from_avro_json(
            schema_json,
            topic.domain.namespace,
            topic.domain.source,
            topic.domain.owner_email
        )
        expected = models.AvroSchema(
            avro_schema_json=schema_json,
            status=models.AvroSchemaStatus.READ_AND_WRITE,
            avro_schema_elements=[
                models.AvroSchemaElement(key="foo", element_type="enum")
            ]
        )
        self.assert_equal_avro_schema_partial(expected, actual)

    def test_get_latest_topic_of_domain(self, domain, topic):
        actual = schema_repo.get_latest_topic_of_domain(
            domain.namespace,
            domain.source
        )
        self.assert_equal_topic(topic, actual)

        new_topic = factories.create_topic(
            'new_topic',
            namespace=domain.namespace,
            source=domain.source
        )
        actual = schema_repo.get_latest_topic_of_domain(
            domain.namespace,
            domain.source
        )
        self.assert_equal_topic(new_topic, actual)

    def test_get_latest_topic_of_domain_with_no_topic(self, domain):
        actual = schema_repo.get_latest_topic_of_domain(
            domain.namespace,
            domain.source
        )
        assert actual is None

    def test_get_latest_topic_of_domain_with_nonexistent_domain(self):
        with pytest.raises(schema_repo.EntityNotFoundException):
            schema_repo.get_latest_topic_of_domain('foo', 'bar')

    def test_get_latest_topic_of_domain_id(self, domain, topic):
        actual = schema_repo.get_latest_topic_of_domain_id(domain.id)
        self.assert_equal_topic(topic, actual)

        new_topic = factories.create_topic(
            'new_topic',
            namespace=domain.namespace,
            source=domain.source
        )
        actual = schema_repo.get_latest_topic_of_domain_id(domain.id)
        self.assert_equal_topic(new_topic, actual)

    def test_get_latest_topic_of_domain_id_with_no_topic(self, domain):
        actual = schema_repo.get_latest_topic_of_domain_id(domain.id)
        assert actual is None

    def test_get_latest_topic_of_domain_id_with_nonexistent_domain(self):
        actual = schema_repo.get_latest_topic_of_domain_id(0)
        assert actual is None

    @pytest.mark.usefixtures('domain', 'rw_schema', 'disabled_schema')
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
        assert True == actual

    @pytest.mark.usefixtures('disabled_schema', 'rw_schema')
    def test_is_schema_compatible_in_topic_with_bad_topic_name(self):
        with pytest.raises(schema_repo.EntityNotFoundException):
            schema_repo.is_schema_compatible_in_topic('int', 'foo')

    def test_get_topic_by_name(self, topic):
        actual = schema_repo.get_topic_by_name(self.topic_name)
        self.assert_equal_topic(topic, actual)

    def test_get_topic_by_name_with_nonexistent_topic(self):
        actual = schema_repo.get_topic_by_name('foo')
        assert actual is None

    def test_get_domain_by_fullname(self, domain):
        actual = schema_repo.get_domain_by_fullname(
            self.namespace,
            self.source
        )
        self.assert_equal_domain(domain, actual)

    def test_get_domain_by_fullname_with_nonexistent_domain(self):
        actual = schema_repo.get_domain_by_fullname('foo', 'bar')
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
        with pytest.raises(schema_repo.EntityNotFoundException):
            schema_repo.get_latest_schema_by_topic_name('_bad.topic')

    @pytest.mark.usefixtures('rw_schema', 'disabled_schema')
    @pytest.mark.parametrize("is_compatible", [True, False])
    def test_is_schema_compatible(self, mock_compatible_func, is_compatible):
        mock_compatible_func.return_value = is_compatible
        target_schema = 'avro schema to be validated'
        actual = schema_repo.is_schema_compatible(
            target_schema,
            self.namespace,
            self.source
        )
        expected = mock_compatible_func.return_value
        assert expected == actual

    def test_is_schema_compatible_with_nonexistent_domain(self):
        with pytest.raises(schema_repo.EntityNotFoundException):
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
        with pytest.raises(schema_repo.EntityNotFoundException):
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

    def test_get_domains(self, domain):
        actual = schema_repo.get_domains()
        assert 1 == len(actual)
        self.assert_equal_domain(domain, actual[0])

    @pytest.mark.usefixtures('domain')
    def test_get_namespaces(self):
        factories.DomainFactory.create(self.namespace, 'another source')
        actual = schema_repo.get_namespaces()
        assert 1 == len(actual)
        assert self.namespace == actual[0]

    def test_get_domains_by_namespace(self, domain):
        factories.DomainFactory.create('another namespace', 'another source')
        actual = schema_repo.get_domains_by_namespace(self.namespace)
        assert 1 == len(actual)
        self.assert_equal_domain(domain, actual[0])

    @pytest.mark.usefixtures('domain')
    def test_get_domains_by_namespace_with_nonexistent_namespace(self):
        actual = schema_repo.get_domains_by_namespace('foo')
        assert 0 == len(actual)

    def test_get_topics_by_domain_id(self, domain, topic):
        actual = schema_repo.get_topics_by_domain_id(domain.id)
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

    def assert_equal_domain_partial(self, expected, actual):
        assert expected.namespace == actual.namespace
        assert expected.source == actual.source
        assert expected.owner_email == actual.owner_email

    def assert_equal_domain(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_domain_partial(expected, actual)

    def assert_equal_topic_partial(self, expected, actual):
        assert expected.name == actual.name

    def assert_equal_topic(self, expected, actual):
        assert expected.id == actual.id
        assert expected.domain_id == actual.domain_id
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
