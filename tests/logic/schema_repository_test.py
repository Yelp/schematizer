# -*- coding: utf-8 -*-
import mock
import pytest
import simplejson

from schematizer import models
from schematizer.components import converters
from schematizer.logic import schema_repository as schema_repo
from schematizer.models.database import session
from testing import factories
from tests.models.testing_db import DBTestCase


class TestSchemaRepository(DBTestCase):

    @property
    def namespace_name(self):
        return factories.fake_namespace

    @property
    def source_name(self):
        return factories.fake_source

    @property
    def source_owner_email(self):
        return factories.fake_owner_email

    @pytest.fixture
    def namespace(self):
        return factories.NamespaceFactory.create_in_db(self.namespace_name)

    @pytest.fixture
    def source(self, namespace):
        return factories.SourceFactory.create_in_db(
            self.source_name,
            namespace
        )

    @property
    def topic_name(self):
        return factories.fake_topic_name

    @pytest.fixture
    def topic(self, source):
        return factories.TopicFactory.create_in_db(self.topic_name, source)

    @property
    def disabled_avro_schema_string(self):
        return 'disabled avro schema'

    @property
    def rw_avro_schema_string(self):
        return ('{"name": "business", "namespace": "yelp", '
                '"fields": [{"name": "col", "type": "int"}]}')

    @property
    def rw_avro_schema_json(self):
        return simplejson.loads(self.rw_avro_schema_string)

    @pytest.fixture
    def avro_schemas(self, topic):
        disabled_schema = factories.AvroSchemaFactory.create_in_db(
            self.disabled_avro_schema_string,
            topic,
            status=models.AvroSchemaStatus.DISABLED
        )
        enabled_schema = factories.AvroSchemaFactory.create_in_db(
            self.rw_avro_schema_string,
            topic,
            status=models.AvroSchemaStatus.READ_AND_WRITE
        )
        return [disabled_schema, enabled_schema]

    @pytest.fixture
    def disabled_avro_schema(self, avro_schemas):
        return avro_schemas[0]

    @pytest.fixture
    def rw_avro_schema(self, avro_schemas):
        return avro_schemas[1]

    @pytest.yield_fixture(params=[True, False])
    def mock_compatible_func(self, request):
        target = ('schematizer.logic.schema_repository.'
                  'SchemaCompatibilityValidator.is_backward_compatible')
        with mock.patch(target, return_value=request.param) as mock_func:
            yield mock_func

    def get_namespace(self, name):
        return session.query(
            models.Namespace
        ).filter(
            models.Namespace.name == name
        ).one()

    def get_source(self, namespace, name):
        return session.query(
            models.Source
        ).filter(
            models.Source.namespace_id == namespace.id,
            models.Source.name == name
        ).one()

    def test_create_schema_from_avro_json(self):
        expected_base_schema_id = 100
        actual = schema_repo.create_avro_schema_from_avro_json(
            self.rw_avro_schema_json,
            self.namespace_name,
            self.source_name,
            self.source_owner_email,
            base_schema_id=expected_base_schema_id
        )
        assert self.rw_avro_schema_json == simplejson.loads(actual.avro_schema)
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status
        assert expected_base_schema_id == actual.base_schema_id

        created_namespace = self.get_namespace(self.namespace_name)
        created_source = self.get_source(created_namespace, self.source_name)

        assert created_source.id == actual.topic.source.id
        assert created_namespace.id == created_source.namespace_id
        assert self.source_owner_email == created_source.owner_email

    @pytest.mark.usefixtures('avro_schemas')
    def test_create_schema_from_avro_json_with_existing_topic(
            self,
            topic,
            mock_compatible_func
    ):
        schema_to_register = "new schema"
        actual = schema_repo.create_avro_schema_from_avro_json(
            schema_to_register,
            self.namespace_name,
            self.source_name,
            self.source_owner_email
        )
        actual_schema_json = simplejson.loads(actual.avro_schema)

        assert schema_to_register == actual_schema_json
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

        if mock_compatible_func.return_value:
            # schema is compatible with the topic
            assert topic.id == actual.topic_id
        else:
            # schema is not compatible and new topic is created
            created_topic = session.query(
                models.Topic
            ).filter(
                models.Topic.id == actual.topic_id
            ).one()
            assert created_topic.name != topic.name
            assert created_topic.id != topic.id
            assert created_topic.source_id == topic.source_id

            expected_namespace = self.get_namespace(self.namespace_name)
            expected_source = self.get_source(
                expected_namespace,
                self.source_name
            )
            assert expected_source.id == created_topic.source_id
            assert expected_source.id == actual.topic.source.id

    @pytest.mark.usefixtures('mock_compatible_func')
    def test_create_schema_from_avro_json_with_same_schema(
            self,
            rw_avro_schema,
            mock_compatible_func
    ):
        if mock_compatible_func.return_value:
            # only test the case with compatible schemas
            actual = schema_repo.create_avro_schema_from_avro_json(
                self.rw_avro_schema_json,
                self.namespace_name,
                self.source_name,
                self.source_owner_email,
            )
            assert rw_avro_schema.id == actual.id

    def test_create_schema_from_avro_json_with_diff_base_schema(
            self,
            topic,
            rw_avro_schema,
            mock_compatible_func
    ):
        if mock_compatible_func.return_value:
            expected_base_schema_id = 100
            actual = schema_repo.create_avro_schema_from_avro_json(
                self.rw_avro_schema_json,
                self.namespace_name,
                self.source_name,
                self.source_owner_email,
                base_schema_id=expected_base_schema_id
            )
            actual_schema_json = simplejson.loads(actual.avro_schema)
            assert rw_avro_schema.id != actual.id
            assert self.rw_avro_schema_json == actual_schema_json
            assert topic.id == actual.topic_id
            assert expected_base_schema_id == actual.base_schema_id

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
        self.verify_topic(topic, actual)

        new_topic = factories.TopicFactory.create_in_db('new_topic', source)
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        self.verify_topic(new_topic, actual)

    def test_get_latest_topic_of_source_id(self, source, topic):
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        self.verify_topic(topic, actual)

        new_topic = factories.TopicFactory.create_in_db('new_topic', source)
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        self.verify_topic(new_topic, actual)

    def verify_topic(self, expected, actual):
        assert expected.id == actual.id
        assert expected.name == actual.name
        assert expected.source_id == actual.source_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    def test_get_latest_topic_of_source_with_no_topic(self, namespace, source):
        factories.SourceFactory.delete_topics(source.id)
        actual = schema_repo.get_latest_topic_of_namespace_source(
            namespace.name,
            source.name
        )
        assert actual is None

    def test_get_latest_topic_of_source_with_nonexistent_source(self):
        actual = schema_repo.get_latest_topic_of_namespace_source('foo', 'bar')
        assert actual is None

    def test_get_latest_topic_of_source_id_with_no_topic(self, source):
        factories.SourceFactory.delete_topics(source.id)
        actual = schema_repo.get_latest_topic_of_source_id(source.id)
        assert actual is None

    def test_get_latest_topic_of_source_id_with_nonexistent_source(self):
        actual = schema_repo.get_latest_topic_of_source_id(0)
        assert actual is None

    @pytest.mark.usefixtures('source', 'avro_schemas')
    def test_is_schema_compatible_in_topic(self, topic, mock_compatible_func):
        actual = schema_repo.is_schema_compatible_in_topic(
            self.rw_avro_schema_json,
            topic.name
        )
        expected = mock_compatible_func.return_value
        assert expected == actual

    def test_is_schema_compatible_in_topic_with_no_enabled_schema(
            self,
            topic,
            rw_avro_schema
    ):
        factories.AvroSchemaFactory.delete(rw_avro_schema.id)
        actual = schema_repo.is_schema_compatible_in_topic('avro', topic.name)
        assert actual is True

    @pytest.mark.usefixtures('avro_schemas')
    def test_is_schema_compatible_in_topic_with_bad_topic_name(self):
        actual = schema_repo.is_schema_compatible_in_topic('avro', 'foo')
        assert actual is True

    def test_get_topic_by_name(self, topic):
        actual = schema_repo.get_topic_by_name(self.topic_name)
        assert topic.id == actual.id
        assert topic.name == actual.name
        assert topic.source_id == actual.source_id
        assert topic.created_at == actual.created_at
        assert topic.updated_at == actual.updated_at

    def test_get_topic_by_name_with_nonexistent_topic(self):
        actual = schema_repo.get_topic_by_name('foo')
        assert actual is None

    def test_get_source_by_fullname(self, source):
        actual = schema_repo.get_source_by_fullname(
            self.namespace_name,
            self.source_name
        )
        assert source.id == actual.id
        assert source.namespace_id == actual.namespace_id
        assert source.name == actual.name
        assert source.created_at == actual.created_at
        assert source.updated_at == actual.updated_at

    def test_get_source_by_fullname_with_nonexistent_source(self):
        actual = schema_repo.get_source_by_fullname('foo', 'bar')
        assert actual is None

    def test_get_schema_by_id(self, rw_avro_schema):
        actual = schema_repo.get_schema_by_id(rw_avro_schema.id)
        self.verify_avro_schema(rw_avro_schema, actual)

    def verify_avro_schema(self, expected, actual):
        assert expected.id == actual.id
        assert expected.avro_schema == actual.avro_schema
        assert expected.topic_id == actual.topic_id
        assert expected.base_schema_id == actual.base_schema_id
        assert expected.status == actual.status
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    def test_get_schema_by_id_with_nonexistent_schema(self):
        actual = schema_repo.get_schema_by_id(0)
        assert actual is None

    def test_get_latest_schema_by_topic_id(self, topic, rw_avro_schema):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        self.verify_avro_schema(rw_avro_schema, actual)

    def test_get_latest_schema_by_topic_id_with_nonexistent_topic(self):
        actual = schema_repo.get_latest_schema_by_topic_id(0)
        assert actual is None

    def test_get_latest_schema_by_topic_id_with_empty_topic(self, topic):
        factories.TopicFactory.delete_avro_schemas(topic.id)
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        assert actual is None

    def test_get_latest_schema_by_topic_id_with_all_disabled_schema(
            self,
            topic,
            rw_avro_schema
    ):
        factories.AvroSchemaFactory.delete(rw_avro_schema.id)
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        assert actual is None

    def test_get_latest_schema_by_topic_name(self, topic, rw_avro_schema):
        actual = schema_repo.get_latest_schema_by_topic_name(topic.name)
        self.verify_avro_schema(rw_avro_schema, actual)

    def test_get_latest_schema_by_topic_name_with_nonexistent_topic(self):
        actual = schema_repo.get_latest_schema_by_topic_name('_bad.topic')
        assert actual is None

    @pytest.mark.usefixtures('avro_schemas')
    def test_is_schema_compatible(self, mock_compatible_func):
        target_schema = 'avro schema to be validated'
        actual = schema_repo.is_schema_compatible(
            target_schema,
            self.namespace_name,
            self.source_name
        )
        expected = mock_compatible_func.return_value
        assert expected == actual

    def test_is_schema_compatible_with_no_topic_in_source(
        self,
        namespace,
        source
    ):
        factories.SourceFactory.delete_topics(source.id)
        actual = schema_repo.is_schema_compatible(
            'avro schema to be validated',
            namespace.name,
            source.name
        )
        assert actual is True

        actual = schema_repo.is_schema_compatible('avro schema', 'foo', 'bar')
        assert actual is True

    def test_get_schemas_by_topic_name(self, topic, rw_avro_schema):
        actual = schema_repo.get_schemas_by_topic_name(topic.name)
        assert 1 == len(actual)
        self.verify_avro_schema(rw_avro_schema, actual[0])

    def test_get_schemas_by_topic_name_including_disabled(
            self,
            topic,
            rw_avro_schema,
            disabled_avro_schema
    ):
        actual = schema_repo.get_schemas_by_topic_name(topic.name, True)
        assert 2 == len(actual)
        self.verify_avro_schema(disabled_avro_schema, actual[0])
        self.verify_avro_schema(rw_avro_schema, actual[1])

    def test_get_schemas_by_topic_name_with_nonexistent_topic(self):
        actual = schema_repo.get_schemas_by_topic_name('foo')
        assert [] == actual

    def test_get_schemas_by_topic_id(self, topic, rw_avro_schema):
        actual = schema_repo.get_schemas_by_topic_id(topic.id)
        assert 1 == len(actual)
        self.verify_avro_schema(rw_avro_schema, actual[0])

    def test_get_schemas_by_topic_id_including_disabled(
            self,
            topic,
            rw_avro_schema,
            disabled_avro_schema
    ):
        actual = schema_repo.get_schemas_by_topic_id(topic.id, True)
        assert 2 == len(actual)
        self.verify_avro_schema(disabled_avro_schema, actual[0])
        self.verify_avro_schema(rw_avro_schema, actual[1])

    def test_get_schemas_by_topic_id_with_nonexistent_topic(self):
        actual = schema_repo.get_schemas_by_topic_id(0)
        assert [] == actual

    def test_mark_schema_disabled(self, rw_avro_schema):
        schema_repo.mark_schema_disabled(rw_avro_schema.id)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_avro_schema.id
        ).one()
        assert models.AvroSchemaStatus.DISABLED == actual.status

    def test_mark_schema_disabled_with_nonexisted_schema(self, rw_avro_schema):
        # nothing should happen
        schema_repo.mark_schema_disabled(0)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_avro_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

    def test_mark_schema_readonly(self, rw_avro_schema):
        schema_repo.mark_schema_readonly(rw_avro_schema.id)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_avro_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_ONLY == actual.status

    def test_mark_schema_readonly_with_nonexisted_schema(self, rw_avro_schema):
        # nothing should happen
        schema_repo.mark_schema_readonly(0)
        actual = session.query(
            models.AvroSchema
        ).filter(
            models.AvroSchema.id == rw_avro_schema.id
        ).one()
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

    def test_get_sources(self, source):
        actual = schema_repo.get_sources()
        assert 1 == len(actual)
        self.verify_source(source, actual[0])

    def verify_source(self, expected, actual):
        assert expected.namespace_id == actual.namespace_id
        assert expected.name == actual.name
        assert expected.owner_email == actual.owner_email
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    @pytest.mark.usefixtures('source')
    def test_get_namespaces(self, namespace):
        factories.SourceFactory.create('another source', namespace)
        actual = schema_repo.get_namespaces()
        assert 1 == len(actual)
        self.verify_namespace(namespace, actual[0])

    def verify_namespace(self, expected, actual):
        print(expected)
        print(actual)
        assert expected.name == actual.name
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    def test_get_sources_by_namespace(self, source):
        namespace = factories.NamespaceFactory.create('another namespace')
        factories.SourceFactory.create('another source', namespace)
        actual = schema_repo.get_sources_by_namespace(self.namespace_name)
        assert 1 == len(actual)
        self.verify_source(source, actual[0])

    @pytest.mark.usefixtures('source')
    def test_get_sources_by_namespace_with_nonexistent_namespace(self):
        actual = schema_repo.get_sources_by_namespace('foo')
        assert 0 == len(actual)

    def test_get_topics_by_source_id(self, source, topic):
        actual = schema_repo.get_topics_by_source_id(source.id)
        assert 1 == len(actual)
        self.verify_topic(topic, actual[0])

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
                self.rw_avro_schema_json
            )
            mock_converter.assert_called_once_with(self.rw_avro_schema_json)

    def test_convert_schema_with_no_suitable_converter(self):
        with pytest.raises(Exception):
            schema_repo.convert_schema(
                mock.Mock(),
                mock.Mock(),
                self.rw_avro_schema_json
            )
