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
        return factories.DomainFactory.create_in_db(
            self.namespace,
            self.source
        )

    @property
    def topic_name(self):
        return factories.fake_topic_name

    @pytest.fixture
    def topic(self, domain):
        return factories.TopicFactory.create_in_db(self.topic_name, domain.id)

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
            topic.id,
            status=models.AvroSchemaStatus.DISABLED
        )
        enabled_schema = factories.AvroSchemaFactory.create_in_db(
            self.rw_avro_schema_string,
            topic.id,
            status=models.AvroSchemaStatus.READ_AND_WRITE
        )
        return [disabled_schema, enabled_schema]

    @pytest.fixture
    def disabled_avro_schema(self, avro_schemas):
        return avro_schemas[0]

    @pytest.fixture
    def rw_avro_schema(self, avro_schemas):
        return avro_schemas[1]

    def test_create_schema_from_avro_json(self):
        expected_base_schema_id = 100
        actual = schema_repo.create_avro_schema_from_avro_json(
            self.rw_avro_schema_json,
            self.namespace,
            self.source,
            self.domain_owner_email,
            base_schema_id=expected_base_schema_id
        )
        assert self.rw_avro_schema_json == simplejson.loads(actual.avro_schema)
        assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status
        assert expected_base_schema_id == actual.base_schema_id

        created_domain = session.query(
            models.Domain
        ).filter(
            models.Domain.namespace == self.namespace,
            models.Domain.source == self.source
        ).one()
        assert self.domain_owner_email == created_domain.owner_email

        created_topic = session.query(
            models.Topic
        ).filter(
            models.Topic.domain_id == created_domain.id
        ).one()
        assert created_topic.id == actual.topic_id

    def test_create_schema_from_avro_json_with_existing_topic(self, topic):
        with mock.patch.object(
            schema_repo,
            'is_schema_compatible_in_topic',
            return_value=True
        ):
            actual = schema_repo.create_avro_schema_from_avro_json(
                self.rw_avro_schema_json,
                self.namespace,
                self.source,
                self.domain_owner_email
            )
            actual_schema_json = simplejson.loads(actual.avro_schema)
            assert self.rw_avro_schema_json == actual_schema_json
            assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status
            assert topic.id == actual.topic_id

    def test_create_schema_from_avro_json_with_incompatible_schema(
            self,
            topic
    ):
        with mock.patch.object(
            schema_repo,
            'is_schema_compatible_in_topic',
            return_value=False
        ):
            actual = schema_repo.create_avro_schema_from_avro_json(
                self.rw_avro_schema_json,
                self.namespace,
                self.source,
                self.domain_owner_email
            )
            actual_schema_json = simplejson.loads(actual.avro_schema)
            assert self.rw_avro_schema_json == actual_schema_json
            assert models.AvroSchemaStatus.READ_AND_WRITE == actual.status

            created_topic = session.query(
                models.Topic
            ).filter(
                models.Topic.id == actual.topic_id
            ).one()
            assert created_topic.topic != topic.topic
            assert created_topic.id != topic.id
            assert created_topic.domain_id == topic.domain_id

            expected_domain = session.query(
                models.Domain
            ).filter(
                models.Domain.namespace == self.namespace,
                models.Domain.source == self.source
            ).one()
            assert expected_domain.id == created_topic.domain_id

    def test_get_latest_topic_of_domain(self, domain, topic):
        actual = schema_repo.get_latest_topic_of_domain(
            domain.namespace,
            domain.source
        )
        self.verify_topic(topic, actual)

        new_topic = factories.TopicFactory.create_in_db('newtopic', domain.id)
        actual = schema_repo.get_latest_topic_of_domain(
            domain.namespace,
            domain.source
        )
        self.verify_topic(new_topic, actual)

    def verify_topic(self, expected, actual):
        assert expected.id == actual.id
        assert expected.topic == actual.topic
        assert expected.domain_id == actual.domain_id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    def test_get_latest_topic_of_domain_with_no_topic(self, domain):
        factories.DomainFactory.delete_topics(domain.id)
        actual = schema_repo.get_latest_topic_of_domain(
            domain.namespace,
            domain.source
        )
        assert actual is None

    def test_get_latest_topic_of_domain_with_nonexsited_domain(self):
        actual = schema_repo.get_latest_topic_of_domain('foo', 'bar')
        assert actual is None

    def test_is_schema_compatible_in_topic(self, domain, topic, avro_schemas):
        with mock.patch.object(
            schema_repo,
            'is_full_compatible',
            return_value=True
        ) as mock_compatible_func:
            actual = schema_repo.is_schema_compatible_in_topic(
                self.rw_avro_schema_json,
                topic.topic
            )
            assert True == actual
            mock_compatible_func.assert_has_calls([
                mock.call(self.rw_avro_schema_json, self.rw_avro_schema_json)
            ])

    def test_is_schema_compatible_in_topic_with_incompatible_schema(
            self,
            topic,
            avro_schemas
    ):
        with mock.patch.object(
            schema_repo,
            'is_full_compatible',
            return_value=False
        ) as mock_compatible_func:
            target_avro_schema = {'name': 'avro'}
            actual = schema_repo.is_schema_compatible_in_topic(
                target_avro_schema,
                topic.topic
            )
            assert False == actual
            mock_compatible_func.assert_has_calls([
                mock.call(self.rw_avro_schema_json, target_avro_schema)
            ])

    def test_is_schema_compatible_in_topic_with_no_enabled_schema(
            self,
            topic,
            rw_avro_schema
    ):
        factories.AvroSchemaFactory.delete(rw_avro_schema.id)
        actual = schema_repo.is_schema_compatible_in_topic('avro', topic.topic)
        assert True == actual

    def test_is_schema_compatible_in_topic_with_bad_topic_name(
            self,
            avro_schemas
    ):
        actual = schema_repo.is_schema_compatible_in_topic('avro', 'foo')
        assert True == actual

    def test_get_topic_by_name(self, topic):
        actual = schema_repo.get_topic_by_name(self.topic_name)
        assert topic.id == actual.id
        assert topic.topic == actual.topic
        assert topic.domain_id == actual.domain_id
        assert topic.created_at == actual.created_at
        assert topic.updated_at == actual.updated_at

    def test_get_topic_by_name_with_nonexisted_topic(self):
        actual = schema_repo.get_topic_by_name('foo')
        assert actual is None

    def test_get_domain_by_fullname(self, domain):
        actual = schema_repo.get_domain_by_fullname(
            self.namespace,
            self.source
        )
        assert domain.id == actual.id
        assert domain.namespace == actual.namespace
        assert domain.source == actual.source
        assert domain.created_at == actual.created_at
        assert domain.updated_at == actual.updated_at

    def test_get_domain_by_fullname_with_nonexisted_domain(self):
        actual = schema_repo.get_domain_by_fullname('foo', 'bar')
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

    def test_get_schema_by_id_with_nonexisted_schema(self):
        actual = schema_repo.get_schema_by_id(0)
        assert actual is None

    def test_get_latest_schema_by_topic_id(self, topic, rw_avro_schema):
        actual = schema_repo.get_latest_schema_by_topic_id(topic.id)
        self.verify_avro_schema(rw_avro_schema, actual)

    def test_get_latest_schema_by_topic_id_with_nonexisted_topic(self):
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
        actual = schema_repo.get_latest_schema_by_topic_name(topic.topic)
        self.verify_avro_schema(rw_avro_schema, actual)

    def test_get_latest_schema_by_topic_name_with_nonexisted_topic(self):
        actual = schema_repo.get_latest_schema_by_topic_name('_bad.topic')
        assert actual is None

    def test_is_schema_compatible(self, avro_schemas):
        with mock.patch.object(
            schema_repo,
            'is_schema_compatible_in_topic',
            return_value=True
        ) as mock_compatible_func:
            target_schema = 'avro schema to be validated'
            actual = schema_repo.is_schema_compatible(
                target_schema,
                self.namespace,
                self.source
            )
            assert True == actual
            mock_compatible_func.assert_has_calls([
                mock.call(target_schema, self.topic_name)
            ])

    def test_is_schema_compatible_with_incompatible_schema(self, avro_schemas):
        with mock.patch.object(
            schema_repo,
            'is_schema_compatible_in_topic',
            return_value=False
        ) as mock_compatible_func:
            target_schema = 'avro schema to be validated'
            actual = schema_repo.is_schema_compatible(
                target_schema,
                self.namespace,
                self.source
            )
            assert False == actual
            mock_compatible_func.assert_has_calls([
                mock.call(target_schema, self.topic_name)
            ])

    def test_is_schema_compatible_with_no_topic_in_domain(self, domain):
        factories.DomainFactory.delete_topics(domain.id)
        actual = schema_repo.is_schema_compatible(
            'avro schema to be validated',
            domain.namespace,
            domain.source
        )
        assert True == actual

        actual = schema_repo.is_schema_compatible('avro schema', 'foo', 'bar')
        assert True == actual

    def test_get_schemas_by_topic_name(self, topic, rw_avro_schema):
        actual = schema_repo.get_schemas_by_topic_name(topic.topic)
        assert 1 == len(actual)
        self.verify_avro_schema(rw_avro_schema, actual[0])

    def test_get_schemas_by_topic_name_including_disabled(
            self,
            topic,
            rw_avro_schema,
            disabled_avro_schema
    ):
        actual = schema_repo.get_schemas_by_topic_name(topic.topic, True)
        assert 2 == len(actual)
        self.verify_avro_schema(disabled_avro_schema, actual[0])
        self.verify_avro_schema(rw_avro_schema, actual[1])

    def test_get_schemas_by_topic_name_with_nonexisted_topic(self):
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

    def test_get_schemas_by_topic_id_with_nonexisted_topic(self):
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

    def test_get_domains(self, domain):
        actual = schema_repo.get_domains()
        assert 1 == len(actual)
        self.verify_domain(domain, actual[0])

    def verify_domain(self, expected, actual):
        assert expected.namespace == actual.namespace
        assert expected.source == actual.source
        assert expected.owner_email == actual.owner_email
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at

    def test_get_namespaces(self, domain):
        factories.DomainFactory.create(self.namespace, 'another source')
        actual = schema_repo.get_namespaces()
        assert 1 == len(actual)
        assert self.namespace == actual[0]

    def test_get_domains_by_namespace(self, domain):
        factories.DomainFactory.create('another namespace', 'another source')
        actual = schema_repo.get_domains_by_namespace(self.namespace)
        assert 1 == len(actual)
        self.verify_domain(domain, actual[0])

    def test_get_domains_by_namespace_with_nonexisted_namespace(self, domain):
        actual = schema_repo.get_domains_by_namespace('foo')
        assert 0 == len(actual)

    def test_get_topics_by_domain_id(self, domain, topic):
        actual = schema_repo.get_topics_by_domain_id(domain.id)
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
