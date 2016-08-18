# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer import models
from schematizer.models import exceptions as sch_exc
from schematizer.models.database import session
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.testing_db import DBTestCase


class TestGetModelById(DBTestCase):

    @pytest.fixture
    def dw_data_target(self):
        return factories.create_data_target(
            target_type='redshift',
            destination='dwv1.redshift.yelpcorp.com'
        )

    @pytest.fixture
    def dw_consumer_group(self, dw_data_target):
        return factories.create_consumer_group('dw', dw_data_target)

    @pytest.fixture
    def dw_consumer_group_data_source(self, dw_consumer_group, biz_source):
        return factories.create_consumer_group_data_source(
            dw_consumer_group,
            data_src_type=models.DataSourceTypeEnum.SOURCE,
            data_src_id=biz_source.id
        )

    def test_get_data_target_by_id(self, dw_data_target):
        actual = models.DataTarget.get_by_id(dw_data_target.id)
        asserts.assert_equal_data_target(actual, dw_data_target)

    def test_get_consumer_group_by_id(self, dw_consumer_group):
        actual = models.ConsumerGroup.get_by_id(dw_consumer_group.id)
        asserts.assert_equal_consumer_group(actual, dw_consumer_group)

    def test_get_consumer_group_data_source_by_id(
        self,
        dw_consumer_group_data_source
    ):
        actual = models.ConsumerGroupDataSource.get_by_id(
            dw_consumer_group_data_source.id
        )
        asserts.assert_equal_consumer_group_data_source(
            actual,
            dw_consumer_group_data_source
        )

    def test_get_source_by_id(self, biz_source):
        actual = models.Source.get_by_id(biz_source.id)
        asserts.assert_equal_source(actual, biz_source)

    def test_get_topic_by_id(self, biz_topic):
        actual = models.Topic.get_by_id(biz_topic.id)
        asserts.assert_equal_topic(actual, biz_topic)

    def test_get_avro_schema_by_id(self, biz_schema):
        actual = models.AvroSchema.get_by_id(biz_schema.id)
        asserts.assert_equal_avro_schema(actual, biz_schema)

    def test_get_refresh_by_id(self, biz_src_refresh):
        actual = models.Refresh.get_by_id(biz_src_refresh.id)
        asserts.assert_equal_refresh(actual, biz_src_refresh)

    @pytest.mark.parametrize('model_cls', [
        models.Source,
        models.Topic,
        models.AvroSchema,
        models.AvroSchemaElement,
        models.Refresh,
        models.Note,
        models.SourceCategory,
        models.DataTarget,
        models.ConsumerGroup,
        models.ConsumerGroupDataSource
    ])
    def test_get_invalid_id(self, model_cls):
        with pytest.raises(sch_exc.EntityNotFoundError):
            model_cls.get_by_id(0)


class TestCreateModel(DBTestCase):

    def test_create_data_target(self):
        actual = models.DataTarget.create(
            session,
            target_type='foo',
            destination='bar'
        )
        expected = models.DataTarget.get_by_id(actual.id)
        asserts.assert_equal_data_target(actual, expected)
        assert actual.target_type == 'foo'
        assert actual.destination == 'bar'

    def test_create_consumer_group(self, dw_data_target):
        actual = models.ConsumerGroup.create(
            session,
            group_name='foo',
            data_target=dw_data_target
        )
        expected = models.ConsumerGroup.get_by_id(actual.id)
        asserts.assert_equal_consumer_group(actual, expected)
        assert actual.group_name == 'foo'
        assert actual.data_target_id == dw_data_target.id

    def test_create_consumer_group_data_source(
        self,
        yelp_namespace,
        dw_consumer_group
    ):
        actual = models.ConsumerGroupDataSource.create(
            session,
            data_source_type=models.DataSourceTypeEnum.NAMESPACE,
            data_source_id=yelp_namespace.id,
            consumer_group=dw_consumer_group
        )
        expected = models.ConsumerGroupDataSource.get_by_id(actual.id)
        asserts.assert_equal_consumer_group_data_source(actual, expected)
        assert actual.data_source_type == models.DataSourceTypeEnum.NAMESPACE
        assert actual.data_source_id == yelp_namespace.id
        assert actual.consumer_group_id == dw_consumer_group.id
