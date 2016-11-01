# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer import models
from schematizer.models import exceptions as sch_exc
from schematizer.models.database import session
from schematizer.models.page_info import PageInfo
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.testing_db import DBTestCase


class GetAllModelTestBase(DBTestCase):

    entity_model = None
    create_entity_func = None
    assert_func_name = None

    @property
    def assert_func(self):
        return getattr(asserts, self.assert_func_name)

    @pytest.fixture
    def entity_1(self):
        return self.create_entity_func(1)

    @pytest.fixture
    def entity_2(self, entity_1):
        return self.create_entity_func(2)

    @pytest.fixture
    def entity_3(self, entity_2):
        return self.create_entity_func(3)

    def test_get_all_entities(self, entity_1, entity_2, entity_3):
        actual = self.entity_model.get_all()
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[entity_1, entity_2, entity_3],
            assert_func=self.assert_func
        )

    def test_when_no_entity_exists(self):
        actual = self.entity_model.get_all()
        assert actual == []

    def test_get_only_one_entity(self, entity_1, entity_2, entity_3):
        actual = self.entity_model.get_all(PageInfo(count=1))
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[entity_1],
            assert_func=self.assert_func
        )

    def test_filter_by_min_entity_id(self, entity_1, entity_2, entity_3):
        actual = self.entity_model.get_all(PageInfo(min_id=entity_1.id + 1))
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[entity_2, entity_3],
            assert_func=self.assert_func
        )

    def test_get_only_one_entity_with_id_greater_than_min_id(
        self,
        entity_1,
        entity_2,
        entity_3
    ):
        actual = self.entity_model.get_all(
            PageInfo(count=1, min_id=entity_1.id + 1)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[entity_2],
            assert_func=self.assert_func
        )


class TestGetModelById(DBTestCase):

    @pytest.fixture
    def dw_data_target(self):
        return factories.create_data_target(
            name='yelp_redshift',
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
            name='yelp_redshift',
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
