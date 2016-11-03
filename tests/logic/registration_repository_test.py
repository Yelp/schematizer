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

import datetime

import pytest

from schematizer import models
from schematizer.logic import registration_repository as reg_repo
from schematizer.models import exceptions as sch_exc
from schematizer.models.database import session
from schematizer_testing import asserts
from schematizer_testing import factories
from schematizer_testing import utils
from tests.models.testing_db import DBTestCase


class TestCreateDataTarget(DBTestCase):

    def test_happy_case(self):
        actual = reg_repo.create_data_target(
            name='yelp_redshift',
            target_type='redshift',
            destination='dwv1.redshift.yelpcorp.com'
        )
        expected = utils.get_entity_by_id(models.DataTarget, actual.id)
        asserts.assert_equal_data_target(actual, expected)

    def test_add_invalid_empty_target_name(self):
        with pytest.raises(ValueError):
            reg_repo.create_data_target(
                name='',
                target_type='foo',
                destination='bar'
            )

    def test_add_invalid_empty_target_type(self):
        with pytest.raises(ValueError):
            reg_repo.create_data_target(
                name='yelp_redshift',
                target_type='',
                destination='foo'
            )

    def test_add_invalid_empty_destination(self):
        with pytest.raises(ValueError):
            reg_repo.create_data_target(
                name='yelp_redshift',
                target_type='foo',
                destination=''
            )


class TestCreateConsumerGroup(DBTestCase):

    @pytest.fixture
    def dw_data_target(self):
        return factories.create_data_target(
            name='yelp_redshift',
            target_type='dw_redshift',
            destination='dwv1.redshift.yelpcorp.com'
        )

    def test_happy_case(self, dw_data_target):
        actual = reg_repo.create_consumer_group('dw', dw_data_target.id)
        expected = utils.get_entity_by_id(models.ConsumerGroup, actual.id)
        asserts.assert_equal_consumer_group(actual, expected)

    def test_add_invalid_empty_consumer_group_name(self, dw_data_target):
        with pytest.raises(ValueError) as e:
            reg_repo.create_consumer_group(
                group_name='',
                data_target_id=dw_data_target.id
            )
            assert e.value == "Consumer group name cannot be empty."

    def test_add_consumer_group_with_non_existing_data_target(self):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.create_consumer_group('dw', data_target_id=0)

    def test_add_same_consumer_group_name_twice(self, dw_data_target):
        reg_repo.create_consumer_group(
            group_name='foo',
            data_target_id=dw_data_target.id
        )
        with pytest.raises(ValueError) as e:
            reg_repo.create_consumer_group(
                group_name='foo',
                data_target_id=dw_data_target.id
            )
            assert e.value == "Consumer group name foo already exists."


class TestGetConsumerGroupsByDataTargetId(DBTestCase):

    def test_happy_case(self, dw_data_target, dw_consumer_group):
        actual = reg_repo.get_consumer_groups_by_data_target_id(
            dw_data_target.id
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[dw_consumer_group],
            assert_func=asserts.assert_equal_consumer_group
        )

    def test_data_target_without_consumer_group(self, dw_data_target):
        actual = reg_repo.get_consumer_groups_by_data_target_id(
            dw_data_target.id
        )
        assert actual == []

    def test_non_existing_data_target(self):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.get_consumer_groups_by_data_target_id(data_target_id=0)


class TestRegisterConsumerGroupDataSource(DBTestCase):

    def test_happy_case(self, dw_consumer_group, yelp_namespace):
        actual = reg_repo.register_consumer_group_data_source(
            consumer_group_id=dw_consumer_group.id,
            data_source_type=models.DataSourceTypeEnum.NAMESPACE,
            data_source_id=yelp_namespace.id
        )
        expected = utils.get_entity_by_id(
            models.ConsumerGroupDataSource,
            actual.id
        )
        asserts.assert_equal_consumer_group_data_source(actual, expected)

    def test_register_with_invalid_consumer_group(self, biz_source):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.register_consumer_group_data_source(
                consumer_group_id=0,
                data_source_type=models.DataSourceTypeEnum.SOURCE,
                data_source_id=biz_source.id
            )

    def test_register_with_missing_data_source(self, dw_consumer_group):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.register_consumer_group_data_source(
                consumer_group_id=dw_consumer_group.id,
                data_source_type=models.DataSourceTypeEnum.SOURCE,
                data_source_id=0
            )

    def test_register_with_invalid_data_source_type(
        self,
        dw_consumer_group,
        biz_source
    ):
        with pytest.raises(ValueError):
            reg_repo.register_consumer_group_data_source(
                consumer_group_id=dw_consumer_group.id,
                data_source_type='bad_type',
                data_source_id=biz_source.id
            )


class TestGetDataSourcesByConsumerGroupId(DBTestCase):

    def test_get_data_sources_by_consumer_group_id(
        self,
        dw_consumer_group,
        dw_consumer_group_namespace_data_src
    ):
        actual = reg_repo.get_data_sources_by_consumer_group_id(
            dw_consumer_group.id
        )
        expected = [dw_consumer_group_namespace_data_src]
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_consumer_group_data_source
        )

    def test_consumer_group_without_data_source(self, dw_consumer_group):
        actual = reg_repo.get_data_sources_by_consumer_group_id(
            dw_consumer_group.id
        )
        assert actual == []

    def test_non_existing_consumer_group(self):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.get_data_sources_by_consumer_group_id(consumer_group_id=0)


class TestGetDataSourcesByDataTargetId(DBTestCase):

    def test_get_data_sources_by_data_target_id(
        self,
        dw_data_target,
        dw_consumer_group_namespace_data_src,
        dw_consumer_group_source_data_src
    ):
        actual = reg_repo.get_data_sources_by_data_target_id(dw_data_target.id)
        expected = {
            dw_consumer_group_namespace_data_src,
            dw_consumer_group_source_data_src
        }
        asserts.assert_equal_entity_set(
            actual_set=actual,
            expected_set=expected,
            assert_func=asserts.assert_equal_consumer_group_data_source,
            id_attr='id'
        )

    def test_data_target_with_no_data_source(self, dw_data_target):
        actual = reg_repo.get_data_sources_by_data_target_id(dw_data_target.id)
        assert actual == []

    def test_non_existing_data_target(self):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.get_data_sources_by_data_target_id(data_target_id=0)


@pytest.mark.usefixtures('yelp_namespace', 'biz_source', 'biz_topic')
class TestGetTopicsByDataTargetId(DBTestCase):

    @pytest.fixture
    def foo_source(self, yelp_namespace):
        return factories.create_source(yelp_namespace.name, 'foo')

    @pytest.fixture
    def foo_topic(self, foo_source):
        return factories.create_topic(
            topic_name='foo',
            namespace_name=foo_source.namespace.name,
            source_name=foo_source.name
        )

    @pytest.fixture
    def dw_consumer_group_source_data_src(self, dw_consumer_group, foo_source):
        return factories.create_consumer_group_data_source(
            dw_consumer_group,
            data_src_type=models.DataSourceTypeEnum.SOURCE,
            data_src_id=foo_source.id
        )

    def test_get_topics_by_data_target_id(
        self,
        dw_data_target,
        biz_topic,
        foo_topic,
        dw_consumer_group_namespace_data_src,
        dw_consumer_group_source_data_src
    ):
        actual = reg_repo.get_topics_by_data_target_id(dw_data_target.id)
        expected = sorted((biz_topic, foo_topic), key=lambda t: t.id)
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=expected,
            assert_func=asserts.assert_equal_topic
        )

    def test_filter_by_created_timestamp(
        self,
        dw_data_target,
        biz_topic,
        foo_topic,
        dw_consumer_group_namespace_data_src,
        dw_consumer_group_source_data_src
    ):
        # set the creation timestamp of foo_topic 10 seconds behind biz_topic
        new_created_at = biz_topic.created_at + datetime.timedelta(seconds=10)
        session.query(models.Topic).filter(
            models.Topic.id == foo_topic.id
        ).update(
            {models.Topic.created_at: new_created_at}
        )

        actual = reg_repo.get_topics_by_data_target_id(
            dw_data_target.id,
            created_after=new_created_at + datetime.timedelta(seconds=-1)
        )
        asserts.assert_equal_entity_list(
            actual_list=actual,
            expected_list=[foo_topic],
            assert_func=asserts.assert_equal_topic
        )

    def test_non_existing_data_target_id(self):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.get_topics_by_data_target_id(data_target_id=0)


class TestGetDataTargetBySchemaID(DBTestCase):

    @pytest.fixture
    def dw_consumer_group_data_source(
        self,
        dw_consumer_group,
        biz_source,
        yelp_namespace
    ):
        factories.create_consumer_group_data_source(
            dw_consumer_group,
            data_src_type=models.DataSourceTypeEnum.SOURCE,
            data_src_id=biz_source.id
        )
        factories.create_consumer_group_data_source(
            dw_consumer_group,
            data_src_type=models.DataSourceTypeEnum.NAMESPACE,
            data_src_id=yelp_namespace.id
        )

    @pytest.fixture
    def dwv2_consumer_group(self, dwv2_data_target):
        return factories.create_consumer_group('dw2', dwv2_data_target)

    @pytest.fixture
    def dwv2_data_target(self):
        return factories.create_data_target(
            name='yelp_redshift_v2',
            target_type='redshift',
            destination='dwv2.redshift.yelpcorp.com'
        )

    @pytest.fixture
    def dw_new_consumer_group_data_source(
        self,
        dwv2_consumer_group,
        biz_source
    ):
        return factories.create_consumer_group_data_source(
            dwv2_consumer_group,
            data_src_type=models.DataSourceTypeEnum.SOURCE,
            data_src_id=biz_source.id
        )

    def test_get_data_target_by_schema_id(
        self,
        biz_schema,
        dw_data_target,
        dw_consumer_group_data_source
    ):
        actual = reg_repo.get_data_targets_by_schema_id(
            biz_schema.id,
        )
        expected = [dw_data_target]
        asserts.assert_equal_entity_list(
            actual,
            expected,
            assert_func=asserts.assert_equal_data_target
        )

    def test_return_multiple_data_targets(
        self,
        biz_schema,
        dw_data_target,
        dwv2_data_target,
        dw_consumer_group_data_source,
        dw_new_consumer_group_data_source
    ):
        actual = reg_repo.get_data_targets_by_schema_id(
            biz_schema.id
        )
        expected = [dw_data_target, dwv2_data_target]
        asserts.assert_equal_entity_set(
            actual,
            expected,
            assert_func=asserts.assert_equal_data_target,
            id_attr='id'
        )

    def test_return_zero_data_targets(self, biz_schema):
        actual = reg_repo.get_data_targets_by_schema_id(
            biz_schema.id
        )
        expected = []
        asserts.assert_equal_entity_list(
            actual,
            expected,
            assert_func=asserts.assert_equal_data_target
        )
