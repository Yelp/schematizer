# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

import pytest

from schematizer import models
from schematizer.logic import registration_repository as reg_repo
from schematizer.models import exceptions as sch_exc
from schematizer.models.database import session
from testing import asserts
from testing import factories
from testing import utils
from tests.models.testing_db import DBTestCase


class TestCreateDataTarget(DBTestCase):

    def test_happy_case(self):
        actual = reg_repo.create_data_target(
            target_type='redshift',
            destination='dwv1.redshift.yelpcorp.com'
        )
        expected = utils.get_entity_by_id(models.DataTarget, actual.id)
        asserts.assert_equal_data_target(actual, expected)

    def test_add_invalid_empty_target_type(self):
        with pytest.raises(ValueError):
            reg_repo.create_data_target(target_type='', destination='foo')

    def test_add_invalid_empty_destination(self):
        with pytest.raises(ValueError):
            reg_repo.create_data_target(target_type='foo', destination='')


class TestCreateConsumerGroup(DBTestCase):

    @pytest.fixture(scope="class")
    def dw_data_target(self):
        return factories.create_data_target(
            target_type='dw_redshift',
            destination='dwv1.redshift.yelpcorp.com'
        )

    def test_happy_case(self, dw_data_target):
        actual = reg_repo.create_consumer_group('dw', dw_data_target.id)
        expected = utils.get_entity_by_id(models.ConsumerGroup, actual.id)
        asserts.assert_equal_consumer_group(actual, expected)

    def test_add_invalid_empty_consumer_group_name(self, dw_data_target):
        with pytest.raises(ValueError):
            reg_repo.create_consumer_group(
                group_name='',
                data_target_id=dw_data_target.id
            )

    def test_add_consumer_group_with_non_existing_data_target(self):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.create_consumer_group('dw', data_target_id=0)


class TestGetConsumerGroupsByDataTargetId(DBTestCase):

    def test_happy_case(self, dw_data_target, dw_consumer_group):
        actual = reg_repo.get_consumer_groups_by_data_target_id(
            dw_data_target.id
        )
        asserts.assert_equal_entities(
            actual_entities=actual,
            expected_entities=[dw_consumer_group],
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
        assert set(actual) == expected

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
        asserts.assert_equal_entities(
            actual_entities=actual,
            expected_entities=expected,
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
        asserts.assert_equal_entities(
            actual_entities=actual,
            expected_entities=[foo_topic],
            assert_func=asserts.assert_equal_topic
        )

    def test_non_existing_data_target_id(self):
        with pytest.raises(sch_exc.EntityNotFoundError):
            reg_repo.get_topics_by_data_target_id(data_target_id=0)
