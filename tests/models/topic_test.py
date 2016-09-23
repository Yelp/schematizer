# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models.source import Topic
from schematizer.models.exceptions import InvalidTopicClusterTypeError
from schematizer_testing import factories
from tests.models.base_model_test import GetAllModelTestBase
from tests.models.testing_db import DBTestCase


class TestGetAllTopics(GetAllModelTestBase):

    def create_topic(self, topic_no):
        source_bar = factories.get_or_create_source(
            namespace_name='foo',
            source_name='bar',
            owner_email='test.dev@yelp.com'
        )
        return factories.create_topic(
            topic_name='topic_{}'.format(topic_no),
            namespace_name=source_bar.namespace.name,
            source_name=source_bar.name
        )

    entity_model = Topic
    create_entity_func = create_topic
    assert_func_name = 'assert_equal_topic'


class TestTopicModel(DBTestCase):

    @pytest.mark.parametrize("overrides, expected_cluster_type", [
        ({}, 'datapipe'),
        ({'cluster_type': 'scribe'}, 'scribe')
    ])
    def test_valid_cluster_type(
        self,
        biz_source,
        overrides,
        expected_cluster_type
    ):
        topic = factories.create_topic(
            topic_name='yelp.biz_test.1',
            namespace_name=biz_source.namespace.name,
            source_name=biz_source.name,
            **overrides
        )
        assert topic.cluster_type == expected_cluster_type

    def test_invalid_cluster_type(self, biz_source):
        with pytest.raises(InvalidTopicClusterTypeError)as e:
            factories.create_topic(
                topic_name='yelp.biz_test.1',
                namespace_name=biz_source.namespace.name,
                source_name=biz_source.name,
                cluster_type='not_a_kafka_cluster_type'
            )
            assert e.value == (
                '`not_a_kafka_cluster_type`  kafka cluster type does not exist.')
