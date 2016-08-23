# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.source import Topic
from testing import factories
from tests.models.base_model_test import GetAllModelTestBase


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
