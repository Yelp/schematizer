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

from datetime import datetime

import pytest

from schematizer.api.exceptions import exceptions_v1 as exc_v1
from schematizer.views import topics as topic_views
from tests.views.api_test_base import ApiTestBase


class TestGetTopicByTopicName(ApiTestBase):

    def test_non_existing_topic_name(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'topic_name': 'foo'}
            topic_views.get_topic_by_topic_name(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_topic):
        mock_request.matchdict = {'topic_name': biz_topic.name}
        actual = topic_views.get_topic_by_topic_name(mock_request)
        expected = self.get_expected_topic_resp(biz_topic.id)
        assert actual == expected

    def test_pkey_in_topic(self, mock_request, biz_pkey_topic):
        mock_request.matchdict = {'topic_name': biz_pkey_topic.name}
        actual = topic_views.get_topic_by_topic_name(mock_request)
        expected = self.get_expected_topic_resp(biz_pkey_topic.id)
        assert actual == expected


class TestListSchemasByTopicName(ApiTestBase):

    def test_non_existing_topic_name(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'topic_name': 'foo'}
            topic_views.list_schemas_by_topic_name(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_non_existing_schemas(self, mock_request, biz_topic):
        mock_request.matchdict = {'topic_name': biz_topic.name}
        actual = topic_views.list_schemas_by_topic_name(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, biz_topic, biz_schema):
        mock_request.matchdict = {'topic_name': biz_topic.name}
        actual = topic_views.list_schemas_by_topic_name(mock_request)
        expected = [self.get_expected_schema_resp(biz_schema.id)]
        assert actual == expected


class TestGetLatestSchemaByTopicName(ApiTestBase):

    def test_non_existing_topic_name(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'topic_name': 'ba'}
            topic_views.get_latest_schema_by_topic_name(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_no_latest_schema(self, mock_request, biz_topic):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'topic_name': biz_topic.name}
            topic_views.get_latest_schema_by_topic_name(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(
        self,
        mock_request,
        biz_topic,
        biz_schema,
        disabled_schema
    ):
        mock_request.matchdict = {'topic_name': biz_topic.name}
        actual = topic_views.get_latest_schema_by_topic_name(mock_request)
        expected = self.get_expected_schema_resp(biz_schema.id)
        assert actual == expected


class TestGetTopicsByCriteria(ApiTestBase):

    def test_non_existing_namespace_name(self, mock_request, biz_topic):
        mock_request.params = {'namespace': 'missing'}
        actual = topic_views.get_topics_by_criteria(mock_request)
        assert actual == []

    def test_bad_source_name(self, mock_request, biz_topic):
        mock_request.params = {
            'namespace': biz_topic.source.namespace.name,
            'source': 'missing'
        }
        actual = topic_views.get_topics_by_criteria(mock_request)
        assert actual == []

    def test_filter_by_namespace_and_time(self, mock_request, biz_topic):
        mock_request.params = {
            'namespace': biz_topic.source.namespace.name,
            'created_after': (biz_topic.created_at -
                              datetime.utcfromtimestamp(0)).total_seconds()
        }
        actual = topic_views.get_topics_by_criteria(mock_request)
        expected = [self.get_expected_topic_resp(biz_topic.id)]
        assert actual == expected

    def test_count(self, mock_request, biz_topic, biz_pkey_topic):
        mock_request.params = {
            'count': 1
        }

        # Without the count param, length would be 2
        assert len(topic_views.get_topics_by_criteria(mock_request)) == 1

    def test_min_id(self, mock_request, biz_topic, biz_pkey_topic):
        # Sorting the topics to find the one with the lowest id,
        # and then setting min_id to be one higher so we are
        # guaranteed to only have the topic with
        # the higher id (if min_id is working properly)
        sorted_topics = sorted(
            [biz_topic, biz_pkey_topic],
            key=lambda topic: topic.id
        )
        mock_request.params = {
            'min_id': sorted_topics[0].id + 1
        }

        actual = topic_views.get_topics_by_criteria(mock_request)
        expected = [self.get_expected_topic_resp(sorted_topics[1].id)]
        assert actual == expected

    def test_min_id_equal(self, mock_request, biz_topic, biz_pkey_topic):
        # Sorting the topics to find the one with the lowest id,
        # and then setting min_id to be equal so we are
        # guaranteed to have both topics
        # (if min_id is working properly)
        sorted_topics = sorted(
            [biz_topic, biz_pkey_topic],
            key=lambda topic: topic.id
        )
        mock_request.params = {
            'min_id': sorted_topics[0].id
        }

        actual = topic_views.get_topics_by_criteria(mock_request)
        expected = [
            self.get_expected_topic_resp(topic.id)
            for topic in sorted_topics
        ]
        assert actual == expected
