# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy
from datetime import datetime

import pytest

from schematizer.views import data_targets as data_target_views
from tests.views.api_test_base import ApiTestBase


class TestGetDataTargets(ApiTestBase):

    def test_no_data_targets(self, mock_request):
        actual = data_target_views.get_data_targets(mock_request)
        assert actual == []

    def test_one_data_target(self, mock_request, dw_data_target):
        actual = data_target_views.get_data_targets(mock_request)
        expected = [self.get_expected_data_target_resp(dw_data_target.id)]
        assert actual == expected


class TestGetDataTargetByID(ApiTestBase):

    def test_happy_case(self, mock_request, dw_data_target):
        mock_request.matchdict = {'data_target_id': str(dw_data_target.id)}
        actual = data_target_views.get_data_target_by_id(mock_request)
        expected = self.get_expected_data_target_resp(dw_data_target.id)
        assert actual == expected

    def test_non_existing_data_target_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'data_target_id': '0'}
            data_target_views.get_data_target_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'DataTarget id 0 not found.'


class TestCreateDataTarget(ApiTestBase):

    @pytest.fixture
    def request_json(self):
        return {
            'target_type': 'redshift',
            'destination': 'prod.yelpcorp'
        }

    def test_happy_case(self, mock_request, request_json):
        mock_request.json_body = request_json
        actual = data_target_views.create_data_target(mock_request)

        expected = self.get_expected_data_target_resp(
            actual['data_target_id'],
            **request_json
        )
        assert actual == expected

    def test_invalid_empty_target_type(self, mock_request, request_json):
        invalid_request = copy.deepcopy(request_json)
        invalid_request['target_type'] = ''

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = invalid_request
            data_target_views.create_data_target(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "data target type cannot be empty."

    def test_invalid_empty_destination(self, mock_request, request_json):
        invalid_request = copy.deepcopy(request_json)
        invalid_request['destination'] = ''

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = invalid_request
            data_target_views.create_data_target(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "destination of data target cannot be empty."


class TestCreateConsumerGroup(ApiTestBase):

    @pytest.fixture
    def request_json(self):
        return {'group_name': 'dw'}

    def test_happy_case(self, mock_request, dw_data_target, request_json):
        mock_request.matchdict = {'data_target_id': str(dw_data_target.id)}
        mock_request.json_body = request_json
        actual = data_target_views.create_consumer_group(mock_request)

        expected = self.get_expected_consumer_group_resp(
            actual['consumer_group_id'],
            **request_json
        )
        assert actual == expected
        assert actual['data_target']['data_target_id'] == dw_data_target.id

    def test_non_existing_data_target_id(self, mock_request, request_json):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'data_target_id': '0'}
            mock_request.json_body = request_json
            data_target_views.create_consumer_group(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "DataTarget id 0 not found."

    def test_invalid_empty_group_name(
        self,
        mock_request,
        dw_data_target,
        request_json
    ):
        invalid_request = copy.deepcopy(request_json)
        invalid_request['group_name'] = ''

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'data_target_id': str(dw_data_target.id)}
            mock_request.json_body = invalid_request
            data_target_views.create_consumer_group(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Consumer group name cannot be empty."


class TestGetConsumerGroupsByDataTargetId(ApiTestBase):

    def test_data_target_with_consumer_group(
        self,
        mock_request,
        dw_data_target,
        dw_consumer_group
    ):
        mock_request.matchdict = {'data_target_id': str(dw_data_target.id)}
        actual = data_target_views.get_consumer_groups_by_data_target_id(
            mock_request
        )
        expected = [
            self.get_expected_consumer_group_resp(dw_consumer_group.id)
        ]
        assert actual == expected

    def test_data_target_without_consumer_group(
        self,
        mock_request,
        dw_data_target
    ):
        mock_request.matchdict = {'data_target_id': str(dw_data_target.id)}
        actual = data_target_views.get_consumer_groups_by_data_target_id(
            mock_request
        )
        assert actual == []

    def test_non_existing_data_target(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'data_target_id': '0'}
            data_target_views.get_consumer_groups_by_data_target_id(
                mock_request
            )

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'DataTarget id 0 not found.'


class TestGetTopicsByDataTargetId(ApiTestBase):

    def test_data_target_without_associated_topics(
        self,
        mock_request,
        dw_data_target
    ):
        mock_request.matchdict = {'data_target_id': str(dw_data_target.id)}
        mock_request.params = {}
        actual = data_target_views.get_topics_by_data_target_id(mock_request)
        assert actual == []

    def test_non_existing_data_target_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'data_target_id': '0'}
            mock_request.params = {}
            data_target_views.get_topics_by_data_target_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "DataTarget id 0 not found."

    def test_filter_by_creation_time(
        self,
        mock_request,
        biz_topic,
        dw_data_target,
        dw_consumer_group_source_data_src
    ):
        epoch_create_time = (biz_topic.created_at -
                             datetime.utcfromtimestamp(0)).total_seconds()
        mock_request.matchdict = {'data_target_id': str(dw_data_target.id)}
        mock_request.params = {
            'created_after': epoch_create_time
        }
        actual = data_target_views.get_topics_by_data_target_id(mock_request)
        expected = [self.get_expected_topic_resp(biz_topic.id)]
        assert actual == expected

        five_sec_after_create_time = epoch_create_time + 5
        mock_request.params = {'created_after': five_sec_after_create_time}
        actual = data_target_views.get_topics_by_data_target_id(mock_request)
        assert actual == []
