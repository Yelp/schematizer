# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

import pytest

from schematizer.views import consumer_groups as con_group_views
from testing.factories import create_consumer_group
from tests.views.api_test_base import ApiTestBase


class TestGetConsumerGroups(ApiTestBase):

    def test_no_consumer_group(self, mock_request):
        actual = con_group_views.get_consumer_groups(mock_request)
        assert actual == []

    def test_one_consumer_group(
        self,
        mock_request,
        dw_consumer_group,
        dw_data_target
    ):
        another_consumer_group = create_consumer_group(
            'another_consumer_group',
            data_target=dw_data_target
        )
        actual = con_group_views.get_consumer_groups(mock_request)
        expected = [
            self.get_expected_consumer_group_resp(dw_consumer_group.id),
            self.get_expected_consumer_group_resp(another_consumer_group.id)
        ]
        assert actual == expected


class TestGetConsumerGroupByID(ApiTestBase):

    def test_happy_case(self, mock_request, dw_consumer_group):
        mock_request.matchdict = {
            'consumer_group_id': str(dw_consumer_group.id)
        }
        actual = con_group_views.get_consumer_group_by_id(mock_request)
        expected = self.get_expected_consumer_group_resp(dw_consumer_group.id)
        assert actual == expected

    def test_non_existing_consumer_group_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'consumer_group_id': '0'}
            con_group_views.get_consumer_group_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'ConsumerGroup id 0 not found.'


class TestGetDataSourcesByConsumerGroupId(ApiTestBase):

    def test_data_source_with_consumer_group(
        self,
        mock_request,
        dw_consumer_group,
        dw_consumer_group_namespace_data_src
    ):
        mock_request.matchdict = {
            'consumer_group_id': str(dw_consumer_group.id)
        }
        actual = con_group_views.get_data_sources_by_consumer_group_id(
            mock_request
        )
        expected = [
            self.get_expected_consumer_group_data_src_resp(
                dw_consumer_group_namespace_data_src.id
            )
        ]
        assert actual == expected

    def test_consumer_group_without_data_source(
        self,
        mock_request,
        dw_consumer_group
    ):
        mock_request.matchdict = {
            'consumer_group_id': str(dw_consumer_group.id)
        }
        actual = con_group_views.get_data_sources_by_consumer_group_id(
            mock_request
        )
        assert actual == []

    def test_non_existing_consumer_group(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'consumer_group_id': '0'}
            con_group_views.get_data_sources_by_consumer_group_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == 'ConsumerGroup id 0 not found.'


class TestCreateConsumerGroupDataSource(ApiTestBase):

    @pytest.fixture
    def request_json(self, yelp_namespace):
        return {
            'data_source_type': 'Namespace',
            'data_source_id': yelp_namespace.id
        }

    def test_happy_case(self, mock_request, dw_consumer_group, request_json):
        mock_request.matchdict = {
            'consumer_group_id': str(dw_consumer_group.id)
        }
        mock_request.json_body = request_json
        actual = con_group_views.create_consumer_group_data_source(
            mock_request
        )

        expected = self.get_expected_consumer_group_data_src_resp(
            actual['consumer_group_data_source_id'],
            **request_json
        )
        assert actual == expected
        assert actual['consumer_group_id'] == dw_consumer_group.id

    def test_non_existing_consumer_group_id(self, mock_request, request_json):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'consumer_group_id': '0'}
            mock_request.json_body = request_json
            con_group_views.create_consumer_group_data_source(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "ConsumerGroup id 0 not found."

    def test_invalid_data_source_type(
        self,
        mock_request,
        dw_consumer_group,
        request_json
    ):
        invalid_request = copy.deepcopy(request_json)
        invalid_request['data_source_type'] = ''

        expected_exception = self.get_http_exception(400)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {
                'consumer_group_id': str(dw_consumer_group.id)
            }
            mock_request.json_body = invalid_request
            con_group_views.create_consumer_group_data_source(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == ("Invalid data source type . "
                                "It should be one of DataSourceTypeEnum.")

    def test_non_existing_data_source(
        self,
        mock_request,
        dw_consumer_group,
        request_json
    ):
        invalid_request = copy.deepcopy(request_json)
        invalid_request['data_source_id'] = '0'

        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {
                'consumer_group_id': str(dw_consumer_group.id)
            }
            mock_request.json_body = invalid_request
            con_group_views.create_consumer_group_data_source(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == "Namespace id 0 not found."
