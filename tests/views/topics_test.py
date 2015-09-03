# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

import pytest

from schematizer.api.exceptions import exceptions_v1 as exc_v1
from schematizer.views import topics as topic_views
from tests.views.api_test_base import TestApiBase


class TestTopicsViewBase(TestApiBase):

    test_view_module = 'schematizer.views.topics'


class TestGetTopicByTopicName(TestApiBase):

    def test_non_existing_topic_name(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
            topic_views.get_topic_by_topic_name(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_topic, biz_topic_response):
        mock_request.matchdict = self.get_mock_dict({
            'topic_name': biz_topic.name
        })
        actual = topic_views.get_topic_by_topic_name(mock_request)
        assert biz_topic_response == actual


class TestListSchemasByTopicName(TestTopicsViewBase):

    def test_non_existing_topic_name(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
            mock_repo.get_schemas_by_topic_name.return_value = []
            mock_repo.get_topic_by_name.return_value = None
            topic_views.list_schemas_by_topic_name(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.TOPIC_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_schemas_by_topic_name.assert_called_once_with('foo')
        mock_repo.get_topic_by_name.assert_called_once_with('foo')

    def test_non_existing_schemas(self, mock_request, mock_repo):
        mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
        mock_repo.get_schemas_by_topic_name.return_value = []

        schemas = topic_views.list_schemas_by_topic_name(mock_request)

        assert schemas == []
        mock_repo.get_schemas_by_topic_name.assert_called_once_with('foo')

    def test_happy_case(self, mock_request, mock_repo, mock_schema):
        mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
        mock_repo.get_schemas_by_topic_name.return_value = self.schemas

        schemas = topic_views.list_schemas_by_topic_name(mock_request)

        assert self.schemas_response == schemas
        mock_repo.get_schemas_by_topic_name.assert_called_once_with('foo')


class TestGetLatestSchemaByTopicName(TestTopicsViewBase):

    def test_non_existing_topic_name(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'ba'})
            mock_repo.get_latest_schema_by_topic_name.return_value = None
            mock_repo.get_topic_by_name.return_value = None
            topic_views.get_latest_schema_by_topic_name(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.TOPIC_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_schema_by_topic_name.assert_called_once_with('ba')
        mock_repo.get_topic_by_name.assert_called_once_with('ba')

    def test_no_latest_schema(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'ba'})
            mock_repo.get_latest_schema_by_topic_name.return_value = None
            topic_views.get_latest_schema_by_topic_name(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_schema_by_topic_name.assert_called_once_with('ba')

    def test_happy_case(self, mock_request, mock_repo, mock_schema):
        mock_request.matchdict = self.get_mock_dict({'topic_name': 'ba'})
        mock_repo.get_latest_schema_by_topic_name.return_value = self.schema

        actual = topic_views.get_latest_schema_by_topic_name(mock_request)

        assert self.schema_response == actual
        mock_repo.get_latest_schema_by_topic_name.assert_called_once_with('ba')


class TestGetTopicsByCriteria(TestApiBase):

    def test_non_existing_namespace_name(self, mock_request, biz_topic):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.params = self.get_mock_dict({'namespace': 'missing'})
            topic_views.get_topics_by_criteria(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.NAMESPACE_NOT_FOUND_ERROR_MESSAGE

    def test_bad_source_name(self, mock_request, biz_topic):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.params = self.get_mock_dict({
                'namespace': biz_topic.source.namespace.name,
                'source': 'missing'
            })
            topic_views.get_topics_by_criteria(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_filter_by_namespace_and_time(
        self,
        mock_request,
        biz_topic,
        biz_topic_response
    ):
        mock_request.params = self.get_mock_dict({
            'namespace': biz_topic.source.namespace.name,
            'created_after': (biz_topic.created_at -
                              datetime.utcfromtimestamp(0)).total_seconds() - 1
        })
        actual = topic_views.get_topics_by_criteria(mock_request)
        assert actual == [biz_topic_response]
