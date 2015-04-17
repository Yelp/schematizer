# -*- coding: utf-8 -*-
import pytest
from pyramid.httpexceptions import HTTPNotFound

from schematizer.views import constants
from schematizer.views.topics import get_latest_schema_by_topic_name
from schematizer.views.topics import get_topic_by_topic_name
from schematizer.views.topics import list_schemas_by_topic_name
from tests.views.api_test_base import TestApiBase


class TestTopicsViewBase(TestApiBase):

    test_view_module = 'schematizer.views.topics'


class TestGetTopicByTopicName(TestTopicsViewBase):

    def test_non_existing_topic_name(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
            mock_repo.get_topic_by_name.return_value = None
            get_topic_by_topic_name(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.TOPIC_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_topic_by_name.assert_called_once_with('foo')

    def test_happy_case(self, mock_request, mock_repo):
        mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
        mock_repo.get_topic_by_name.return_value = self.topic

        actual = get_topic_by_topic_name(mock_request)

        assert self.topic_response == actual
        mock_repo.get_topic_by_name.assert_called_once_with('foo')


class TestListSchemasByTopicName(TestTopicsViewBase):

    def test_non_existing_topic_name(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
            mock_repo.get_schemas_by_topic_name.return_value = []
            mock_repo.get_topic_by_name.return_value = None
            list_schemas_by_topic_name(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.TOPIC_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_schemas_by_topic_name.assert_called_once_with('foo')

    def test_non_existing_schemas(self, mock_request, mock_repo):
        mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
        mock_repo.get_schemas_by_topic_name.return_value = []

        schemas = list_schemas_by_topic_name(mock_request)

        assert schemas == []
        mock_repo.get_schemas_by_topic_name.assert_called_once_with('foo')

    def test_happy_case(self, mock_request, mock_repo):
        mock_request.matchdict = self.get_mock_dict({'topic_name': 'foo'})
        mock_repo.get_schemas_by_topic_name.return_value = self.schemas

        schemas = list_schemas_by_topic_name(mock_request)

        assert self.schemas_response == schemas
        mock_repo.get_schemas_by_topic_name.assert_called_once_with('foo')


class TestGetLatestSchemaByTopicName(TestTopicsViewBase):

    def test_non_existing_topic_name(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'ba'})
            mock_repo.get_latest_schema_by_topic_name.return_value = None
            mock_repo.get_topic_by_name.return_value = None
            get_latest_schema_by_topic_name(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.TOPIC_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_schema_by_topic_name.assert_called_once_with('ba')

    def test_no_latest_schema(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_request.matchdict = self.get_mock_dict({'topic_name': 'ba'})
            mock_repo.get_latest_schema_by_topic_name.return_value = None
            get_latest_schema_by_topic_name(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_schema_by_topic_name.assert_called_once_with('ba')

    def test_happy_case(self, mock_request, mock_repo):
        mock_request.matchdict = self.get_mock_dict({'topic_name': 'ba'})
        mock_repo.get_latest_schema_by_topic_name.return_value = self.schema

        actual = get_latest_schema_by_topic_name(mock_request)
        assert self.schema_response == actual
        mock_repo.get_latest_schema_by_topic_name.assert_called_once_with('ba')
