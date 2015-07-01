# -*- coding: utf-8 -*-
import pytest

from schematizer.api.exceptions import exceptions_v1 as exc_v1
from schematizer.views import sources as source_views
from tests.views.api_test_base import TestApiBase


class TestSourcesViewBase(TestApiBase):

    test_view_module = 'schematizer.views.sources'


class TestListSources(TestSourcesViewBase):

    def test_no_sources(self, mock_request, mock_repo):
        mock_repo.get_sources.return_value = []
        sources = source_views.list_sources(mock_request)
        assert sources == []
        mock_repo.get_sources.assert_called_once_with()

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_sources.return_value = self.sources
        sources = source_views.list_sources(mock_request)
        assert self.sources_response == sources
        mock_repo.get_sources.assert_called_once_with()


class TestGetSourceByID(TestSourcesViewBase):

    def test_non_existing_source_id(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_source_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            source_views.get_source_by_id(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_source_by_id.assert_called_once_with(0)

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_source_by_id.return_value = self.source
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        source = source_views.get_source_by_id(mock_request)

        assert self.source_response == source
        mock_repo.get_source_by_id.assert_called_once_with(1)


class TestListTopicsBySourceID(TestSourcesViewBase):

    def test_non_existing_source_id(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_topics_by_source_id.return_value = []
            mock_repo.get_source_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            source_views.list_topics_by_source_id(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_topics_by_source_id.assert_called_once_with(0)

    def test_no_topics(self, mock_request, mock_repo):
        mock_repo.get_topics_by_source_id.return_value = []
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        topics = source_views.list_topics_by_source_id(mock_request)

        assert topics == []
        mock_repo.get_topics_by_source_id.assert_called_once_with(1)

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_topics_by_source_id.return_value = self.topics
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        topics = source_views.list_topics_by_source_id(mock_request)

        assert self.topics_response == topics
        mock_repo.get_topics_by_source_id.assert_called_once_with(1)


class TestGetLatestTopicBySourceID(TestSourcesViewBase):

    def test_non_existing_source_id(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_latest_topic_of_source_id.return_value = None
            mock_repo.get_source_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            source_views.get_latest_topic_by_source_id(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_topic_of_source_id.assert_called_once_with(0)
        mock_repo.get_source_by_id.assert_called_once_with(0)

    def test_no_latest_topic(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_latest_topic_of_source_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '1'})
            source_views.get_latest_topic_by_source_id(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_topic_of_source_id.assert_called_once_with(1)

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_latest_topic_of_source_id.return_value = self.topic
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        topic = source_views.get_latest_topic_by_source_id(mock_request)

        assert self.topic_response == topic
        mock_repo.get_latest_topic_of_source_id.assert_called_once_with(1)
