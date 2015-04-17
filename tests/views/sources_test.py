# -*- coding: utf-8 -*-
import pytest
from pyramid.httpexceptions import HTTPNotFound

from schematizer.views import constants
from schematizer.views.sources import get_latest_topic_by_source_id
from schematizer.views.sources import get_source_by_id
from schematizer.views.sources import list_sources
from schematizer.views.sources import list_topics_by_source_id
from tests.views.api_test_base import TestApiBase


class TestSourcesViewBase(TestApiBase):

    test_view_module = 'schematizer.views.sources'


class TestListSources(TestSourcesViewBase):

    def test_no_sources(self, mock_request, mock_repo):
        mock_repo.get_domains.return_value = []
        sources = list_sources(mock_request)
        assert sources == []
        mock_repo.get_domains.assert_called_once_with()

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_domains.return_value = self.sources
        sources = list_sources(mock_request)
        assert self.sources_response == sources
        mock_repo.get_domains.assert_called_once_with()


class TestGetSourceByID(TestSourcesViewBase):

    def test_non_existing_source_id(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_repo.get_domain_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            get_source_by_id(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_domain_by_id.assert_called_once_with(0)

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_domain_by_id.return_value = self.source
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        source = get_source_by_id(mock_request)

        assert self.source_response == source
        mock_repo.get_domain_by_id.assert_called_once_with(1)


class TestListTopicsBySourceID(TestSourcesViewBase):

    def test_non_existing_source_id(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_repo.get_topics_by_domain_id.return_value = []
            mock_repo.get_domain_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            list_topics_by_source_id(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_topics_by_domain_id.assert_called_once_with(0)

    def test_no_topics(self, mock_request, mock_repo):
        mock_repo.get_topics_by_domain_id.return_value = []
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        topics = list_topics_by_source_id(mock_request)

        assert topics == []
        mock_repo.get_topics_by_domain_id.assert_called_once_with(1)

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_topics_by_domain_id.return_value = self.topics
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        topics = list_topics_by_source_id(mock_request)

        assert self.topics_response == topics
        mock_repo.get_topics_by_domain_id.assert_called_once_with(1)


class TestGetLatestTopicBySourceID(TestSourcesViewBase):

    def test_non_existing_source_id(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_repo.get_latest_topic_of_domain_id.return_value = None
            mock_repo.get_domain_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            get_latest_topic_by_source_id(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_topic_of_domain_id.assert_called_once_with(0)

    def test_no_latest_topic(self, mock_request, mock_repo):
        with pytest.raises(HTTPNotFound) as e:
            mock_repo.get_latest_topic_of_domain_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '1'})
            get_latest_topic_by_source_id(mock_request)

        assert e.value.code == 404
        assert str(e.value) == constants.LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_latest_topic_of_domain_id.assert_called_once_with(1)

    def test_happy_case(self, mock_request, mock_repo):
        mock_repo.get_latest_topic_of_domain_id.return_value = self.topic
        mock_request.matchdict = self.get_mock_dict({'source_id': '1'})

        topic = get_latest_topic_by_source_id(mock_request)

        assert self.topic_response == topic
        mock_repo.get_latest_topic_of_domain_id.assert_called_once_with(1)
