# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

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


class TestUpdateCategory(TestSourcesViewBase):

    def test_non_existing_source_id(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_source_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            source_views.update_category(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_source_by_id.assert_called_once_with(0)

    def test_update_category_new_source_category(
        self,
        mock_request,
        mock_repo,
        mock_doc_tool
    ):
        mock_repo.get_source_by_id.return_value = self.source
        mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
        mock_doc_tool.get_source_category_by_source_id.return_value = None
        mock_doc_tool.create_source_category.return_value = \
            self.source_category
        mock_request.json_body = self.category_request
        source_category = source_views.update_category(mock_request)
        assert source_category == self.source_category_response
        mock_doc_tool.create_source_category.assert_called_once_with(
            0,
            self.category
        )

    def test_update_category_existing_source_category(
        self,
        mock_request,
        mock_repo,
        mock_doc_tool
    ):
        mock_repo.get_source_by_id.return_value = self.source
        mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
        mock_doc_tool.get_source_category_by_source_id.return_value = \
            self.source_category
        mock_doc_tool.update_source_category.return_value = 1
        mock_request.json_body = self.category_request
        source_category = source_views.update_category(mock_request)
        assert source_category == self.source_category_response
        mock_doc_tool.update_source_category.assert_called_once_with(
            0,
            self.category
        )


class TestDeleteCategory(TestSourcesViewBase):

    def test_non_existing_source_id(
        self,
        mock_request,
        mock_repo,
        mock_doc_tool
    ):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_source_by_id.return_value = None
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            source_views.delete_category(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE
        mock_repo.get_source_by_id.assert_called_once_with(0)

    def test_non_existing_category(
        self,
        mock_request,
        mock_repo,
        mock_doc_tool
    ):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_repo.get_source_by_id.return_value = self.source
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            mock_doc_tool.get_source_category_by_source_id.return_value = None
            source_views.delete_category(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.CATEGORY_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(
        self,
        mock_request,
        mock_repo,
        mock_doc_tool
    ):
        mock_repo.get_source_by_id.return_value = self.source
        mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
        mock_doc_tool.get_source_category_by_source_id.return_value = \
            self.source_category
        source_category = source_views.delete_category(mock_request)
        assert source_category == self.source_category_response
        mock_doc_tool.delete_source_category_by_source_id.\
            assert_called_once_with(0)


class TestCreateRefresh(TestApiBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            source_views.create_refresh(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(
            self,
            mock_request,
            refresh_source,
            create_refresh_request,
            refresh_response
    ):
        mock_request.matchdict = self.get_mock_dict(
            {
                'source_id': refresh_source.id
            }
        )
        mock_request.json_body = create_refresh_request
        actual = source_views.create_refresh(mock_request)
        self.assert_equal_refresh_partial(refresh_response, actual)

    def assert_equal_refresh_partial(self, expected, actual):
        assert expected['source']['source_id'] == actual['source']['source_id']
        assert expected['offset'] == actual['offset']
        assert expected['batch_size'] == actual['batch_size']
        assert expected['priority'] == actual['priority']
        assert expected['filter_condition'] == actual['filter_condition']


class TestListRefreshes(TestApiBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = self.get_mock_dict({'source_id': '0'})
            source_views.list_refreshes_by_source_id(mock_request)

        assert expected_exception.code == e.value.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(
            self,
            mock_request,
            refresh_response_list,
            refresh_source
    ):
        mock_request.matchdict = self.get_mock_dict(
            {
                'source_id': refresh_source.id
            }
        )
        actual = source_views.list_refreshes_by_source_id(mock_request)
        assert actual == refresh_response_list
