# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.api.exceptions import exceptions_v1 as exc_v1
from schematizer.logic import doc_tool
from schematizer.views import sources as source_views
from schematizer_testing import factories
from tests.views.api_test_base import ApiTestBase


class TestListSources(ApiTestBase):

    def test_no_sources(self, mock_request):
        mock_request.params = {}
        actual = source_views.list_sources(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, biz_source):
        mock_request.params = {}
        actual = source_views.list_sources(mock_request)
        expected = [self.get_expected_src_resp(biz_source.id)]
        assert actual == expected

    def test_limit_sources_by_count(
        self,
        mock_request,
        biz_source,
        another_biz_source
    ):
        """ Tests that sources are filtered by count. """
        mock_request.params = {
            'count': 1
        }
        actual = source_views.list_sources(mock_request)

        # Without the count param, length would be 2
        assert len(actual) == 1

    def test_limit_sources_by_min_id(
        self,
        mock_request,
        biz_source,
        another_biz_source
    ):
        """ Tests that filtering by min_id returns all the sources which have
        id equal to or greater than min_id.
        """
        min_id = another_biz_source.id
        expected = [self.get_expected_src_resp(another_biz_source.id)]
        mock_request.params = {
            'min_id': min_id
        }
        actual = source_views.list_sources(mock_request)

        assert actual == expected


class TestGetSourceByID(ApiTestBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': '0'}
            source_views.get_source_by_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_source):
        mock_request.matchdict = {'source_id': str(biz_source.id)}
        actual = source_views.get_source_by_id(mock_request)
        expected = self.get_expected_src_resp(biz_source.id)
        assert actual == expected


class TestListTopicsBySourceID(ApiTestBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': '0'}
            source_views.list_topics_by_source_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_no_topics(self, mock_request, biz_source):
        mock_request.matchdict = {'source_id': str(biz_source.id)}
        actual = source_views.list_topics_by_source_id(mock_request)
        assert actual == []

    def test_happy_case(self, mock_request, biz_source, biz_topic):
        mock_request.matchdict = {'source_id': str(biz_source.id)}
        actual = source_views.list_topics_by_source_id(mock_request)
        expected = [self.get_expected_topic_resp(biz_topic.id)]
        assert actual == expected


class TestGetLatestTopicBySourceID(ApiTestBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': '0'}
            source_views.get_latest_topic_by_source_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_no_latest_topic(self, mock_request, biz_source):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': str(biz_source.id)}
            source_views.get_latest_topic_by_source_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_source, biz_topic):
        # create another topic after biz_topic for the same source
        latest_topic = factories.create_topic(
            'new_topic',
            namespace_name=biz_source.namespace.name,
            source_name=biz_source.name
        )
        expected = self.get_expected_topic_resp(latest_topic.id)

        mock_request.matchdict = {'source_id': str(biz_source.id)}
        actual = source_views.get_latest_topic_by_source_id(mock_request)

        assert actual == expected


class TestUpdateCategory(ApiTestBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': '0'}
            source_views.update_category(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_add_new_source_category(self, mock_request, biz_source):
        mock_request.matchdict = {'source_id': str(biz_source.id)}
        mock_request.json_body = {'category': 'Deals'}
        actual = source_views.update_category(mock_request)

        expected = self._get_expected_category_response(biz_source.id, 'Deals')
        assert actual == expected

    def test_update_existing_source_category(self, mock_request, biz_source):
        factories.create_source_category(biz_source.id, 'Biz')
        mock_request.matchdict = {'source_id': str(biz_source.id)}
        mock_request.json_body = {'category': 'Sales'}
        actual = source_views.update_category(mock_request)

        expected = self._get_expected_category_response(biz_source.id, 'Sales')
        assert actual == expected

    def _get_expected_category_response(self, source_id, expected_category):
        src_category = doc_tool.get_source_category_by_source_id(source_id)
        return {
            'source_id': source_id,
            'category': expected_category,
            'created_at': src_category.created_at.isoformat(),
            'updated_at': src_category.updated_at.isoformat()
        }


class TestDeleteCategory(ApiTestBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': '0'}
            source_views.delete_category(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_non_existing_category(self, mock_request, biz_source):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': str(biz_source.id)}
            source_views.delete_category(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.CATEGORY_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_source):
        doc_tool.create_source_category(biz_source.id, 'Biz')
        src_category = doc_tool.get_source_category_by_source_id(biz_source.id)
        expected = {
            'source_id': src_category.source_id,
            'category': 'Biz',
            'created_at': src_category.created_at.isoformat(),
            'updated_at': src_category.updated_at.isoformat()
        }

        mock_request.matchdict = {'source_id': str(biz_source.id)}
        actual = source_views.delete_category(mock_request)
        assert actual == expected


class TestCreateRefresh(ApiTestBase):

    @pytest.fixture
    def request_json(self):
        return {
            'offset': 100,
            'batch_size': 500,
            'priority': 80,
            'avg_rows_per_second_cap': 1000
        }

    def test_non_existing_source_id(self, mock_request, request_json):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': '0'}
            mock_request.json_body = request_json
            source_views.create_refresh(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_source, request_json):
        mock_request.matchdict = {'source_id': str(biz_source.id)}
        mock_request.json_body = request_json
        actual = source_views.create_refresh(mock_request)

        expected = self.get_expected_src_refresh_resp(
            actual['refresh_id'],
            offset=100,
            batch_size=500,
            priority=80,
            avg_rows_per_second_cap=1000
        )
        assert actual == expected
        assert actual['source_name'] == biz_source.name
        assert actual['namespace_name'] == biz_source.namespace.name

    def test_happy_case_no_cap(self, mock_request, biz_source, request_json):
        mock_request.matchdict = {'source_id': str(biz_source.id)}
        mock_request.json_body = request_json
        del request_json['avg_rows_per_second_cap']
        actual = source_views.create_refresh(mock_request)

        expected = self.get_expected_src_refresh_resp(
            actual['refresh_id'],
            offset=100,
            batch_size=500,
            priority=80,
        )
        assert actual == expected
        assert actual['source_name'] == biz_source.name
        assert actual['namespace_name'] == biz_source.namespace.name


class TestListRefreshes(ApiTestBase):

    def test_non_existing_source_id(self, mock_request):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.matchdict = {'source_id': '0'}
            source_views.list_refreshes_by_source_id(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exc_v1.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self, mock_request, biz_source, biz_src_refresh):
        mock_request.matchdict = {'source_id': biz_source.id}
        actual = source_views.list_refreshes_by_source_id(mock_request)
        expected = [self.get_expected_src_refresh_resp(biz_src_refresh.id)]
        assert actual == expected
