# -*- coding: utf-8 -*-
import contextlib
import pytest
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound

from schematizer.views import constants
from schematizer.views.sources import get_latest_topic_by_source_id
from schematizer.views.sources import get_source_by_id
from schematizer.views.sources import list_sources
from schematizer.views.sources import list_topics_by_source_id
from testing import factories


class TestSourcesBase(object):

    @property
    def default_source(self):
        return factories.DomainFactory.create(
            factories.fake_namespace,
            factories.fake_source,
            created_at=None,
            updated_at=None
        )

    @property
    def default_topic(self):
        return factories.TopicFactory.create(
            factories.fake_topic_name,
            self.default_source.id,
            created_at=None,
            updated_at=None
        )

    @property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'source_id': '1'}
        return dummy_request


class TestListSources(TestSourcesBase):

    def test_no_sources(self):
        with patch(
            'schematizer.logic.schema_repository.get_domains',
            return_value=[]
        ):
            sources = list_sources(
                self.request_mock
            )
            assert sources == []

    def test_happy_case(self):
        with patch(
            'schematizer.logic.schema_repository.get_domains',
            return_value=[self.default_source]
        ):
            sources = list_sources(
                self.request_mock
            )
            assert sources == [self.default_source.to_dict()]


class TestGetSourceByID(TestSourcesBase):

    @patch(
        'schematizer.logic.schema_repository.get_domain_by_domain_id',
        return_value=None
    )
    def test_none_existing_source_id(
        self,
        get_domain_by_domain_id_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            get_source_by_id(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.SOURCE_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self):
        with patch(
            'schematizer.logic.schema_repository.get_domain_by_domain_id',
            return_value=self.default_source
        ):
            source = get_source_by_id(
                self.request_mock
            )
            assert source == self.default_source.to_dict()


class TestListTopicsBySourceID(TestSourcesBase):

    @patch(
        'schematizer.logic.schema_repository.is_domain_id_existing',
        return_value=False
    )
    @patch(
        'schematizer.logic.schema_repository.get_topics_by_domain_id',
        return_value=[]
    )
    def test_none_existing_source_id(
        self,
        is_domain_id_existing_mock,
        get_topics_by_domain_id_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            list_topics_by_source_id(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.SOURCE_NOT_FOUND_ERROR_MESSAGE

    @patch(
        'schematizer.logic.schema_repository.is_domain_id_existing',
        return_value=True
    )
    def test_no_topics(
        self,
        is_domain_id_existing_mock
    ):
        with patch(
            'schematizer.logic.schema_repository.get_topics_by_domain_id',
            return_value=[]
        ):
            topics = list_topics_by_source_id(
                self.request_mock
            )
            assert topics == []

    @patch(
        'schematizer.logic.schema_repository.is_domain_id_existing',
        return_value=True
    )
    def test_happy_case(
        self,
        is_domain_id_existing_mock
    ):
        with patch(
            'schematizer.logic.schema_repository.get_topics_by_domain_id',
            return_value=[self.default_topic]
        ):
            topics = list_topics_by_source_id(
                self.request_mock
            )
            assert topics == [self.default_topic.to_dict()]


class TestGetLatestTopicBySourceID(TestSourcesBase):

    @patch(
        'schematizer.logic.schema_repository.is_domain_id_existing',
        return_value=False
    )
    @patch(
        'schematizer.logic.schema_repository.'
        'get_latest_topic_of_domain_id',
        return_value=None
    )
    def test_none_existing_source_id(
        self,
        is_domain_id_existing_mock,
        get_latest_topic_of_domain_id_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            get_latest_topic_by_source_id(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.SOURCE_NOT_FOUND_ERROR_MESSAGE

    @patch(
        'schematizer.logic.schema_repository.is_domain_id_existing',
        return_value=True
    )
    def test_no_latest_topic(
        self,
        is_domain_id_existing_mock
    ):
        with contextlib.nested(
            patch(
                'schematizer.logic.schema_repository.'
                'get_latest_topic_of_domain_id',
                return_value=None
            ),
            pytest.raises(HTTPNotFound)
        ) as (_, e):
            get_latest_topic_by_source_id(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) \
                == constants.LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE

    @patch(
        'schematizer.logic.schema_repository.is_domain_id_existing',
        return_value=True
    )
    def test_happy_case(
        self,
        is_domain_id_existing_mock
    ):
        with patch(
            'schematizer.logic.schema_repository.'
            'get_latest_topic_of_domain_id',
            return_value=self.default_topic
        ):
            topic = get_latest_topic_by_source_id(
                self.request_mock
            )
            assert topic == self.default_topic.to_dict()
