# -*- coding: utf-8 -*-
import contextlib
import pytest
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound

from schematizer.views import constants
from schematizer.views.topics import get_latest_schema_by_topic_name
from schematizer.views.topics import get_topic_by_topic_name
from schematizer.views.topics import list_schemas_by_topic_name
from testing import factories


class TestTopicBase(object):

    @property
    def default_topic(self):
        return factories.TopicFactory.create(
            factories.fake_topic_name,
            1,
            created_at=None,
            updated_at=None
        )

    @property
    def default_schema(self):
        return factories.AvroSchemaFactory.create(
            factories.fake_avro_schema,
            self.default_topic.id,
            created_at=None,
            updated_at=None
        )

    @property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'topic_name': 'yelp.business.v1'}
        return dummy_request


class TestGetTopicByTopicName(TestTopicBase):

    @patch(
        'schematizer.logic.schema_repository.get_topic_by_name',
        return_value=None
    )
    def test_none_existing_topic_name(
        self,
        get_topic_by_name_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            get_topic_by_topic_name(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self):
        with patch(
            'schematizer.logic.schema_repository.get_topic_by_name',
            return_value=self.default_topic
        ):
            topic = get_topic_by_topic_name(
                self.request_mock
            )
            assert topic == self.default_topic.to_dict()


class TestListSchemasByTopicName(TestTopicBase):

    @patch(
        'schematizer.logic.schema_repository.get_topic_by_name',
        return_value=None
    )
    @patch(
        'schematizer.logic.schema_repository.get_schemas_by_topic_name',
        return_value=[]
    )
    def test_none_existing_topic_name(
        self,
        get_topic_by_name_mock,
        get_schemas_by_topic_name_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            list_schemas_by_topic_name(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_none_existing_schemas(self):
        with contextlib.nested(
            patch(
                'schematizer.logic.schema_repository.get_topic_by_name',
                return_value=self.default_topic
            ),
            patch(
                'schematizer.logic.schema_repository'
                '.get_schemas_by_topic_name',
                return_value=[]
            )
        ):
            schemas = list_schemas_by_topic_name(
                self.request_mock
            )
            assert schemas == []

    def test_happy_case(self):
        with contextlib.nested(
            patch(
                'schematizer.logic.schema_repository.get_topic_by_name',
                return_value=self.default_topic
            ),
            patch(
                'schematizer.logic.schema_repository'
                '.get_schemas_by_topic_name',
                return_value=[self.default_schema]
            )
        ):
            schemas = list_schemas_by_topic_name(
                self.request_mock
            )
            assert schemas == [self.default_schema.to_dict()]


class TestGetLatestSchemaByTopicName(TestTopicBase):

    @patch(
        'schematizer.logic.schema_repository.get_topic_by_name',
        return_value=None
    )
    @patch(
        'schematizer.logic.schema_repository.'
        'get_latest_schema_by_topic_name',
        return_value=None
    )
    def test_none_existing_topic_name(
        self,
        get_topic_by_name_mock,
        get_latest_schema_by_topic_name_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            get_latest_schema_by_topic_name(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.TOPIC_NOT_FOUND_ERROR_MESSAGE

    def test_no_latest_schema(self):
        with contextlib.nested(
            patch(
                'schematizer.logic.schema_repository.get_topic_by_name',
                return_value=self.default_topic
            ),
            patch(
                'schematizer.logic.schema_repository.'
                'get_latest_schema_by_topic_name',
                return_value=None
            ),
            pytest.raises(HTTPNotFound)
        ) as (_, _, e):
            get_latest_schema_by_topic_name(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) \
                == constants.LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self):
        with contextlib.nested(
            patch(
                'schematizer.logic.schema_repository.get_topic_by_name',
                return_value=self.default_topic
            ),
            patch(
                'schematizer.logic.schema_repository.'
                'get_latest_schema_by_topic_name',
                return_value=self.default_schema
            )
        ):
            schema = get_latest_schema_by_topic_name(
                self.request_mock
            )
            assert schema == self.default_schema.to_dict()
