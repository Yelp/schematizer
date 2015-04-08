# -*- coding: utf-8 -*-
import pytest
from mock import Mock
from mock import patch
from pyramid.httpexceptions import HTTPNotFound

from schematizer.views import constants
from schematizer.views.schemas import get_schema_by_id
from testing import factories


class TestSchemasBase(object):

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
    def default_schema(self):
        return factories.AvroSchemaFactory.create(
            factories.fake_avro_schema,
            self.default_topic.id,
            created_at=None,
            updated_at=None
        )


class TestGetSchemaByID(TestSchemasBase):

    @property
    def request_mock(self):
        dummy_request = Mock()
        dummy_request.matchdict = {'schema_id': '1'}
        return dummy_request

    @patch(
        'schematizer.logic.schema_repository.get_schema_by_id',
        return_value=None
    )
    def test_none_existing_schema(
        self,
        get_schema_by_id_mock
    ):
        with pytest.raises(HTTPNotFound) as e:
            get_schema_by_id(
                self.request_mock
            )
            assert e.value.code == 404
            assert str(e.value) == constants.SCHEMA_NOT_FOUND_ERROR_MESSAGE

    def test_happy_case(self):
        with patch(
            'schematizer.logic.schema_repository.get_schema_by_id',
            return_value=self.default_schema
        ):
            schema = get_schema_by_id(
                self.request_mock
            )
            assert schema == self.default_schema.to_dict()
