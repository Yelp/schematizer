# -*- coding: utf-8 -*-
import pytest

from schematizer import models
from schematizer.logic import doc_tool
from testing import factories
from tests.models.testing_db import DBTestCase


class TestDocTool(DBTestCase):

    @pytest.fixture
    def note_text(self):
        return "qwer<3"

    @pytest.fixture
    def source(self):
        return factories.create_source(
            factories.fake_namespace,
            factories.fake_source
        )

    @pytest.fixture
    def topic(self):
        return factories.create_topic(
            factories.fake_topic_name,
            factories.fake_namespace,
            factories.fake_source
        )

    @property
    def schema_json(self):
        return {
            "name": "foo",
            "namespace": "yelp",
            "type": "record",
            "fields": [{"name": "bar", "type": "int", "doc": "bar"}],
            "doc": "table foo"
        }

    @property
    def schema_elements(self):
        return [
            models.AvroSchemaElement(
                key="yelp.foo",
                element_type="record",
                doc="table foo"
            ),
        ]

    @pytest.fixture
    def schema(self, topic):
        return factories.create_avro_schema(
            self.schema_json,
            self.schema_elements,
            topic_name=topic.name
        )

    @pytest.fixture
    def schema_element(self, schema):
        return schema.avro_schema_elements[0]

    @pytest.fixture
    def table_note(self, source):
        return factories.create_note(
            models.NoteTypeEnum.TABLE,
            source.id,
            self.note_text,
            factories.fake_user_email
        )

    @pytest.fixture
    def field_note(self, schema_element):
        return factories.create_note(
            models.NoteTypeEnum.FIELD,
            schema_element.id,
            self.note_text,
            factories.fake_user_email
        )

    def test_get_note_by_schema_id(self, table_note):
        note = doc_tool.get_note_by_schema_id(table_note.reference_id)
        self.assert_equal_note(table_note, note)

    def test_get_note_by_schema_id_with_no_note(self):
        note = doc_tool.get_note_by_schema_id(1)
        assert note is None

    def test_get_note_by_schema_element_id(self, field_note):
        note = doc_tool.get_note_by_schema_element_id(field_note.reference_id)
        self.assert_equal_note(field_note, note)

    def test_get_note_by_schema_element_id_with_no_note(self):
        note = doc_tool.get_note_by_schema_element_id(1)
        assert note is None

    def assert_equal_note(self, expected, actual):
        assert expected.id == actual.id
        assert expected.note_type == actual.note_type
        assert expected.reference_id == actual.reference_id
        assert expected.note == actual.note
        assert expected.last_updated_by == actual.last_updated_by
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
