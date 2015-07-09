# -*- coding: utf-8 -*-
import pytest

from schematizer import models
from schematizer.logic import doc_tool
from testing import factories
from tests.models.testing_db import DBTestCase


class TestDocTool(DBTestCase):

    @property
    def note_text(self):
        return "qwer<3"

    @property
    def user(self):
        return factories.fake_user_email

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
    def table_note(self, schema):
        return factories.create_note(
            models.NoteTypeEnum.TABLE,
            schema.id,
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

    def test_create_and_update_table_note(self, schema):
        # Test table note creation
        actual_note = doc_tool.create_or_update_table_note(
            schema.id,
            self.note_text,
            self.user
        )
        expected_note = models.Note(
            note_type=models.NoteTypeEnum.TABLE,
            reference_id=schema.id,
            note=self.note_text,
            last_updated_by=self.user,
        )
        self.assert_equal_note_partial(expected_note, actual_note)

        # Test table note update
        new_text = "This is new text"
        new_user = "user2@yelp.com"
        actual_note = doc_tool.create_or_update_table_note(
            schema.id,
            "This is new text",
            "user2@yelp.com"
        )
        expected_note = models.Note(
            note_type=models.NoteTypeEnum.TABLE,
            reference_id=schema.id,
            note=new_text,
            last_updated_by=new_user,
        )
        self.assert_equal_note_partial(expected_note, actual_note)

    def test_create_and_update_field_note(self, schema_element):
        # Test field note creation
        actual_note = doc_tool.create_or_update_field_note(
            schema_element.id,
            self.note_text,
            self.user
        )
        expected_note = models.Note(
            note_type=models.NoteTypeEnum.FIELD,
            reference_id=schema_element.id,
            note=self.note_text,
            last_updated_by=self.user,
        )
        self.assert_equal_note_partial(expected_note, actual_note)

        # Test field note update
        new_text = "This is new text"
        new_user = "user2@yelp.com"
        actual_note = doc_tool.create_or_update_field_note(
            schema_element.id,
            "This is new text",
            "user2@yelp.com"
        )
        expected_note = models.Note(
            note_type=models.NoteTypeEnum.FIELD,
            reference_id=schema_element.id,
            note=new_text,
            last_updated_by=new_user,
        )
        self.assert_equal_note_partial(expected_note, actual_note)

    def assert_equal_note(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_note_partial(expected, actual)

    def assert_equal_note_partial(self, expected, actual):
        assert expected.note_type == actual.note_type
        assert expected.reference_id == actual.reference_id
        assert expected.note == actual.note
        assert expected.last_updated_by == actual.last_updated_by
