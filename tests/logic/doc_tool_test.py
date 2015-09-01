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
    def user_email(self):
        return "user@yelp.com"

    @pytest.fixture
    def topic(self):
        return factories.create_topic(
            factories.fake_topic_name,
            factories.fake_namespace,
            factories.fake_source,
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
    def schema_note(self, schema):
        return factories.create_note(
            models.ReferenceTypeEnum.SCHEMA,
            schema.id,
            self.note_text,
            self.user_email
        )

    @pytest.fixture
    def schema_element_note(self, schema_element):
        return factories.create_note(
            models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            schema_element.id,
            self.note_text,
            self.user_email
        )

    @pytest.fixture
    def source(self):
        return factories.create_source(
            factories.fake_namespace,
            factories.fake_source
        )

    @property
    def category(self):
        return 'Business Info'

    @property
    def new_category(self):
        return 'Deals'

    @pytest.fixture
    def source_category(self, source):
        return factories.create_source_category(source.id, self.category)

    def test_get_schema_note(self, schema_note):
        note = doc_tool.get_note_by_reference_id_and_type(
            schema_note.reference_id,
            models.ReferenceTypeEnum.SCHEMA
        )
        self.assert_equal_note(schema_note, note)

    def test_get_schema_element_note(self, schema_element_note):
        note = doc_tool.get_note_by_reference_id_and_type(
            schema_element_note.reference_id,
            models.ReferenceTypeEnum.SCHEMA_ELEMENT
        )
        self.assert_equal_note(schema_element_note, note)

    def test_get_note_with_no_note(self):
        note = doc_tool.get_note_by_reference_id_and_type(1, "type")
        assert note is None

    def test_create_schema_note(self, schema):
        actual_note = doc_tool.create_note(
            models.ReferenceTypeEnum.SCHEMA,
            schema.id,
            self.note_text,
            self.user_email
        )
        expected_note = models.Note(
            reference_type=models.ReferenceTypeEnum.SCHEMA,
            reference_id=schema.id,
            note=self.note_text,
            last_updated_by=self.user_email,
        )
        self.assert_equal_note_partial(expected_note, actual_note)

    def test_update_schema_note(self, schema_note):
        new_text = "This is new text"
        new_user = "user2@yelp.com"
        doc_tool.update_note(
            schema_note.id,
            new_text,
            new_user
        )
        expected_note = models.Note(
            reference_type=models.ReferenceTypeEnum.SCHEMA,
            reference_id=schema_note.reference_id,
            note=new_text,
            last_updated_by=new_user,
        )
        self.assert_equal_note_partial(expected_note, schema_note)

    def test_create_schema_element_note(self, schema_element):
        actual_note = doc_tool.create_note(
            models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            schema_element.id,
            self.note_text,
            self.user_email
        )
        expected_note = models.Note(
            reference_type=models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            reference_id=schema_element.id,
            note=self.note_text,
            last_updated_by=self.user_email,
        )
        self.assert_equal_note_partial(expected_note, actual_note)

    def test_update_schema_element_note(self, schema_element_note):
        new_text = "This is new text"
        new_user = "user2@yelp.com"
        doc_tool.update_note(
            schema_element_note.id,
            new_text,
            new_user
        )
        expected_note = models.Note(
            reference_type=schema_element_note.reference_type,
            reference_id=schema_element_note.reference_id,
            note=new_text,
            last_updated_by=new_user,
        )
        self.assert_equal_note_partial(expected_note, schema_element_note)

    def test_get_distinct_categories(self, source_category):
        actual = doc_tool.get_distinct_categories()
        assert actual == [source_category.category]

    def test_get_source_category(self, source, source_category):
        actual = doc_tool.get_source_category_by_source_id(source.id)
        self.assert_equal_source_category(source_category, actual)

    def test_create_source_category(self, source):
        actual = doc_tool.create_source_category(source.id, self.category)
        expected = models.SourceCategory(
            source_id=source.id,
            category=self.category
        )
        self.assert_equal_source_category_partial(expected, actual)

    def test_update_source_category(self, source, source_category):
        doc_tool.update_source_category(
            source.id,
            self.new_category
        )
        expected = models.SourceCategory(
            source_id=source_category.source_id,
            category=self.new_category
        )
        self.assert_equal_source_category_partial(expected, source_category)

    def assert_equal_note(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_note_partial(expected, actual)

    def assert_equal_note_partial(self, expected, actual):
        assert expected.reference_type == actual.reference_type
        assert expected.reference_id == actual.reference_id
        assert expected.note == actual.note
        assert expected.last_updated_by == actual.last_updated_by

    def assert_equal_source_category(self, expected, actual):
        assert expected.id == actual.id
        assert expected.created_at == actual.created_at
        assert expected.updated_at == actual.updated_at
        self.assert_equal_source_category_partial(expected, actual)

    def assert_equal_source_category_partial(self, expected, actual):
        assert expected.source_id == actual.source_id
        assert expected.category == actual.category
