# -*- coding: utf-8 -*-
import pytest

from schematizer.api.exceptions import exceptions_v1
from schematizer.views import notes as note_views
from tests.views.api_test_base import TestApiBase


class TestNotesViewBase(TestApiBase):

    test_view_module = 'schematizer.views.notes'


class TestCreateNote(TestNotesViewBase):

    def test_create_note(self, mock_request, mock_repo, mock_doc_tool):
        mock_request.json_body = self.create_note_request
        mock_repo.get_schema_by_id.return_value = self.schema
        mock_doc_tool.create_note.return_value = self.note
        actual = note_views.create_note(mock_request)
        assert actual == self.note_response

    def test_non_existing_schema(self, mock_request, mock_repo):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = self.create_note_request
            mock_repo.get_schema_by_id.return_value = None
            note_views.create_note(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.REFERENCE_NOT_FOUND_ERROR_MESSAGE


class TestUpdateNote(TestNotesViewBase):

    def test_update_note(self, mock_request, mock_doc_tool):
        mock_request.json_body = self.update_note_request
        mock_request.matchdict = self.get_mock_dict({'note_id': '1'})
        mock_doc_tool.get_note_by_id.return_value = self.note
        mock_doc_tool.update_note.return_value = self.note
        actual = note_views.update_note(mock_request)
        assert actual == self.note_response

    def test_update_non_existing_note(self, mock_request, mock_doc_tool):
        expected_exception = self.get_http_exception(404)
        with pytest.raises(expected_exception) as e:
            mock_request.json_body = self.update_note_request
            mock_request.matchdict = self.get_mock_dict({'note_id': '1'})
            mock_doc_tool.get_note_by_id.return_value = None
            note_views.update_note(mock_request)

        assert e.value.code == expected_exception.code
        assert str(e.value) == exceptions_v1.NOTE_NOT_FOUND_ERROR_MESSAGE
