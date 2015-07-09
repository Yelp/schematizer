# -*- coding: utf-8 -*-
from schematizer import models

from schematizer.models.database import session


def get_note_by_schema_id(schema_id):
    return session.query(
        models.Note
    ).filter(
        models.Note.note_type == models.NoteTypeEnum.TABLE,
        models.Note.reference_id == schema_id,
    ).first()


def get_note_by_schema_element_id(schema_element_id):
    return session.query(
        models.Note
    ).filter(
        models.Note.note_type == models.NoteTypeEnum.FIELD,
        models.Note.reference_id == schema_element_id,
    ).first()


def create_or_update_table_note(schema_id, note_text, user_email):
    note = get_note_by_schema_id(schema_id)
    # update the note if one already exists, and return it
    if note is not None:
        _update_note(note.id, note_text, user_email)
        return note
    # create a new note if it does not exist
    return _create_note(
        models.NoteTypeEnum.TABLE,
        schema_id,
        note_text,
        user_email
    )


def create_or_update_field_note(
    schema_element_id,
    note_text,
    user_email
):
    note = get_note_by_schema_element_id(schema_element_id)
    # update the note if one already exists, and return it
    if note is not None:
        _update_note(note.id, note_text, user_email)
        return note
    # create a new note if it does not exist
    return _create_note(
        models.NoteTypeEnum.FIELD,
        schema_element_id,
        note_text,
        user_email
    )


def _update_note(note_id, note_text, user_email):
    return session.query(
        models.Note
    ).filter(
        models.Note.id == note_id
    ).update(
        {
            models.Note.note: note_text,
            models.Note.last_updated_by: user_email
        },
        synchronize_session='evaluate'
    )


def _create_note(note_type, reference_id, note_text, user_email):
    note = models.Note(
        note_type=note_type,
        reference_id=reference_id,
        note=note_text,
        last_updated_by=user_email
    )
    session.add(note)
    session.flush()
    return note
