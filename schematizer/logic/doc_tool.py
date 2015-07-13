# -*- coding: utf-8 -*-
from schematizer import models

from schematizer.models.database import session


def get_note_by_reference_id_and_type(reference_id, reference_type):
    return session.query(
        models.Note
    ).filter(
        models.Note.reference_type == reference_type,
        models.Note.reference_id == reference_id
    ).first()


def upsert_note(
    reference_id,
    reference_type,
    note_text,
    user_email
):
    note = get_note_by_reference_id_and_type(reference_id, reference_type)
    # update the note if one already exists, and return it
    if note is not None:
        _update_note(note.id, note_text, user_email)
        return note
    # create a new note if it does not exist
    return _create_note(
        reference_type,
        reference_id,
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


def _create_note(reference_type, reference_id, note_text, user_email):
    note = models.Note(
        reference_type=reference_type,
        reference_id=reference_id,
        note=note_text,
        last_updated_by=user_email
    )
    session.add(note)
    session.flush()
    return note
