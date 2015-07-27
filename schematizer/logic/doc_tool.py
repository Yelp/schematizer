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


def get_note_by_id(id):
    return session.query(
        models.Note
    ).filter(
        models.Note.id == id
    ).first()


def update_note(id, note_text, last_updated_by):
    return session.query(
        models.Note
    ).filter(
        models.Note.id == id
    ).update(
        {
            models.Note.note: note_text,
            models.Note.last_updated_by: last_updated_by
        }
    )


def create_note(reference_type, reference_id, note_text, last_updated_by):
    note = models.Note(
        reference_type=reference_type,
        reference_id=reference_id,
        note=note_text,
        last_updated_by=last_updated_by
    )
    session.add(note)
    session.flush()
    return note
