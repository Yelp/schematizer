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
    last_updated_by
):
    note = get_note_by_reference_id_and_type(reference_id, reference_type)
    # update the note if one already exists, and return it
    if note is not None:
        _update_note(note.id, note_text, last_updated_by)
        return note
    # create a new note if it does not exist
    return _create_note(
        reference_type,
        reference_id,
        note_text,
        last_updated_by
    )


def _update_note(note_id, note_text, last_updated_by):
    return session.query(
        models.Note
    ).filter(
        models.Note.id == note_id
    ).update(
        {
            models.Note.note: note_text,
            models.Note.last_updated_by: last_updated_by
        }
    )


def _create_note(reference_type, reference_id, note_text, last_updated_by):
    note = models.Note(
        reference_type=reference_type,
        reference_id=reference_id,
        note=note_text,
        last_updated_by=last_updated_by
    )
    session.add(note)
    session.flush()
    return note


def upsert_source_category(source_id, category):
    source_category = _get_source_category_by_source_id(source_id)
    if source_category is not None:
        _update_source_category(source_category.id, category)
        return source_category
    return _create_source_category(source_id, category)


def _get_source_category_by_source_id(source_id):
    return session.query(
        models.SourceCategory
    ).filter(
        models.SourceCategory.source_id == source_id
    ).first()


def _update_source_category(source_category_id, category):
    return session.query(
        models.SourceCategory
    ).filter(
        models.SourceCategory.id == source_category_id
    ).update(
        {
            models.SourceCategory.category: category
        }
    )


def _create_source_category(source_id, category):
    source_category = models.SourceCategory(
        source_id=source_id,
        category=category
    )
    session.add(source_category)
    session.flush()
    return source_category
