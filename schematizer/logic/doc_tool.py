# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import datetime

from sqlalchemy import and_
from sqlalchemy import or_

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


def get_notes_by_schemas_and_elements(schemas, elements):
    if not (schemas or elements):
        return []
    # Can't use a join since sqlalchemy doesn't like the composite foreign key of Note
    schema_ids = [schema.id for schema in schemas]
    element_ids = [element.id for element in elements]

    if schema_ids and element_ids:
        note_filter = or_(
            and_(
                models.Note.reference_type == models.ReferenceTypeEnum.SCHEMA,
                models.Note.reference_id.in_(schema_ids)
            ),
            and_(
                models.Note.reference_type == models.ReferenceTypeEnum.SCHEMA_ELEMENT,
                models.Note.reference_id.in_(element_ids)
            )
        )
    elif schema_ids:
        note_filter = and_ (
            models.Note.reference_type == models.ReferenceTypeEnum.SCHEMA,
            models.Note.reference_id.in_(schema_ids)
        )
    else:
        note_filter = and_(
            models.Note.reference_type == models.ReferenceTypeEnum.SCHEMA_ELEMENT,
            models.Note.reference_id.in_(element_ids)
        )

    return session.query(
        models.Note
    ).filter(
        note_filter
    ).order_by(models.Note.id).all()



def update_note(id, note_text, last_updated_by):
    return session.query(
        models.Note
    ).filter(
        models.Note.id == id
    ).update(
        {
            models.Note.note: note_text,
            models.Note.last_updated_by: last_updated_by,
            models.Note.updated_at: datetime.datetime.utcnow()
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


def get_distinct_categories():
    categories = session.query(models.SourceCategory.category).distinct().all()
    # categories is a list of single item lists. Return a single layered list.
    return [category for category, in categories]


def get_source_categories_by_criteria(namespace_name, source_name=None):
    """Get source_categories by namespace_name, optionally filtering
    by source_name.
    """
    qry = session.query(
       models.SourceCategory
    ).join(
        models.Source,
        models.Namespace
    ).filter(
        models.SourceCategory.source_id == models.Source.id,
        models.Source.namespace_id == models.Namespace.id,
        models.Namespace.name == namespace_name
    )
    if source_name:
        qry.filter(models.Source.name == source_name)
    return qry.order_by(models.SourceCategory.id).all()


def get_source_category_by_source_id(source_id):
    return session.query(
        models.SourceCategory
    ).filter(
        models.SourceCategory.source_id == source_id
    ).first()


def update_source_category(source_id, category):
    return session.query(
        models.SourceCategory
    ).filter(
        models.SourceCategory.source_id == source_id
    ).update(
        {models.SourceCategory.category: category}
    )


def create_source_category(source_id, category):
    source_category = models.SourceCategory(
        source_id=source_id,
        category=category
    )
    session.add(source_category)
    session.flush()
    return source_category


def delete_source_category_by_source_id(source_id):
    return session.query(
        models.SourceCategory
    ).filter(
        models.SourceCategory.source_id == source_id
    ).delete()
