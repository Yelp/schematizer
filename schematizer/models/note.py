# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class NoteTypeEnum(object):

    # Table level note in the doc tool.
    # Note references an object in the avro_schema table
    TABLE = 'table'
    # Field level note in the doc tool.
    # Note references an object in the avro_schema_element table
    FIELD = 'field'


class Note(Base):

    __tablename__ = 'note'
    __table_args__ = (
        UniqueConstraint(
            'note_type',
            'reference_id',
            name='note_type_reference_id_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # The type of object the note is referring to
    note_type = Column(
        Enum(
            NoteTypeEnum.TABLE,
            NoteTypeEnum.FIELD,
            name='note_type_enum',
        ),
        nullable=False
    )

    # The id of the object the note is referring to.
    # The db table for the id is specified by note_type
    reference_id = Column(Integer, nullable=False)

    # The note text
    note = Column('note', Text)

    # The email of the last user to update the note
    last_updated_by = Column(String)

    # Timestamp when the entry is created
    created_at = build_time_column(
        default_now=True,
        nullable=False
    )

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )

    def to_dict(self):
        return {
            'id': self.id,
            'type': self.note_type,
            'reference_id': self.reference_id,
            'note': self.note,
            'last_updated_by': self.last_updated_by,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
