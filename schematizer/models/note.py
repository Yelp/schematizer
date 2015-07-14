# -*- coding: utf-8 -*-
from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class ReferenceTypeEnum(object):

    # Table level note in the doc tool.
    # Note references an object in the avro_schema table
    SCHEMA = 'schema'
    # Field level note in the doc tool.
    # Note references an object in the avro_schema_element table
    SCHEMA_ELEMENT = 'schema_element'


class Note(Base):

    __tablename__ = 'note'
    __table_args__ = (
        UniqueConstraint(
            'reference_type',
            'reference_id',
            name='ref_type_ref_id_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # The type of object the note is referring to
    reference_type = Column(
        Enum(
            ReferenceTypeEnum.SCHEMA,
            ReferenceTypeEnum.SCHEMA_ELEMENT,
            name='note_type_enum',
        ),
        nullable=False
    )

    # The id of the object the note is referring to.
    # The db table for the id is specified by reference_type
    reference_id = Column(Integer, nullable=False)

    # The note text
    note = Column('note', Text, nullable=False)

    # The email of the last user to update the note
    last_updated_by = Column(String, nullable=False)

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
            'type': self.reference_type,
            'reference_id': self.reference_id,
            'note': self.note,
            'last_updated_by': self.last_updated_by,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
