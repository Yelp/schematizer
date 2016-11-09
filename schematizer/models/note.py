# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

from sqlalchemy import Column
from sqlalchemy import Enum
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class ReferenceTypeEnum(object):

    # Table level note in the doc tool.
    # Note references an object in the avro_schema table
    SCHEMA = 'schema'
    # Field level note in the doc tool.
    # Note references an object in the avro_schema_element table
    SCHEMA_ELEMENT = 'schema_element'


class Note(Base, BaseModel):

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
