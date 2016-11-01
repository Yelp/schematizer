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
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class SourceCategory(Base, BaseModel):

    __tablename__ = 'source_category'
    __tableargs__ = (
        UniqueConstraint(
            'source_id',
            name='source_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # ID of the source this entry refers to
    source_id = Column(
        Integer,
        ForeignKey('source.id'),
        nullable=False
    )

    # Category that the source belongs to
    category = Column(String, nullable=False)

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
