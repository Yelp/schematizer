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
from sqlalchemy.orm import relationship

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.refresh import Refresh
from schematizer.models.source_category import SourceCategory
from schematizer.models.topic import Topic
from schematizer.models.types.time import build_time_column


class Source(Base, BaseModel):

    __tablename__ = 'source'
    __table_args__ = (
        UniqueConstraint(
            'name',
            'namespace_id',
            name='name_namespace_id_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Source of the Avro schema, such as table "User",
    # or log "service.foo" etc.
    name = Column(String, nullable=False)

    # Email address of the source owner.
    owner_email = Column(String, nullable=False)

    namespace_id = Column(
        Integer,
        ForeignKey('namespace.id'),
        nullable=False
    )

    topics = relationship(Topic, backref="source")

    refreshes = relationship(Refresh, backref="source")

    # The category relationship object for this source.
    # A source matches 1-to-1 with a SourceCategory.
    category = relationship(SourceCategory, uselist=False, backref="source")

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
