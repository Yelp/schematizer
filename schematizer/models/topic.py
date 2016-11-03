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

from data_pipeline_avro_util.data_pipeline.avro_meta_data \
    import AvroMetaDataKeys
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import relationship

from schematizer.models.avro_schema import AvroSchema
from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class Topic(Base, BaseModel):

    __tablename__ = 'topic'
    __table_args__ = (
        UniqueConstraint(
            'name',
            name='topic_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Topic name.
    name = Column(String, nullable=False)

    # The associated source_id for this topic.
    source_id = Column(
        Integer,
        ForeignKey('source.id'),
        nullable=False
    )

    avro_schemas = relationship(AvroSchema, backref="topic")

    _contains_pii = Column('contains_pii', Integer, nullable=False)

    @property
    def contains_pii(self):
        return bool(self._contains_pii)

    @contains_pii.setter
    def contains_pii(self, value):
        if not isinstance(value, bool):
            raise ValueError(
                "Type of contains_pii should be bool."
            )

        self._contains_pii = int(value)

    cluster_type = Column(String, nullable=False)

    @property
    def primary_keys(self):
        if not self.avro_schemas:
            return []

        return self.avro_schemas[0].avro_schema_json.get(
            AvroMetaDataKeys.PRIMARY_KEY,
            []
        )

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

    def __eq__(self, other):
        return type(self) is type(other) and self._key == other._key

    def __hash__(self):
        return hash(self._key)

    @property
    def _key(self):
        return (
            self.id,
            self.name,
            self.source_id,
            self.contains_pii,
            self.cluster_type,
            self.created_at,
            self.updated_at
        )
