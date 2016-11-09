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

from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class Producer(Base):

    __tablename__ = 'producer'
    __table_args__ = (
        UniqueConstraint(
            'job_name',
            'schema_id',
            name='job_schema_unique_constraint'
        ),
    )

    id = Column(Integer, primary_key=True)

    # Email address of the producer.
    email = Column(String, nullable=False)

    # Name of the job, process or service.
    job_name = Column(String, nullable=False)

    # How many seconds does the producer expect to use the schema.
    # This would be used to deprecate old schemas.
    expected_frequency = Column(Integer, nullable=False)

    # ID of the Avro schema this producer uses
    schema_id = Column(
        Integer,
        ForeignKey('avro_schema.id'),
        nullable=False
    )

    # Timestamp when this producer uses the schema last time
    last_used_at = build_time_column()

    # Timestamp when the entry is created
    created_at = build_time_column(default_now=True, nullable=False)

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )
