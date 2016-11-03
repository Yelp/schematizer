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
from sqlalchemy.types import Enum

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class DataSourceTypeEnum(object):

    NAMESPACE = 'Namespace'
    SOURCE = 'Source'
    SCHEMA = 'Schema'


class ConsumerGroupDataSource(Base, BaseModel):

    __tablename__ = 'consumer_group_data_source'

    id = Column(Integer, primary_key=True)
    consumer_group_id = Column(
        Integer,
        ForeignKey('consumer_group.id'),
        nullable=False
    )

    # The level that this consumer group is interested in.
    # Value from DataSourceTypeEnum.
    data_source_type = Column(
        Enum(
            DataSourceTypeEnum.NAMESPACE,
            DataSourceTypeEnum.SOURCE,
            DataSourceTypeEnum.SCHEMA,
            name='data_source_type_enum',
        ),
        nullable=False
    )

    # The id of the data_source_type entry in its corresponding table.
    data_source_id = Column(Integer, nullable=False)

    # Timestamp when the entry is created
    created_at = build_time_column(default_now=True, nullable=False)

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )
