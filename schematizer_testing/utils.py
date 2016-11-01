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

from schematizer.models.database import session


def get_entity_by_id(entity_cls, entity_id):
    return session.query(entity_cls).filter(
        getattr(entity_cls, 'id') == entity_id
    ).one()


def get_entity_by_kwargs(entity_cls, **filter_kwargs):
    query = session.query(entity_cls)
    for col in filter_kwargs:
        query.filter(getattr(entity_cls, col) == filter_kwargs[col])
    return query.all()
