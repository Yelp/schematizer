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
"""
This module consists of the helper functions for common validations.
"""
from __future__ import absolute_import
from __future__ import unicode_literals


def verify_entity_exists(session, entity_cls, entity_id):
    """Check if the specified entity exists by trying to retrieving it and
    throw exception if not.  The `get_by_id` function will throw
    :class:schematizer.models.exceptions.EntityNotFoundError if not found.
    """
    entity_cls.get_by_id(entity_id)


def verify_truthy_value(value, value_name):
    """Check if the given value is truthy and throw ValueError if not.  The
    `value_name` is a string that describes the value and is used in the error
    message.
    """
    if not value:
        raise ValueError('{} cannot be empty.'.format(value_name))
