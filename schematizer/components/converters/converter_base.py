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

import abc

from schematizer import models


class BaseConverter(object):

    source_type = models.SchemaKindEnum.Unknown
    target_type = models.SchemaKindEnum.Unknown

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def convert(self, src_schema):
        """Convert the given source schema to the target schema type."""
        raise NotImplementedError()


class SchemaConversionException(Exception):
    pass


class UnsupportedTypeException(SchemaConversionException):
    pass
