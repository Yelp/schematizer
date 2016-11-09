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
This module contains the internal data structure to hold the information
of redshift SQL schemas.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class RedshiftSQLTable(SQLTable):
    """Internal data structure that represents a redshift sql table.
    """

    def __init__(
        self,
        table_name,
        columns=None,
        doc=None,
        diststyle=None,
        **metadata
    ):
        super(RedshiftSQLTable, self).__init__(
            table_name,
            columns,
            doc,
            **metadata
        )
        self.diststyle = diststyle

    def __eq__(self, other):
        return (type(other) is type(RedshiftSQLTable) and
                self.name == other.name and
                self.columns == other.columns and
                self.diststyle == other.diststyle and
                self.metadata == other.metadata)

    @property
    def sortkeys(self):
        return sorted(
            (col for col in self.columns if col.sort_key_order),
            key=lambda c: c.sort_key_order
        )

    @property
    def distkey(self):
        candidate_distkey = [col for col in self.columns if col.is_dist_key]
        if len(candidate_distkey) > 1:
            raise ValueError(
                "More than one distkey for {table}".format(self.name)
            )
        if candidate_distkey:
            return candidate_distkey[0]  # a table should have one distkey
        else:
            return None


class RedshiftSQLColumn(SQLColumn):
    """Internal data structure that represents a redshift sql column.
    It is intended to support sql column definition in redshift.
    """

    def __init__(self, column_name, column_type, primary_key_order=None,
                 sort_key_order=None, is_dist_key=None, encode=None,
                 is_nullable=True, default_value=None,
                 attributes=None, doc=None, **metadata):
        super(RedshiftSQLColumn, self).__init__(
            column_name,
            column_type,
            primary_key_order,
            is_nullable,
            default_value,
            attributes,
            doc,
            **metadata
        )
        self.sort_key_order = sort_key_order
        self.is_dist_key = is_dist_key
        self.encode = encode

    def __eq__(self, other):
        return (type(other) is type(RedshiftSQLColumn) and
                self.name == other.name and
                self.type == other.type and
                self.primary_key_order == other.primary_key_order and
                self.sort_key_order == other.sort_key_order and
                self.encode == other.encode and
                self.is_dist_key == other.is_dist_key and
                self.is_nullable == other.is_nullable and
                self.default_value == other.default_value and
                self.attributes == other.attributes and
                self.metadata == other.metadata)
