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


class SQLDialect(object):

    SQL = 'sql'
    MySQL = 'mysql'


class SQLHandlerBase(object):

    __metaclass__ = abc.ABCMeta

    dialect = None

    def create_sql_table_from_sql_stmts(self, sqls):
        """Parse and process the raw table related SQL statements of specific
        SQL dialect, and generate the corresponding SQLTable object.
        """
        parsed_sqls = [self._parse(sql) for sql in sqls]
        table = self._create_sql_table(parsed_sqls)
        if not table.columns:
            raise SQLHandlerException(
                "No column exists in the table. Raw sqls: {0}".format(sqls)
            )
        return table

    def _parse(self, sql):
        raise NotImplementedError()

    def _create_sql_table(self, parsed_sqls):
        raise NotImplementedError()


class SQLHandlerException(Exception):
    pass
