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

import mock
import pytest

from schematizer.components.handlers import sql_handler
from schematizer.components.handlers import sql_handler_base
from schematizer.models.mysql_data_types import MySQLInt
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class TestSQLHandler(object):

    @pytest.fixture
    def raw_sql(self):
        return 'CREATE TABLE `foo` (`id` int(11) not null);'

    @pytest.fixture
    def sql_table(self):
        return SQLTable(
            table_name='foo',
            columns=[SQLColumn('id', MySQLInt(11), is_nullable=False)]
        )

    def test_create_sql_table_from_mysql_sql_stmts(self, raw_sql, sql_table):
        actual = sql_handler.create_sql_table_from_sql_stmts(
            [raw_sql],
            sql_handler_base.SQLDialect.MySQL
        )
        assert actual == sql_table

    def test_create_sql_table_from_unsupported_dialect_sql_stmt(self, raw_sql):
        with pytest.raises(sql_handler_base.SQLHandlerException):
            sql_handler.create_sql_table_from_sql_stmts(
                [raw_sql],
                mock.Mock()
            )
