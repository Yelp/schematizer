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

from schematizer.components.handlers import sql_handler_base
from schematizer.components.handlers.mysql_handler import LoggingMySQLHandler


_sql_handlers = {
    sql_handler_base.SQLDialect.SQL: None,
    sql_handler_base.SQLDialect.MySQL: LoggingMySQLHandler
}


def create_sql_table_from_sql_stmts(sqls, dialect=None):
    dialect = dialect or sql_handler_base.SQLDialect.SQL
    handler = _sql_handlers.get(dialect)
    if not handler:
        raise sql_handler_base.SQLHandlerException(
            "Unable to process {0} statements {1}.".format(dialect, sqls)
        )
    return handler().create_sql_table_from_sql_stmts(sqls)
