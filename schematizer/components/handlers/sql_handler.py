# -*- coding: utf-8 -*-
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
