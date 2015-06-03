# -*- coding: utf-8 -*-
import contextlib
import mock
import pytest

from schematizer.components.handlers import sql_handler
from schematizer.components.handlers import sql_handler_base


class TestSQLHandler(object):

    def test_create_sql_table_from_sql_stmts(self):
        with contextlib.nested(
            mock.patch.object(
                sql_handler.MySQLHandler,
                '_parse',
                return_value=mock.Mock()
            ),
            mock.patch.object(
                sql_handler.MySQLHandler,
                '_create_sql_table',
                return_value=mock.Mock()
            )
        ) as (mock_parse_func, mock_create_func):
            sql = mock.Mock()
            sql_handler.create_sql_table_from_sql_stmts(
                [sql],
                sql_handler_base.SQLDialect.MySQL
            )
            mock_parse_func.assert_called_once_with(sql)

    def test_create_sql_table_from_sql_stmts_with_unsupported_dialetct(self):
        with pytest.raises(sql_handler_base.SQLHandlerException):
            sql_handler.create_sql_table_from_sql_stmts(
                mock.Mock()
            )
