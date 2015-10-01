# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import contextlib

import mock
import pytest

from schematizer.components.handlers import sql_handler
from schematizer.components.handlers import sql_handler_base


class TestSQLHandler(object):

    @pytest.fixture
    def raw_sql(self):
        return mock.Mock()

    @pytest.fixture
    def parsed_sql(self):
        return mock.Mock()

    def test_create_sql_table_from_sql_stmts_for_mysql_dialect(
        self,
        raw_sql,
        parsed_sql
    ):
        with contextlib.nested(
            mock.patch.object(
                sql_handler.MySQLHandler,
                '_parse',
                return_value=parsed_sql
            ),
            mock.patch.object(
                sql_handler.MySQLHandler,
                '_create_sql_table',
                return_value=mock.Mock()
            )
        ) as (mock_parse_func, mock_create_func):
            sql_handler.create_sql_table_from_sql_stmts(
                [raw_sql],
                sql_handler_base.SQLDialect.MySQL
            )
            mock_parse_func.assert_called_once_with(raw_sql)
            mock_create_func.assert_called_once_with([parsed_sql])

    def test_create_sql_table_from_sql_stmts_with_unsupported_dialect(
        self,
        raw_sql
    ):
        with pytest.raises(sql_handler_base.SQLHandlerException):
            sql_handler.create_sql_table_from_sql_stmts(
                [raw_sql],
                mock.Mock()
            )
