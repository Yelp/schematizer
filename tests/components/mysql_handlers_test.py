# -*- coding: utf-8 -*-
import mock
import pytest

from schematizer.components import mysql_handlers


class TestMySQLHandlers(object):

    @property
    def mysql_statements(self):
        return ["stmt_1", "stmt_2"]

    @pytest.yield_fixture(autouse=True)
    def mock_sqlparser(self):
        with mock.patch('sqlparse.parse', return_value=mock.Mock()) as parser:
            yield parser

    @pytest.fixture
    def cls_foo(self):
        return self.create_mock_handler_class('p1', False)

    @pytest.fixture
    def cls_bar(self):
        return self.create_mock_handler_class('p2', True)

    @pytest.fixture
    def cls_baz(self):
        return self.create_mock_handler_class('p3', True)

    def create_mock_handler_class(self, tag, can_handle):
        mock_instance = mock.Mock(spec=mysql_handlers.MySQLHandlerBase)
        type(mock_instance).can_handle = mock.PropertyMock(
            return_value=can_handle
        )
        mock_instance.run.return_value = mock.Mock(tag=tag)

        # mock_cls is the mock class that implements MySQLHandlerBase class
        mock_cls = mock.Mock(return_value=mock_instance)
        return mock_cls

    @pytest.yield_fixture
    def patch_mysql_handlers(self, cls_foo, cls_bar, cls_baz):
        mysql_handlers.mysql_handlers = [cls_foo, cls_bar, cls_baz]
        yield

    @pytest.yield_fixture
    def patch_mysql_handlers_with_one_handler(self, cls_foo):
        mysql_handlers.mysql_handlers = [cls_foo]
        yield

    @pytest.mark.usefixtures('patch_mysql_handlers')
    def test_create_sql_table_from_mysql_stmts(self):
        actual = mysql_handlers.create_sql_table_from_mysql_stmts(
            self.mysql_statements
        )
        assert 'p2' == actual.tag

    @pytest.mark.usefixtures('patch_mysql_handlers_with_one_handler')
    def test_create_sql_table_from_mysql_stmts_without_handler(self):
        with pytest.raises(mysql_handlers.MySQLHandlerException):
            mysql_handlers.create_sql_table_from_mysql_stmts(
                self.mysql_statements
            )
