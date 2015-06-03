# -*- coding: utf-8 -*-
import mock
import pytest

from schematizer.components.handlers import mysql_handler
from schematizer.components.handlers import sql_handler_base
from schematizer.models import mysql_data_types as data_types
from schematizer.models.sql_entities import SQLColumn


class TestMySQLHandler(object):

    @property
    def mysql_statements(self):
        return ["stmt_1", "stmt_2"]

    @pytest.yield_fixture
    def mock_parser(self):
        with mock.patch(
            'sqlparse.parse',
            return_value=(mock.Mock(),)
        ) as parser:
            yield parser

    @pytest.fixture
    def cls_foo(self):
        return self.create_mock_builder_class('p1', False)

    @pytest.fixture
    def cls_bar(self):
        return self.create_mock_builder_class('p2', True)

    @pytest.fixture
    def cls_baz(self):
        return self.create_mock_builder_class('p3', True)

    @pytest.fixture
    def handler(self):
        return mysql_handler.MySQLHandler()

    def create_mock_builder_class(self, tag, can_handle):
        mock_instance = mock.Mock(spec=sql_handler_base.SQLTableBuilderBase)
        type(mock_instance).can_handle = mock.PropertyMock(
            return_value=can_handle
        )
        mock_instance.run.return_value = mock.Mock(tag=tag)

        # mock_cls is the mock class that implements SQLTableBuilderBase class
        mock_cls = mock.Mock(return_value=mock_instance)
        return mock_cls

    @pytest.yield_fixture
    def patch_multi_sqltble_builders(self, handler, cls_foo, cls_bar, cls_baz):
        handler._builders = [cls_foo, cls_bar, cls_baz]
        yield

    @pytest.yield_fixture
    def patch_one_sqltable_builder(self, handler, cls_foo):
        handler._builders = [cls_foo]
        yield

    @pytest.mark.usefixtures('patch_multi_sqltble_builders', 'mock_parser')
    def test_create_sql_table_from_mysql_stmts_with_builders(self, handler):
        actual = handler.create_sql_table_from_sql_stmts(
            self.mysql_statements
        )
        assert 'p2' == actual.tag

    @pytest.mark.usefixtures('patch_one_sqltable_builder', 'mock_parser')
    def test_create_sql_table_from_sql_stmts_without_builder(self, handler):
        with pytest.raises(sql_handler_base.SQLHandlerException):
            handler.create_sql_table_from_sql_stmts(
                self.mysql_statements
            )

    @property
    def create_table_sql(self):
        return ('CREATE TABLE `foo_tbl` ('
                '`id` int(11) auto_increment not null, '
                'name varchar(255) null,'
                'amount decimal(10, 2) default 0.0 unsigned,'
                'primary key (id, pid), '
                'unique index (pid)'
                ');')

    def test_create_sql_table_from_sql_stmts(self, handler):
        sql_table = handler.create_sql_table_from_sql_stmts(
            [self.create_table_sql]
        )
        assert 'foo_tbl' == sql_table.name

        expected_column = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            is_primary_key=True,
            is_nullable=False
        )
        assert expected_column == sql_table.columns[0]

        expected_column = SQLColumn('name', data_types.MySQLVarChar(255))
        assert expected_column == sql_table.columns[1]

        expected_column = SQLColumn(
            'amount',
            data_types.MySQLDecimal(10, 2, unsigned=True),
            default_value='0.0'
        )
        assert expected_column == sql_table.columns[2]


class TestBuildFromFinalCreateTableOnly(object):

    @property
    def create_table_sql(self):
        return 'CREATE TABLE `foo_tbl` (`id` int(11) not null);'

    @property
    def alter_table_sql(self):
        return 'ALTER TABLE `foo_tbl` add `color` varchar(16);'

    @pytest.fixture
    def handler(self):
        handler = mysql_handler.MySQLHandler()
        handler._builders = [mysql_handler.BuildFromFinalCreateTableOnly]
        return handler

    def test_run(self, handler):
        sql_table = handler.create_sql_table_from_sql_stmts(
            [self.create_table_sql]
        )

        assert 'foo_tbl' == sql_table.name
        assert 1 == len(sql_table.columns)

        expected_column = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            is_nullable=False
        )
        assert expected_column == sql_table.columns[0]

    @property
    def table_name(self):
        return 'foo'

    def run_type_test(self, handler, create_definitions, expected_columns):
        sql = self.build_create_table_sql(create_definitions)
        sql_table = handler.create_sql_table_from_sql_stmts([sql])
        self.assert_sql_table_equal(expected_columns, sql_table)

    def build_create_table_sql(self, create_definitions):
        return 'CREATE TABLE `{table}` ({definitions});'.format(
            table=self.table_name,
            definitions=','.join(create_definitions)
        )

    def assert_sql_table_equal(self, expected_columns, actual_table):
        assert self.table_name == actual_table.name
        assert len(expected_columns) == len(actual_table.columns)
        for i, expected_column in enumerate(expected_columns):
            assert expected_column == actual_table.columns[i]

    def test_run_with_integer_type(self, handler):
        create_definition = '`bar` int(4) not null unsigned'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLInt(4, unsigned=True),
            is_nullable=False,
        )
        self.run_type_test(handler, [create_definition], [expected_column])

    def test_run_with_real_number_type(self, handler):
        create_definition = 'bar decimal(10, 2) default 0.0 unsigned,'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLDecimal(10, 2, unsigned=True),
            default_value='0.0'
        )
        self.run_type_test(handler, [create_definition], [expected_column])

    def test_run_with_string_type(self, handler):
        create_definition = 'bar char(3) not null'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLChar(3),
            is_nullable=False,
        )
        self.run_type_test(handler, [create_definition], [expected_column])

        create_definition = 'bar varchar(255) null'
        expected_column = SQLColumn('bar', data_types.MySQLVarChar(255))
        self.run_type_test(handler, [create_definition], [expected_column])

        create_definition = ('bar text CHARACTER SET latin1 '
                             'COLLATE latin1_german1_ci,')
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLText(
                char_set='latin1',
                collate='latin1_german1_ci'
            ),
        )
        self.run_type_test(handler, [create_definition], [expected_column])

    def test_run_with_datetime_type(self, handler):
        create_definition = 'bar timestamp default 10 not null'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLTimestamp(),
            default_value='10',
            is_nullable=False
        )
        self.run_type_test(handler, [create_definition], [expected_column])

        create_definition = 'bar datetime(3)'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLDateTime(fsp=3),
        )
        self.run_type_test(handler, [create_definition], [expected_column])

    def test_run_with_binary_type(self, handler):
        create_definition = 'bar binary(64)'
        expected_column = SQLColumn('bar', data_types.MySQLBinary(64))
        self.run_type_test(handler, [create_definition], [expected_column])

        create_definition = 'bar blob null'
        expected_column = SQLColumn('bar', data_types.MySQLBlob())
        self.run_type_test(handler, [create_definition], [expected_column])

    def test_run_with_enum_type(self, handler):
        create_definition = 'bar enum (a1, a2, a3) character set latin1'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLEnum(['a1', 'a2', 'a3']),
        )
        self.run_type_test(handler, [create_definition], [expected_column])

    def test_run_with_primary_keys(self, handler):
        create_definitions = [
            'id int(11) not null',
            'pid int(11) not null',
            'tag char(3)',
            'primary key(id, pid)'
        ]
        expected_columns = [
            SQLColumn(
                'id',
                data_types.MySQLInt(11),
                is_nullable=False,
                is_primary_key=True
            ),
            SQLColumn(
                'pid',
                data_types.MySQLInt(11),
                is_nullable=False,
                is_primary_key=True
            ),
            SQLColumn('tag', data_types.MySQLChar(3))
        ]
        self.run_type_test(handler, create_definitions, expected_columns)

    def test_run_with_set_type(self, handler):
        create_definition = 'bar set (a1, a2, a3) character set latin1'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLSet(['a1', 'a2', 'a3']),
        )
        self.run_type_test(handler, [create_definition], [expected_column])

    def test_create_sql_table_from_sql_stmts_with_non_create_table_sql(
        self,
        handler
    ):
        with pytest.raises(sql_handler_base.SQLHandlerException):
            handler.create_sql_table_from_sql_stmts(
                [self.create_table_sql, self.alter_table_sql]
            )

    def test_create_sql_table_from_sql_stmts_with_invalid_sql(self, handler):
        sql = 'CREATE TABLE `foo_tbl` `id` int(11) auto_increment;'
        sql_table = handler.create_sql_table_from_sql_stmts([sql])
        assert 'foo_tbl' == sql_table.name
        assert 0 == len(sql_table.columns)

        sql = 'CREATE TABLE `foo_tbl` (id integer(11));'
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([sql])
        assert 'Unknown MySQL column type integer.' == str(e.value)
