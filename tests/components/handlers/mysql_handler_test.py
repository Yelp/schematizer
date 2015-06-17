# -*- coding: utf-8 -*-
import pytest

from schematizer.components.handlers import mysql_handler
from schematizer.components.handlers import sql_handler_base
from schematizer.models import mysql_data_types as data_types
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class TestMySQLHandler(object):

    @pytest.fixture
    def handler(self):
        return mysql_handler.MySQLHandler()

    @property
    def table_name(self):
        return 'foo'

    @property
    def create_table_sql(self):
        return 'CREATE TABLE `{0}` (`id` int(11) not null);'.format(
            self.table_name
        )

    @property
    def alter_table_sql(self):
        return 'ALTER TABLE `{0}` add `color` varchar(16);'.format(
            self.table_name
        )

    @property
    def create_table_foo_sql(self):
        return ('CREATE TABLE `foo` ('
                '`id` int(11) auto_increment not null, '
                'name varchar(255) null,'
                'amount decimal(10, 2) default 0.0 unsigned,'
                'primary key (id, pid), '
                'unique index (pid)'
                ');')

    @property
    def expected_sql_table_foo(self):
        column_id = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            is_primary_key=True,
            is_nullable=False
        )
        column_name = SQLColumn('name', data_types.MySQLVarChar(255))
        column_amount = SQLColumn(
            'amount',
            data_types.MySQLDecimal(10, 2, unsigned=True),
            default_value='0.0'
        )
        return SQLTable('foo', [column_id, column_name, column_amount])

    def test_create_sql_table_from_sql_stmts(self, handler):
        sql_table = handler.create_sql_table_from_sql_stmts(
            [self.create_table_foo_sql]
        )
        assert self.expected_sql_table_foo == sql_table

    def assert_sql_table_equal_with_create_defs(
        self,
        handler,
        create_definitions,
        expected_columns
    ):
        sql = self._build_create_table_sql(create_definitions)
        actual_table = handler.create_sql_table_from_sql_stmts([sql])
        expected_table = SQLTable(self.table_name, expected_columns)
        assert expected_table == actual_table

    def _build_create_table_sql(self, create_definitions):
        return 'CREATE TABLE `{table}` ({definitions});'.format(
            table=self.table_name,
            definitions=','.join(create_definitions)
        )

    def test_create_sql_table_from_sql_stmts_with_integer_type(self, handler):
        create_definition = '`bar` int(4) not null unsigned'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLInt(4, unsigned=True),
            is_nullable=False,
        )
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_real_num_type(self, handler):
        create_definition = 'bar decimal(10, 2) default 0.0 unsigned,'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLDecimal(10, 2, unsigned=True),
            default_value='0.0'
        )
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_string_type(self, handler):
        create_definition = 'bar char(3) not null'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLChar(3),
            is_nullable=False,
        )
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

        create_definition = 'bar varchar(255) null'
        expected_column = SQLColumn('bar', data_types.MySQLVarChar(255))
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

        create_definition = ('bar text CHARACTER SET latin1 '
                             'COLLATE latin1_german1_ci,')
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLText(
                char_set='latin1',
                collate='latin1_german1_ci'
            ),
        )
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_datetime_type(self, handler):
        create_definition = 'bar timestamp default 10 not null'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLTimestamp(),
            default_value='10',
            is_nullable=False
        )
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

        create_definition = 'bar datetime null'
        expected_column = SQLColumn('bar', data_types.MySQLDateTime())
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_binary_type(self, handler):
        create_definition = 'bar binary(64)'
        expected_column = SQLColumn('bar', data_types.MySQLBinary(64))
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

        create_definition = 'bar blob null'
        expected_column = SQLColumn('bar', data_types.MySQLBlob())
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_enum_type(self, handler):
        create_definition = 'bar enum (a1, a2, a3) character set latin1'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLEnum(['a1', 'a2', 'a3']),
        )
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_primary_keys(self, handler):
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
        self.assert_sql_table_equal_with_create_defs(
            handler,
            create_definitions,
            expected_columns
        )

    def test_create_sql_table_from_sql_stmts_with_set_type(self, handler):
        create_definition = 'bar set (a1, a2, a3) character set latin1'
        expected_column = SQLColumn(
            'bar',
            data_types.MySQLSet(['a1', 'a2', 'a3']),
        )
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    def test_create_sql_table_from_sql_stmts_with_multi_sqls(self, handler):
        sql_table = handler.create_sql_table_from_sql_stmts(
            [self.alter_table_sql, self.create_table_sql]
        )
        expected_column = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            is_nullable=False
        )
        expected_table = SQLTable(self.table_name, [expected_column])
        assert expected_table == sql_table

    def test_create_sql_table_from_sql_stmts_with_non_create_table_sql(
        self,
        handler
    ):
        with pytest.raises(ValueError) as e:
            handler.create_sql_table_from_sql_stmts(
                [self.create_table_sql, self.alter_table_sql]
            )
        assert str(e.value).startswith(
            'parsed_stmt should be a create-table statement'
        )

    def test_create_sql_table_from_sql_stmts_with_bad_sql(self, handler):
        sql = 'CREATE TABLE `foo_tbl` `id` int(11) auto_increment;'
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([sql])
        assert str(e.value).startswith('No column exists in the table.')

    def test_create_sql_table_from_sql_stmts_with_bad_col_type(self, handler):
        sql = 'CREATE TABLE `foo_tbl` (id integer(11));'
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([sql])
        assert 'Unknown MySQL column type integer.' == str(e.value)
