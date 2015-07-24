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
                'name varchar(255),'
                'amount decimal(10, 2) default 0.0 unsigned,'
                'primary key (id) '
                ');')

    @property
    def expected_sql_table_foo(self):
        column_id = SQLColumn(
            'id',
            data_types.MySQLInt(11),
            primary_key_order=1,
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

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('`bar` bit(4) not null',
         SQLColumn(
             'bar',
             data_types.MySQLBit(4),
             is_nullable=False
         )),
        ('bar bit null',
         SQLColumn('bar', data_types.MySQLBit(None))),
    ])
    def test_create_sql_table_from_sql_stmts_with_bit_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('`bar` int(4) not null unsigned',
         SQLColumn(
             'bar',
             data_types.MySQLInt(4, unsigned=True),
             is_nullable=False
         )),
        ('bar tinyint null',
         SQLColumn('bar', data_types.MySQLTinyInt(None))),
        ('bar smallint null',
         SQLColumn('bar', data_types.MySQLSmallInt(None))),
        ('bar bigint null',
         SQLColumn('bar', data_types.MySQLBigInt(None))),
        ('bar integer null',
         SQLColumn('bar', data_types.MySQLInteger(None))),
    ])
    def test_create_sql_table_from_sql_stmts_with_integer_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('`bar` bool not null',
         SQLColumn(
             'bar',
             data_types.MySQLBool(),
             is_nullable=False
         )),
        ('bar bool null',
         SQLColumn('bar', data_types.MySQLBool())),
        ('bar boolean null',
         SQLColumn('bar', data_types.MySQLBoolean())),
    ])
    def test_create_sql_table_from_sql_stmts_with_boolean_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar decimal(10, 2) default 0.0 unsigned',
         SQLColumn(
             'bar',
             data_types.MySQLDecimal(10, 2, unsigned=True),
             default_value='0.0'
         )),
        ('bar double null',
         SQLColumn('bar', data_types.MySQLDouble(None, None))),
        ('bar numeric(10) null',
         SQLColumn('bar', data_types.MySQLNumeric(10, None))),
    ])
    def test_create_sql_table_from_sql_stmts_with_real_num_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar char(3) not null',
         SQLColumn('bar', data_types.MySQLChar(3), is_nullable=False)),
        ('bar varchar(255) null',
         SQLColumn('bar', data_types.MySQLVarChar(255))),
        ('bar text CHARACTER SET latin1 COLLATE latin1_german1_ci',
         SQLColumn('bar', data_types.MySQLText(
             char_set='latin1',
             collate='latin1_german1_ci'
         ))),
    ])
    def test_create_sql_table_from_sql_stmts_with_string_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar timestamp default 10 not null',
         SQLColumn(
             'bar',
             data_types.MySQLTimestamp(),
             default_value='10',
             is_nullable=False
         )),
        ('bar datetime null', SQLColumn('bar', data_types.MySQLDateTime())),
    ])
    def test_create_sql_table_from_sql_stmts_with_datetime_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar binary(64)', SQLColumn('bar', data_types.MySQLBinary(64))),
        ('bar blob null', SQLColumn('bar', data_types.MySQLBlob())),
    ])
    def test_create_sql_table_from_sql_stmts_with_binary_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar enum (a1, a2, a3) character set latin1',
         SQLColumn('bar', data_types.MySQLEnum(['a1', 'a2', 'a3']))),
    ])
    def test_create_sql_table_from_sql_stmts_with_enum_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
        self.assert_sql_table_equal_with_create_defs(
            handler,
            [create_definition],
            [expected_column]
        )

    @pytest.mark.parametrize(("create_definition", "expected_column"), [
        ('bar set (a1, a2, a3) character set latin1',
         SQLColumn('bar', data_types.MySQLSet(['a1', 'a2', 'a3']))),
    ])
    def test_create_sql_table_from_sql_stmts_with_set_type(
        self,
        handler,
        create_definition,
        expected_column
    ):
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
                primary_key_order=1
            ),
            SQLColumn(
                'pid',
                data_types.MySQLInt(11),
                is_nullable=False,
                primary_key_order=2
            ),
            SQLColumn('tag', data_types.MySQLChar(3))
        ]
        self.assert_sql_table_equal_with_create_defs(
            handler,
            create_definitions,
            expected_columns
        )

    def test_create_sql_table_column_types_are_case_insensitive(self, handler):
        create_definitions = [
            'lows int(11)',
            'CAPS INT(11)',
            'MiXeD INt(11)'
        ]
        expected_columns = [
            SQLColumn(
                'lows',
                data_types.MySQLInt(11)
            ),
            SQLColumn(
                'CAPS',
                data_types.MySQLInt(11)
            ),
            SQLColumn(
                'MiXeD',
                data_types.MySQLInt(11)
            ),
        ]
        self.assert_sql_table_equal_with_create_defs(
            handler,
            create_definitions,
            expected_columns
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
        sql = 'CREATE TABLE `foo_tbl` (id basscannon(11));'
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([sql])
        assert 'Unknown MySQL column type basscannon.' == str(e.value)

    def test_create_sql_table_from_sql_stmts_with_no_stmts(self, handler):
        with pytest.raises(sql_handler_base.SQLHandlerException) as e:
            handler.create_sql_table_from_sql_stmts([])
        assert 'Unable to process MySQL statements [].' == str(e.value)
