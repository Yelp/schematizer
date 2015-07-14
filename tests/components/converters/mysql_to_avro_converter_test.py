# -*- coding: utf-8 -*-
import pytest

from schematizer.components.converters import MySQLToAvroConverter
from schematizer.components.converters.converter_base \
    import AvroMetaDataKeyEnum
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import mysql_data_types
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class TestMySQLToAvroConverter(object):

    @pytest.fixture
    def converter(self):
        return MySQLToAvroConverter()

    @property
    def table_name(self):
        return 'foo'

    @property
    def empty_namespace(self):
        return ''

    @property
    def namespace(self):
        return 'ns'

    def convert_with_one_column(self, converter, sql_column, expected_field):
        sql_table = SQLTable(self.table_name, [sql_column])
        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [expected_field],
        }
        actual_schema = converter.convert(sql_table)
        assert expected_schema == actual_schema

    def test_convert_with_col_int(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_int', mysql_data_types.MySQLInt(11)),
            {'name': 'col_int', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_col_bigint(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_bigint', mysql_data_types.MySQLBigInt(11)),
            {'name': 'col_bigint', 'type': ['null', 'long'], 'default': None}
        )

    def test_convert_with_col_tinyint(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_tinyint', mysql_data_types.MySQLTinyInt(11)),
            {'name': 'col_tinyint', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_col_double(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_double', mysql_data_types.MySQLDouble(10, 2)),
            {'name': 'col_double',
             'type': ['null', 'double'],
             'default': None,
             AvroMetaDataKeyEnum.PRECISION: 10,
             AvroMetaDataKeyEnum.SCALE: 2},
        )

    def test_convert_with_col_decimal(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_decimal', mysql_data_types.MySQLDecimal(8, 0)),
            {'name': 'col_decimal',
             'type': ['null', 'double'],
             'default': None,
             AvroMetaDataKeyEnum.PRECISION: 8,
             AvroMetaDataKeyEnum.SCALE: 0,
             AvroMetaDataKeyEnum.FIXED_POINT: True}
        )

    def test_convert_with_col_float(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_float', mysql_data_types.MySQLFloat(10, 2)),
            {'name': 'col_float',
             'type': ['null', 'float'],
             'default': None,
             AvroMetaDataKeyEnum.PRECISION: 10,
             AvroMetaDataKeyEnum.SCALE: 2}
        )

    def test_convert_with_col_char(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_char', mysql_data_types.MySQLChar(16)),
            {'name': 'col_char',
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeyEnum.FIX_LEN: 16}
        )

    def test_convert_with_col_varchar(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_varchar', mysql_data_types.MySQLVarChar(16)),
            {'name': 'col_varchar',
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeyEnum.MAX_LEN: 16}
        )

    def test_convert_with_col_text(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_text', mysql_data_types.MySQLText()),
            {'name': 'col_text', 'type': ['null', 'string'], 'default': None},
        )

    def test_convert_with_col_timestamp(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col_ts', mysql_data_types.MySQLTimestamp()),
            {'name': 'col_ts',
             'type': ['null', 'long'],
             'default': None,
             AvroMetaDataKeyEnum.TIMESTAMP: True}
        )

    def test_convert_with_col_enum(self, converter):
        avro_enum = {
            'name': 'col_enum_enum',
            'namespace': '',
            'type': 'enum',
            'symbols': ['a', 'b']
        }
        self.convert_with_one_column(
            converter,
            SQLColumn('col_enum', mysql_data_types.MySQLEnum(['a', 'b'])),
            {'name': 'col_enum',
             'type': ['null', avro_enum],
             'default': None}
        )

    def test_convert_with_unsupported_type(self, converter):
        with pytest.raises(UnsupportedTypeException):
            column = SQLColumn('col', mysql_data_types.MySQLBlob())
            sql_table = SQLTable(self.table_name, [column])
            converter.convert(sql_table)

    def test_convert_with_primary_key_column(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn(
                'col',
                mysql_data_types.MySQLInt(11),
                primary_key_order=1
            ),
            {'name': 'col',
             'type': ['null', 'int'],
             'default': None,
             AvroMetaDataKeyEnum.PRIMARY_KEY: True}
        )

    def test_convert_with_non_nullable_column(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn(
                'col',
                mysql_data_types.MySQLInt(11),
                is_nullable=False,
                default_value=0
            ),
            {'name': 'col', 'type': 'int', 'default': 0}
        )

    def test_convert_with_column_default_value(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col', mysql_data_types.MySQLInt(11), default_value=10),
            {'name': 'col', 'type': ['int', 'null'], 'default': 10}
        )

    def test_convert_with_unsigned_int_column(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col', mysql_data_types.MySQLInt(11, unsigned=True)),
            {'name': 'col',
             'type': ['null', 'int'],
             'default': None,
             'unsigned': True}
        )

    def test_convert_with_non_nullable_without_default_column(self, converter):
        self.convert_with_one_column(
            converter,
            SQLColumn('col', mysql_data_types.MySQLInt(11), is_nullable=False),
            {'name': 'col',
             'type': 'int'}
        )

    def test_convert_with_none_table(self, converter):
        actual_schema = converter.convert(None)
        assert actual_schema is None

    def test_convert_with_non_sqltable(self, converter):
        with pytest.raises(SchemaConversionException):
            converter.convert("create table")

    def test_convert_with_table_metadata(self, converter):
        doc = 'I am doc'
        aliases = ['foo']
        metadata = {
            MetaDataKey.NAMESPACE: self.namespace,
            MetaDataKey.ALIASES: aliases
        }
        sql_table = SQLTable(
            self.table_name,
            [SQLColumn('col', mysql_data_types.MySQLInt(11))],
            doc=doc,
            **metadata
        )
        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.namespace,
            'fields': [
                {'name': 'col', 'type': ['null', 'int'], 'default': None}
            ],
            'doc': doc,
            'aliases': aliases
        }
        actual_schema = converter.convert(sql_table)
        assert expected_schema == actual_schema
