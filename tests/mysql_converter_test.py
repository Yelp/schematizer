# -*- coding: utf-8 -*-
import pytest

from schematizer.mysql_converter import MySqlConverter
from schematizer.converter_base import MetaDataKeyEnum
from schematizer.converter_base import SchemaConversionException
from schematizer.converter_base import UnsupportedTypeException
from schematizer.sql_entities import SQLAttribute
from schematizer.sql_entities import SQLColumn
from schematizer.sql_entities import SQLColumnDataType
from schematizer.sql_entities import SQLTable


class TestSqlConverter(object):

    @pytest.fixture
    def converter(self):
        return MySqlConverter()

    @property
    def table_name(self):
        return 'foo'

    @property
    def column_name(self):
        return 'col'

    @property
    def empty_namespace(self):
        return ''

    @property
    def namespace(self):
        return 'ns'

    def test_create_avro_schema_not_nullable_column(self, converter):
        column = SQLColumn(
            self.column_name,
            SQLColumnDataType('int'),
            is_primary_key=True
        )
        sql_table = SQLTable(self.table_name, [column])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': self.column_name,
                        'type': ['null', 'int'],
                        MetaDataKeyEnum.PRIMARY_KEY: True,
                        'default': None}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_primary_key_column(self, converter):
        column = SQLColumn(
            self.column_name,
            SQLColumnDataType('int'),
            attributes=[SQLAttribute('default', 0, True),
                        SQLAttribute('not null', None, False)]
        )
        sql_table = SQLTable(self.table_name, [column])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': self.column_name,
                        'type': 'int',
                        'default': 0}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_string_column(self, converter):
        column_char_name = self.column_name + '_char'
        column_char = SQLColumn(
            column_char_name,
            SQLColumnDataType('char', length=16)
        )
        column_varchar_name = self.column_name + '_varchar'
        column_varchar = SQLColumn(
            column_varchar_name,
            SQLColumnDataType('varchar', length=32)
        )
        sql_table = SQLTable(self.table_name, [column_char, column_varchar])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': column_char_name,
                        'type': ['null', 'string'],
                        MetaDataKeyEnum.FIX_LEN: 16,
                        'default': None},
                       {'name': column_varchar_name,
                        'type': ['null', 'string'],
                        MetaDataKeyEnum.MAX_LEN: 32,
                        'default': None}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_numeric_column(self, converter):
        column_float_name = self.column_name + '_float'
        column_float = SQLColumn(
            column_float_name,
            SQLColumnDataType('float', length=10, decimal=2)
        )
        column_decimal_name = self.column_name + '_decimal'
        column_decimal = SQLColumn(
            column_decimal_name,
            SQLColumnDataType('decimal', length=10)
        )
        sql_table = SQLTable(self.table_name, [column_float, column_decimal])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': column_float_name,
                        'type': ['null', 'float'],
                        MetaDataKeyEnum.LENGTH: 10,
                        MetaDataKeyEnum.DECIMAL: 2,
                        'default': None},
                       {'name': column_decimal_name,
                        'type': ['null', 'double'],
                        MetaDataKeyEnum.LENGTH: 10,
                        'default': None}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_timestamp_column(self, converter):
        column = SQLColumn(
            self.column_name,
            SQLColumnDataType('timestamp')
        )
        sql_table = SQLTable(self.table_name, [column])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': self.column_name,
                        'type': ['null', 'long'],
                        MetaDataKeyEnum.TIMESTAMP: True,
                        'default': None}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_enum_column(self, converter):
        column = SQLColumn(
            self.column_name,
            SQLColumnDataType('enum', values=['a', 'b'])
        )
        sql_table = SQLTable(self.table_name, [column])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_field_type = {
            'type': 'enum',
            'name': converter.get_enum_type_name(column),
            'namespace': self.empty_namespace,
            'symbols': ['a', 'b']
        }
        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': self.column_name,
                        'type': ['null', expected_field_type],
                        'default': None}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_binary_column(self, converter):
        column = SQLColumn(
            self.column_name,
            SQLColumnDataType('binary', length=16)
        )
        sql_table = SQLTable(self.table_name, [column])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_field_type = {
            'type': 'fixed',
            'name': converter.get_fixed_type_name(column),
            'namespace': self.empty_namespace,
            'size': 16
        }
        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': self.column_name,
                        'type': ['null', expected_field_type],
                        'default': None}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_unsigned_int_column(self, converter):
        column = SQLColumn(
            self.column_name,
            SQLColumnDataType('int'),
            attributes=[SQLAttribute('unsigned', None, False)]
        )
        sql_table = SQLTable(self.table_name, [column])
        actual_schema = converter.create_avro_schema(sql_table)

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [{'name': self.column_name,
                        'type': ['null', 'int'],
                        MetaDataKeyEnum.UNSIGNED: True,
                        'default': None}],
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_metadata(self, converter):
        expected_aliases = ['new_foo']
        expected_doc = 'sample doc'
        column = SQLColumn('col', SQLColumnDataType('varchar', length=256))
        sql_table = SQLTable(
            self.table_name,
            [column],
            namespace=self.namespace,
            aliases=expected_aliases,
            doc=expected_doc
        )
        actual_schema = converter.create_avro_schema(sql_table)

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.namespace,
            'fields': [{'name': 'col',
                        'type': ['null', 'string'],
                        MetaDataKeyEnum.MAX_LEN: 256,
                        'default': None}],
            'aliases': expected_aliases,
            'doc': expected_doc
        }
        assert expected_schema == actual_schema

    def test_create_avro_schema_with_none_table(self, converter):
        actual_schema = converter.create_avro_schema(None)
        assert actual_schema is None

    def test_create_avro_schema_with_non_sqltable(self, converter):
        with pytest.raises(SchemaConversionException):
            converter.create_avro_schema("create table")

    def test_create_avro_schema_with_unsupported_type(self, converter):
        with pytest.raises(UnsupportedTypeException):
            column = SQLColumn('col', SQLColumnDataType('blob'))
            sql_table = SQLTable(
                self.table_name,
                [column]
            )
            converter.create_avro_schema(sql_table)
