# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from yelp_avro.data_pipeline.avro_meta_data import AvroMetaDataKeys

from schematizer.components.converters import MySQLToAvroConverter
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import mysql_data_types
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable
from testing.models.mysql_data_types import MySQLUnsupportedType


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

    def _convert_and_assert_with_one_column(self, converter,
                                            sql_column, expected_field):
        sql_table = SQLTable(self.table_name, [sql_column])
        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.empty_namespace,
            'fields': [expected_field],
        }
        if sql_column.primary_key_order:
            expected_schema.update(
                {AvroMetaDataKeys.PRIMARY_KEY: [sql_column.name]}
            )
        actual_schema = converter.convert(sql_table)
        assert expected_schema == actual_schema

    def test_convert_with_col_int(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_int', mysql_data_types.MySQLInt(11)),
            {'name': 'col_int', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_col_integer(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_integer', mysql_data_types.MySQLInt(11)),
            {'name': 'col_integer', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_unsigned_int_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col', mysql_data_types.MySQLInt(11, unsigned=True)),
            {
                'name': 'col',
                'type': ['null', 'long'],
                'default': None,
                'unsigned': True
            }
        )

    def test_convert_with_col_bigint(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_bigint', mysql_data_types.MySQLBigInt(11)),
            {'name': 'col_bigint', 'type': ['null', 'long'], 'default': None}
        )

    def test_convert_with_unsigned_bigint_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_bigint',
                mysql_data_types.MySQLBigInt(11, unsigned=True)
            ),
            {
                'name': 'col_bigint',
                'type': ['null', 'long'],
                'default': None,
                'unsigned': True
            }
        )

    def test_convert_with_col_tinyint(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_tinyint', mysql_data_types.MySQLTinyInt(11)),
            {'name': 'col_tinyint', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_unsigned_tinyint_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_tinyint',
                mysql_data_types.MySQLTinyInt(11, unsigned=True)
            ),
            {
                'name': 'col_tinyint',
                'type': ['null', 'int'],
                'default': None,
                'unsigned': True
            }
        )

    def test_convert_with_col_double(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_double', mysql_data_types.MySQLDouble(10, 2)),
            {'name': 'col_double',
             'type': ['null', 'double'],
             'default': None,
             AvroMetaDataKeys.PRECISION: 10,
             AvroMetaDataKeys.SCALE: 2},
        )

    def test_convert_with_col_decimal_bytes(self, converter):
        bytes_decimal = {
            'type': 'bytes',
            'logicalType': 'decimal',
            'precision': 5,
            'scale': 2
        }
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_decimal', mysql_data_types.MySQLDecimal(5, 2)),
            {
                'name': 'col_decimal',
                'type': ['null', bytes_decimal],
                'default': None
            }
        )

    def test_convert_with_col_float(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_float', mysql_data_types.MySQLFloat(10, 2)),
            {'name': 'col_float',
             'type': ['null', 'float'],
             'default': None,
             AvroMetaDataKeys.PRECISION: 10,
             AvroMetaDataKeys.SCALE: 2}
        )

    def test_convert_with_col_char(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_char', mysql_data_types.MySQLChar(16)),
            {'name': 'col_char',
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeys.FIX_LEN: 16}
        )

    def test_convert_with_col_varchar(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_varchar', mysql_data_types.MySQLVarChar(16)),
            {'name': 'col_varchar',
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeys.MAX_LEN: 16}
        )

    def test_convert_with_col_text(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_text', mysql_data_types.MySQLText()),
            {'name': 'col_text', 'type': ['null', 'string'], 'default': None},
        )

    def test_convert_with_col_date(self, converter):
        date_schema = {
            'type': 'int',
            'logicalType': 'date'
        }
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_date', mysql_data_types.MySQLDate()),
            {'name': 'col_date',
             'type': ['null', date_schema]
             'default': None,
             AvroMetaDataKeys.DATE: True}
        )

    def test_convert_with_col_datetime(self, converter):
        timestamp_micros_schema = {
            'type': 'long',
            'logicalType': 'timestamp-micros'
        }
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_datetime', mysql_data_types.MySQLDateTime()),
            {'name': 'col_datetime',
             'type': ['null', timestamp_micros_schema],
             'default': None,
             AvroMetaDataKeys.DATETIME: True}
        )

    def test_convert_with_col_time(self, converter):
        time_micros_schema = {
            'type': 'long',
            'logicalType': 'time-micros'
        }
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_time', mysql_data_types.MySQLTime()),
            {'name': 'col_time',
             'type': ['null', time_micros_schema],
             'default': None,
             AvroMetaDataKeys.TIME: True}
        )

    def test_convert_with_col_year(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_year', mysql_data_types.MySQLYear()),
            {'name': 'col_year',
             'type': ['null', 'long'],
             'default': None,
             AvroMetaDataKeys.YEAR: True}
        )

    def test_convert_with_col_timestamp(self, converter):
        timestamp_micros_schema = {
            'type': 'long',
            'logicalType': 'timestamp-micros'
        }
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_ts', mysql_data_types.MySQLTimestamp()),
            {'name': 'col_ts',
             'type': ['null', timestamp_micros_schema],
             'default': None,
             AvroMetaDataKeys.TIMESTAMP: True}
        )

    def test_convert_with_col_enum(self, converter):
        avro_enum = {
            'name': 'col_enum_enum',
            'namespace': '',
            'type': 'enum',
            'symbols': ['a', 'b']
        }
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_enum', mysql_data_types.MySQLEnum(['a', 'b'])),
            {'name': 'col_enum',
             'type': ['null', avro_enum],
             'default': None}
        )

    def test_convert_with_col_bit(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_bit', mysql_data_types.MySQLBit(6)),
            {'name': 'col_bit',
             'type': ['null', 'int'],
             'default': None,
             AvroMetaDataKeys.BIT_LEN: 6}
        )

    def test_convert_with_col_bool(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_bool', mysql_data_types.MySQLBool()),
            {'name': 'col_bool', 'type': ['null', 'boolean'], 'default': None}
        )

    def test_convert_with_col_boolean(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_boolean', mysql_data_types.MySQLBoolean()),
            {'name': 'col_boolean',
             'type': ['null', 'boolean'],
             'default': None}
        )

    def test_convert_with_col_binary(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_binary', mysql_data_types.MySQLBinary(16)),
            {'name': 'col_binary',
             'type': ['null', 'bytes'],
             'default': None,
             AvroMetaDataKeys.FIX_LEN: 16}
        )

    def test_convert_with_col_varbinary(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_varbinary', mysql_data_types.MySQLVarBinary(16)),
            {'name': 'col_varbinary',
             'type': ['null', 'bytes'],
             'default': None,
             AvroMetaDataKeys.MAX_LEN: 16}
        )

    def test_convert_with_col_blob(self, converter):
        data_type_to_column_name = {
            mysql_data_types.MySQLBlob: 'col_blob',
            mysql_data_types.MySQLTinyBlob: 'col_tiny_blob',
            mysql_data_types.MySQLMediumBlob: 'col_medium_blob',
            mysql_data_types.MySQLLongBlob: 'col_long_blob',
        }
        for data_type, column_name in data_type_to_column_name.iteritems():
            self._convert_and_assert_with_one_column(
                converter,
                SQLColumn(column_name, data_type()),
                {'name': column_name,
                 'type': ['null', 'bytes'],
                 'default': None},
            )

    def test_convert_with_set(self, converter):
        avro_set = {
            'items': {
                'namespace': '',
                'name': 'col_set',
                'symbols': ['a', 'b'],
                'type': 'enum'},
            'type': 'array'
        }

        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_set', mysql_data_types.MySQLSet(['a', 'b'])),
            {
                'name': 'col_set',
                'type': ['null', avro_set],
                'default': None,
            }
        )

    def test_convert_with_unsupported_type(self, converter):
        with pytest.raises(UnsupportedTypeException):
            column = SQLColumn('col', MySQLUnsupportedType())
            sql_table = SQLTable(self.table_name, [column])
            converter.convert(sql_table)

    def test_convert_with_primary_key_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col',
                mysql_data_types.MySQLInt(11),
                primary_key_order=1
            ),
            {'name': 'col',
             'type': ['null', 'int'],
             'default': None,
             AvroMetaDataKeys.PRIMARY_KEY: True}
        )

    def test_convert_with_non_nullable_column(self, converter):
        self._convert_and_assert_with_one_column(
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
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col', mysql_data_types.MySQLInt(11), default_value=10),
            {'name': 'col', 'type': ['int', 'null'], 'default': 10}
        )

    def test_convert_with_non_nullable_without_default_column(self, converter):
        self._convert_and_assert_with_one_column(
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

    def test_convert_with_table_namespace_and_aliases(self, converter):
        doc = 'I am doc'
        aliases = ['foo']
        metadata = {
            MetaDataKey.NAMESPACE: self.namespace,
            MetaDataKey.ALIASES: aliases
        }
        col_name = 'col'
        sql_table = SQLTable(
            self.table_name,
            [SQLColumn(col_name, mysql_data_types.MySQLInt(11))],
            doc=doc,
            **metadata
        )

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': self.namespace,
            'fields': [
                {'name': col_name, 'type': ['null', 'int'], 'default': None}
            ],
            'doc': doc,
            'aliases': aliases
        }

        actual_schema = converter.convert(sql_table)
        assert expected_schema == actual_schema

    def test_convert_with_table_metadata(self, converter):
        pkey_col1 = SQLColumn(
            'pkey_col_one',
            mysql_data_types.MySQLInt(11),
            primary_key_order=1
        )
        pkey_col2 = SQLColumn(
            'pkey_col_two',
            mysql_data_types.MySQLInt(11),
            primary_key_order=2
        )
        col = SQLColumn('col', mysql_data_types.MySQLInt(11))
        sql_table = SQLTable(self.table_name, [pkey_col2, pkey_col1, col])

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': '',
            'fields': [
                {'name': pkey_col2.name,
                 'type': ['null', 'int'],
                 'default': None,
                 AvroMetaDataKeys.PRIMARY_KEY: 2},
                {'name': pkey_col1.name,
                 'type': ['null', 'int'],
                 'default': None,
                 AvroMetaDataKeys.PRIMARY_KEY: 1},
                {'name': col.name,
                 'type': ['null', 'int'],
                 'default': None},
            ],
            AvroMetaDataKeys.PRIMARY_KEY: [pkey_col1.name, pkey_col2.name]
        }

        actual_schema = converter.convert(sql_table)
        assert expected_schema == actual_schema
