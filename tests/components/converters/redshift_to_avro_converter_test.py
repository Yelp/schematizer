# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from yelp_avro.data_pipeline.avro_meta_data import AvroMetaDataKeys

from schematizer.components.converters import RedshiftToAvroConverter
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import redshift_data_types
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable
from testing.models.redshift_data_types import RedshiftUnsupportedType


class TestRedShiftToAvroConverter(object):

    @pytest.fixture
    def converter(self):
        return RedshiftToAvroConverter()

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

    def test_convert_with_col_smallint(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_smallint', redshift_data_types.RedshiftSmallInt()),
            {'name': 'col_smallint', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_col_int2(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_int2', redshift_data_types.RedshiftInt2()),
            {'name': 'col_int2', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_col_int4(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_int4', redshift_data_types.RedshiftInt4()),
            {'name': 'col_int4', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_col_integer(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_integer', redshift_data_types.RedshiftInteger()),
            {'name': 'col_integer', 'type': ['null', 'int'], 'default': None}
        )

    def test_convert_with_col_int8(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_int8', redshift_data_types.RedshiftInt8()),
            {'name': 'col_int8', 'type': ['null', 'long'], 'default': None}
        )

    def test_convert_with_col_bigint(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_bigint', redshift_data_types.RedshiftBigInt()),
            {'name': 'col_bigint', 'type': ['null', 'long'], 'default': None}
        )

    def test_convert_with_col_float4(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_float4', redshift_data_types.RedshiftFloat4()),
            {
                'name': 'col_float4',
                'type': ['null', 'float'],
                'default': None,
            }
        )

    def test_convert_with_col_real(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_real', redshift_data_types.RedshiftReal()),
            {
                'name': 'col_real',
                'type': ['null', 'float'],
                'default': None,
            }
        )

    def test_convert_with_col_double(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_double', redshift_data_types.RedshiftDouble()),
            {
                'name': 'col_double',
                'type': ['null', 'double'],
                'default': None,
            }
        )

    def test_convert_with_col_float(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_float', redshift_data_types.RedshiftFloat()),
            {
                'name': 'col_float',
                'type': ['null', 'double'],
                'default': None,
            }
        )

    def test_convert_with_col_float8(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_float8', redshift_data_types.RedshiftFloat8()),
            {
                'name': 'col_float8',
                'type': ['null', 'double'],
                'default': None,
            }
        )

    def test_convert_with_col_decimal(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_decimal',
                redshift_data_types.RedshiftDecimal(8, 0),
            ),
            {
                'name': 'col_decimal',
                'type': ['null', {
                                    'logicalType': 'decimal',
                                    'scale': 0,
                                    'type': 'bytes', 'precision': 8
                            }
                         ],
                'default': None,
                AvroMetaDataKeys.FIXED_POINT: True
            }
        )

    def test_convert_with_col_numeric(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_numeric',
                redshift_data_types.RedshiftNumeric(8, 0)
            ),
            {
                'name': 'col_numeric',
                'type': ['null', {
                                    'logicalType': 'decimal',
                                    'scale': 0,
                                    'type': 'bytes',
                                    'precision': 8
                            }
                         ],
                'default': None,
                AvroMetaDataKeys.FIXED_POINT: True
            }
        )

    def test_convert_with_col_char(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_char', redshift_data_types.RedshiftChar(16)),
            {
                'name': 'col_char',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.FIX_LEN: 16
            }
        )

    def test_convert_with_col_nchar(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_nchar', redshift_data_types.RedshiftNChar(16)),
            {
                'name': 'col_nchar',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.FIX_LEN: 16
            }
        )

    def test_convert_with_col_bpchar(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_bpchar', redshift_data_types.RedshiftBPChar()),
            {
                'name': 'col_bpchar',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.FIX_LEN: 256
            }
        )

    def test_convert_with_col_character(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_character',
                redshift_data_types.RedshiftCharacter(16)
            ),
            {
                'name': 'col_character',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.FIX_LEN: 16
            }
        )

    def test_convert_with_col_varchar(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_varchar',
                redshift_data_types.RedshiftVarChar(16)
            ),
            {
                'name': 'col_varchar',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.MAX_LEN: 16
            }
        )

    def test_convert_with_col_text(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_text', redshift_data_types.RedshiftText()),
            {
                'name': 'col_text',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.MAX_LEN: 256
            },
        )

    def test_convert_with_col_nvarchar(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_nvarchar',
                redshift_data_types.RedshiftNVarChar(20)
            ),
            {
                'name': 'col_nvarchar',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.MAX_LEN: 20
            },
        )

    def test_convert_with_col_charactervarying(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col_charactervarying',
                redshift_data_types.RedshiftCharacterVarying(20)
            ),
            {
                'name': 'col_charactervarying',
                'type': ['null', 'string'],
                'default': None,
                AvroMetaDataKeys.MAX_LEN: 20
            },
        )

    def test_convert_with_col_date(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_date', redshift_data_types.RedshiftDate()),
            {
                'name': 'col_date',
                'type': ['null', 'int'],
                'default': None,
                AvroMetaDataKeys.DATE: True
            }
        )

    def test_convert_with_col_timestamp(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_ts', redshift_data_types.RedshiftTimestamp()),
            {
                'name': 'col_ts',
                'type': ['null', 'long'],
                'default': None,
                AvroMetaDataKeys.TIMESTAMP: True
            }
        )

    def test_convert_with_col_bool(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_bool', redshift_data_types.RedshiftBool()),
            {'name': 'col_bool', 'type': ['null', 'boolean'], 'default': None}
        )

    def test_convert_with_col_boolean(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn('col_boolean', redshift_data_types.RedshiftBoolean()),
            {'name': 'col_boolean',
             'type': ['null', 'boolean'],
             'default': None}
        )

    def test_convert_with_unsupported_type(self, converter):
        with pytest.raises(UnsupportedTypeException):
            column = SQLColumn('col', RedshiftUnsupportedType())
            sql_table = SQLTable(self.table_name, [column])
            converter.convert(sql_table)

    def test_convert_with_primary_key_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col',
                redshift_data_types.RedshiftInteger(),
                primary_key_order=1
            ),
            {
                'name': 'col',
                'type': ['null', 'int'],
                'default': None,
                AvroMetaDataKeys.PRIMARY_KEY: True
            }
        )

    def test_convert_with_non_nullable_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col',
                redshift_data_types.RedshiftInteger(),
                is_nullable=False,
                default_value=0
            ),
            {'name': 'col', 'type': 'int', 'default': 0}
        )

    def test_convert_with_encode_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col',
                redshift_data_types.RedshiftInteger(),
                is_nullable=False,
                default_value=0,
                **{AvroMetaDataKeys.ENCODE:'lzo'}
            ),
            {'name': 'col', 'type': 'int', 'default': 0, 'encode': 'lzo'}
        )

    def test_convert_with_column_default_value(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col',
                redshift_data_types.RedshiftInteger(),
                default_value=10
            ),
            {'name': 'col', 'type': ['int', 'null'], 'default': 10}
        )

    def test_convert_with_non_nullable_without_default_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            SQLColumn(
                'col',
                redshift_data_types.RedshiftInteger(),
                is_nullable=False
            ),
            {
                'name': 'col',
                'type': 'int'
            }
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
            [SQLColumn(col_name, redshift_data_types.RedshiftInteger())],
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
            redshift_data_types.RedshiftInteger(),
            primary_key_order=1,
            **{
                AvroMetaDataKeys.SORT_KEY:2,
                AvroMetaDataKeys.DIST_KEY:True
            }
        )
        pkey_col2 = SQLColumn(
            'pkey_col_two',
            redshift_data_types.RedshiftInteger(),
            primary_key_order=2,
            **{
                AvroMetaDataKeys.SORT_KEY:1
            }
        )
        col = SQLColumn('col', redshift_data_types.RedshiftInteger())
        sql_table = SQLTable(self.table_name, [pkey_col2, pkey_col1, col])

        expected_schema = {
            'type': 'record',
            'name': self.table_name,
            'namespace': '',
            'fields': [
                {
                    'name': pkey_col2.name,
                    'type': ['null', 'int'],
                    'default': None,
                    AvroMetaDataKeys.PRIMARY_KEY: 2,
                    AvroMetaDataKeys.SORT_KEY: 1
                },
                {
                    'name': pkey_col1.name,
                    'type': ['null', 'int'],
                    'default': None,
                    AvroMetaDataKeys.PRIMARY_KEY: 1,
                    AvroMetaDataKeys.DIST_KEY: True,
                    AvroMetaDataKeys.SORT_KEY: 2
                },
                {
                    'name': col.name,
                    'type': ['null', 'int'],
                    'default': None
                },
            ],
            AvroMetaDataKeys.PRIMARY_KEY: [pkey_col1.name, pkey_col2.name],
            AvroMetaDataKeys.SORT_KEY: [pkey_col2.name, pkey_col1.name],
            AvroMetaDataKeys.DIST_KEY: pkey_col1.name
        }

        actual_schema = converter.convert(sql_table)
        assert expected_schema == actual_schema
