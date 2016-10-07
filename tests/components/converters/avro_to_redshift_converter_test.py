# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest
from yelp_avro.data_pipeline.avro_meta_data import AvroMetaDataKeys

from schematizer.components.converters import AvroToRedshiftConverter
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import redshift_data_types as redshift_types
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLColumn
from schematizer.models.sql_entities import SQLTable


class TestAvroToRedshiftConverter(object):

    @pytest.fixture
    def converter(self):
        return AvroToRedshiftConverter()

    @property
    def avro_schema_name(self):
        return 'foo'

    @property
    def redshift_schema_name(self):
        return 'bar'

    @property
    def empty_namespace(self):
        return ''

    @property
    def namespace(self):
        return 'ns'

    @property
    def col_name(self):
        return 'col'

    @property
    def table_aliases(self):
        return ['bar']

    def _convert_and_assert_with_one_column(
            self,
            converter,
            avro_field,
            expected_column
    ):
        record_schema = self.compose_record_schema(avro_field)
        expected_table = SQLTable(
            self.avro_schema_name,
            columns=[expected_column],
            doc=record_schema.get('doc'),
            schema_name=self.redshift_schema_name,
            **self.get_table_metadata()
        )
        actual_table = converter.convert(record_schema)
        assert expected_table == actual_table

    def compose_record_schema(self, avro_field):
        return {
            'type': 'record',
            'name': self.avro_schema_name,
            'namespace': self.namespace,
            'fields': [avro_field],
            'doc': 'sample doc',
            'aliases': self.table_aliases,
            'schema_name': self.redshift_schema_name
        }

    def get_table_metadata(self):
        return {
            MetaDataKey.NAMESPACE: self.namespace,
            MetaDataKey.ALIASES: self.table_aliases
        }

    def test_convert_with_field_int(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name, 'type': ['null', 'int'], 'default': None},
            SQLColumn(self.col_name, redshift_types.RedshiftInteger())
        )

    def test_convert_with_field_long(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name, 'type': ['null', 'long'], 'default': None},
            SQLColumn(self.col_name, redshift_types.RedshiftBigInt())
        )

    def test_convert_with_field_double(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'double'],
             'default': None,
             AvroMetaDataKeys.PRECISION: 10,
             AvroMetaDataKeys.SCALE: 2},
            SQLColumn(self.col_name, redshift_types.RedshiftDouble()),
        )

    def test_convert_with_field_double_with_fixed_flag(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'double'],
             'default': None,
             AvroMetaDataKeys.PRECISION: 8,
             AvroMetaDataKeys.SCALE: 0,
             AvroMetaDataKeys.FIXED_POINT: True},
            SQLColumn(self.col_name, redshift_types.RedshiftDecimal(8, 0))
        )

    def test_convert_with_field_decimal(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null',
                      {'logicalType': 'decimal',
                       AvroMetaDataKeys.PRECISION: 10,
                       AvroMetaDataKeys.SCALE: 2,
                       'type': 'bytes'}
                      ]},
            SQLColumn(self.col_name, redshift_types.RedshiftDecimal(10, 2))
        )

    def test_convert_with_field_date(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null',
                      {'logicalType': 'date', 'type': 'int'}
                      ]},
            SQLColumn(self.col_name, redshift_types.RedshiftDate())
        )

    def test_convert_with_field_float(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'float'],
             'default': None,
             AvroMetaDataKeys.PRECISION: 10,
             AvroMetaDataKeys.SCALE: 2},
            SQLColumn(self.col_name, redshift_types.RedshiftReal())
        )

    def test_convert_with_field_string_with_fixed_len(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeys.FIX_LEN: 16},
            SQLColumn(self.col_name, redshift_types.RedshiftVarChar(16))
        )

    def test_convert_with_field_string_with_max_len(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeys.MAX_LEN: 16},
            SQLColumn(self.col_name, redshift_types.RedshiftVarChar(32))
        )

    def test_convert_with_field_bytes_with_max_len(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'bytes'],
             'default': None,
             AvroMetaDataKeys.MAX_LEN: 16},
            SQLColumn(self.col_name, redshift_types.RedshiftVarChar(16))
        )

    def test_convert_string_field_with_exceeded_max_len(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeys.MAX_LEN: 32768},
            SQLColumn(self.col_name, redshift_types.RedshiftVarChar(65535))
        )

    def test_convert_bytes_field_with_exceeded_max_len(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'bytes'],
             'default': None,
             AvroMetaDataKeys.MAX_LEN: 65536},
            SQLColumn(self.col_name, redshift_types.RedshiftVarChar(65535))
        )

    def test_convert_with_field_string_without_specified_len(self, converter):
        with pytest.raises(SchemaConversionException):
            record_schema = self.compose_record_schema(
                {'name': self.col_name,
                 'type': 'string',
                 'default': ''}
            )
            converter.convert(record_schema)

    def test_convert_with_field_timestamp(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'long'],
             'default': None,
             AvroMetaDataKeys.TIMESTAMP: True},
            SQLColumn(self.col_name, redshift_types.RedshiftTimestamp())
        )

    def test_convert_with_field_boolean(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'boolean'],
             'default': None},
            SQLColumn(self.col_name, redshift_types.RedshiftBoolean()),
        )

    def test_convert_with_field_enum(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': {
                 'type': 'enum',
                 'name': self.col_name,
                 'symbols': ['1', '123', '12']}
             },
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftVarChar(3),
                is_nullable=False
            ),
        )

    def test_convert_with_unsupported_type(self, converter):
        with pytest.raises(UnsupportedTypeException):
            record_schema = self.compose_record_schema(
                {'name': self.col_name,
                 'type': {
                     'type': 'array',
                     'items': {
                         'type': 'map',
                         'values': 'string'
                     }
                 }}
            )
            converter.convert(record_schema)

    def test_convert_with_field_null(self, converter):
        with pytest.raises(SchemaConversionException):
            record_schema = self.compose_record_schema(
                {'name': self.col_name, 'type': 'null', 'default': None}
            )
            converter.convert(record_schema)

    def test_convert_with_primary_key_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'int'],
             'default': None,
             AvroMetaDataKeys.PRIMARY_KEY: True},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                primary_key_order=1
            ),
        )

    def test_convert_with_composite_primary_keys(self, converter):
        record_schema = {
            'type': 'record',
            'name': self.avro_schema_name,
            'namespace': None,
            'fields': [
                {
                    'name': self.col_name,
                    'type': 'int',
                    AvroMetaDataKeys.PRIMARY_KEY: 2
                },
                {
                    'name': 'bar',
                    'type': 'int',
                    AvroMetaDataKeys.PRIMARY_KEY: 1
                }
            ],
            'doc': 'sample doc',
        }
        expected_column_col = SQLColumn(
            self.col_name,
            redshift_types.RedshiftInteger(),
            is_nullable=False,
            primary_key_order=2
        )
        expected_column_bar = SQLColumn(
            'bar',
            redshift_types.RedshiftInteger(),
            is_nullable=False,
            primary_key_order=1
        )
        expected_table = SQLTable(
            self.avro_schema_name,
            columns=[expected_column_col, expected_column_bar],
            doc=record_schema.get('doc')
        )
        actual_table = converter.convert(record_schema)
        assert expected_table == actual_table

    def test_convert_with_non_nullable_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name, 'type': 'int', 'default': 10},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                is_nullable=False,
                default_value=10
            ),
        )

    def test_convert_with_column_default_value(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name, 'type': ['int', 'null'], 'default': 10},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                default_value=10
            )
        )

    def test_convert_with_unsigned_int_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'int'],
             'default': None,
             'unsigned': True},
            SQLColumn(self.col_name, redshift_types.RedshiftInteger())
        )

    def test_convert_with_non_nullable_without_default_column(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name, 'type': 'int'},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                is_nullable=False
            )
        )

    def test_convert_with_column_with_alias(self, converter):
        self._convert_and_assert_with_one_column(
            converter,
            {'name': self.col_name, 'type': 'int', 'aliases': ['abc']},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                is_nullable=False,
                **{MetaDataKey.ALIASES: ['abc']}
            )
        )

    def test_convert_with_no_table_metadata(self, converter):
        record_schema = {
            'type': 'record',
            'name': self.avro_schema_name,
            'namespace': None,
            'fields': [{'name': self.col_name, 'type': 'int'}],
            'doc': 'sample doc',
        }
        expected_column = SQLColumn(
            self.col_name,
            redshift_types.RedshiftInteger(),
            is_nullable=False
        )
        expected_table = SQLTable(
            self.avro_schema_name,
            columns=[expected_column],
            doc=record_schema.get('doc')
        )
        actual_table = converter.convert(record_schema)
        assert expected_table == actual_table

    def test_convert_with_none_record_schema(self, converter):
        actual_schema = converter.convert(None)
        assert actual_schema is None

    def test_convert_with_invalid_avro_record_schema(self, converter):
        with pytest.raises(SchemaConversionException):
            converter.convert('int')

        with pytest.raises(SchemaConversionException):
            converter.convert('foo')
