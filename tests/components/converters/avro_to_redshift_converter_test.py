# -*- coding: utf-8 -*-
import pytest

from schematizer.components.converters import AvroToRedshiftConverter
from schematizer.components.converters.converter_base \
    import AvroMetaDataKeyEnum
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
    def schema_name(self):
        return 'foo'

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

    def convert_with_one_column(self, converter, avro_field, expected_column):
        record_schema = self.compose_record_schema(avro_field)
        expected_table = SQLTable(
            self.schema_name,
            columns=[expected_column],
            doc=record_schema.get('doc'),
            **self.get_table_metadata()
        )
        actual_table = converter.convert(record_schema)
        assert expected_table == actual_table

    def compose_record_schema(self, avro_field):
        return {
            'type': 'record',
            'name': self.schema_name,
            'namespace': self.namespace,
            'fields': [avro_field],
            'doc': 'sample doc',
            'aliases': self.table_aliases,
        }

    def get_table_metadata(self):
        return {
            MetaDataKey.NAMESPACE: self.namespace,
            MetaDataKey.ALIASES: self.table_aliases
        }

    def test_convert_with_field_int(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name, 'type': ['null', 'int'], 'default': None},
            SQLColumn(self.col_name, redshift_types.RedshiftInteger())
        )

    def test_convert_with_field_long(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name, 'type': ['null', 'long'], 'default': None},
            SQLColumn(self.col_name, redshift_types.RedshiftBigInt())
        )

    def test_convert_with_field_double(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'double'],
             'default': None,
             AvroMetaDataKeyEnum.PRECISION: 10,
             AvroMetaDataKeyEnum.SCALE: 2},
            SQLColumn(self.col_name, redshift_types.RedshiftDouble()),
        )

    def test_convert_with_field_double_with_fixed_flag(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'double'],
             'default': None,
             AvroMetaDataKeyEnum.PRECISION: 8,
             AvroMetaDataKeyEnum.SCALE: 0,
             AvroMetaDataKeyEnum.FIXED_POINT: True},
            SQLColumn(self.col_name, redshift_types.RedshiftDecimal(8, 0))
        )

    def test_convert_with_field_float(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'float'],
             'default': None,
             AvroMetaDataKeyEnum.PRECISION: 10,
             AvroMetaDataKeyEnum.SCALE: 2},
            SQLColumn(self.col_name, redshift_types.RedshiftReal())
        )

    def test_convert_with_field_string_with_fixed_len(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeyEnum.FIX_LEN: 16},
            SQLColumn(self.col_name, redshift_types.RedshiftChar(16))
        )

    def test_convert_with_field_string_with_max_len(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'string'],
             'default': None,
             AvroMetaDataKeyEnum.MAX_LEN: 16},
            SQLColumn(self.col_name, redshift_types.RedshiftVarChar(32))
        )

    def test_convert_with_field_string_without_specified_len(self, converter):
        with pytest.raises(SchemaConversionException):
            record_schema = self.compose_record_schema(
                {'name': self.col_name, 'type': 'string', 'default': ''}
            )
            converter.convert(record_schema)

    def test_convert_with_field_timestamp(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'long'],
             'default': None,
             AvroMetaDataKeyEnum.TIMESTAMP: True},
            SQLColumn(self.col_name, redshift_types.RedshiftTimestamp())
        )

    def test_convert_with_field_boolean(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'boolean'],
             'default': None},
            SQLColumn(self.col_name, redshift_types.RedshiftBoolean()),
        )

    def test_convert_with_field_null(self, converter):
        with pytest.raises(SchemaConversionException):
            record_schema = self.compose_record_schema(
                {'name': self.col_name, 'type': 'null', 'default': None}
            )
            converter.convert(record_schema)

    def test_convert_with_unsupported_type(self, converter):
        with pytest.raises(UnsupportedTypeException):
            record_schema = self.compose_record_schema(
                {'name': self.col_name,
                 'type': ['null', 'bytes'],
                 'default': None}
            )
            converter.convert(record_schema)

    def test_convert_with_primary_key_column(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'int'],
             'default': None,
             AvroMetaDataKeyEnum.PRIMARY_KEY: True},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                is_primary_key=True
            ),
        )

    def test_convert_with_non_nullable_column(self, converter):
        self.convert_with_one_column(
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
        self.convert_with_one_column(
            converter,
            {'name': self.col_name, 'type': ['int', 'null'], 'default': 10},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                default_value=10
            )
        )

    def test_convert_with_unsigned_int_column(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name,
             'type': ['null', 'int'],
             'default': None,
             'unsigned': True},
            SQLColumn(self.col_name, redshift_types.RedshiftInteger())
        )

    def test_convert_with_non_nullable_without_default_column(self, converter):
        self.convert_with_one_column(
            converter,
            {'name': self.col_name, 'type': 'int'},
            SQLColumn(
                self.col_name,
                redshift_types.RedshiftInteger(),
                is_nullable=False
            )
        )

    def test_convert_with_column_with_alias(self, converter):
        self.convert_with_one_column(
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
            'name': self.schema_name,
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
            self.schema_name,
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
