# -*- coding: utf-8 -*-
from yelp_avro.avro_builder import AvroSchemaBuilder
from yelp_avro.data_pipeline.avro_meta_data import AvroMetaDataKeyEnum

from schematizer.components.converters.converter_base import BaseConverter
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import SchemaKindEnum
from schematizer.models import mysql_data_types as mysql_types
from schematizer.models.sql_entities import MetaDataKey
from schematizer.models.sql_entities import SQLTable


class MySQLToAvroConverter(BaseConverter):
    """Converter that converts MySQL table schema to Avro schema.
    """

    source_type = SchemaKindEnum.MySQL
    target_type = SchemaKindEnum.Avro

    def __init__(self):
        self._builder = AvroSchemaBuilder()

    def convert(self, src_schema):
        """The src_schema is the SQLTable object that represents a MySQL table.
        It returns the Avro schema json object.
        """
        if not src_schema:
            return None
        if not isinstance(src_schema, SQLTable):
            raise SchemaConversionException('SQLTable is expected.')

        return self._create_avro_record_json(src_schema)

    def _create_avro_record_json(self, table):
        namespace = table.metadata.get(MetaDataKey.NAMESPACE, '')
        aliases = table.metadata.get(MetaDataKey.ALIASES)
        metadata = self._get_table_metadata(table)

        self._builder.begin_record(
            table.name,
            namespace=namespace,
            aliases=aliases,
            doc=table.doc,
            **metadata
        )
        for column in table.columns:
            self._create_avro_field(column)
        record_json = self._builder.end()
        return record_json

    def _get_table_metadata(self, table):
        metadata = {}

        primary_keys = [c.name for c in table.primary_keys]
        if primary_keys:
            metadata[AvroMetaDataKeyEnum.PRIMARY_KEY] = primary_keys

        return metadata

    def _create_avro_field(self, column):
        field_type, field_metadata = self._create_avro_field_type(column)
        if column.is_nullable:
            field_type = self._builder.begin_nullable_type(
                field_type,
                column.default_value
            ).end()

        self._builder.add_field(
            column.name,
            field_type,
            has_default=column.default_value is not None or column.is_nullable,
            default_value=column.default_value,
            aliases=column.metadata.get(MetaDataKey.ALIASES),
            doc=column.doc,
            **field_metadata
        )

    def _create_avro_field_type(self, column):
        type_cls = column.type.__class__
        convert_func = self._type_converters.get(type_cls)
        if convert_func:
            return convert_func(column)

        raise UnsupportedTypeException(
            "Unable to convert MySQL data type {0} to Avro schema type."
            .format(column.type)
        )

    @property
    def _type_converters(self):
        return {
            mysql_types.MySQLTinyInt: self._convert_integer_type,
            mysql_types.MySQLSmallInt: self._convert_integer_type,
            mysql_types.MySQLMediumInt: self._convert_integer_type,
            mysql_types.MySQLInt: self._convert_integer_type,
            mysql_types.MySQLInteger: self._convert_integer_type,
            mysql_types.MySQLBigInt: self._convert_bigint_type,

            mysql_types.MySQLBit: self._convert_bit_type,

            mysql_types.MySQLBool: self._convert_boolean_type,
            mysql_types.MySQLBoolean: self._convert_boolean_type,

            mysql_types.MySQLFloat: self._convert_float_type,
            mysql_types.MySQLDouble: self._convert_double_type,
            mysql_types.MySQLReal: self._convert_double_type,
            mysql_types.MySQLDecimal: self._convert_decimal_type,
            mysql_types.MySQLNumeric: self._convert_decimal_type,

            mysql_types.MySQLChar: self._convert_char_type,
            mysql_types.MySQLVarChar: self._convert_varchar_type,
            mysql_types.MySQLTinyText: self._convert_string_type,
            mysql_types.MySQLText: self._convert_string_type,
            mysql_types.MySQLMediumText: self._convert_string_type,
            mysql_types.MySQLLongText: self._convert_string_type,

            mysql_types.MySQLDate: self._convert_date_type,
            mysql_types.MySQLDateTime: self._convert_datetime_type,
            mysql_types.MySQLTime: self._convert_time_type,
            mysql_types.MySQLYear: self._convert_year_type,
            mysql_types.MySQLTimestamp: self._convert_timestamp_type,
            mysql_types.MySQLEnum: self._convert_enum_type,
        }

    def _convert_integer_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_int(), metadata

    def _get_primary_key_metadata(self, primary_key_order):
        return ({AvroMetaDataKeyEnum.PRIMARY_KEY: primary_key_order}
                if primary_key_order else {})

    def _get_unsigned_metadata(self, is_unsigned):
        return ({AvroMetaDataKeyEnum.UNSIGNED: is_unsigned}
                if is_unsigned else {})

    def _convert_bigint_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_long(), metadata

    def _convert_bit_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_bitlen_metadata(column.type.length))
        return self._builder.create_int(), metadata

    def _get_bitlen_metadata(self, bit_length):
        return ({AvroMetaDataKeyEnum.BIT_LEN: bit_length}
                if bit_length else {})

    def _convert_boolean_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        return self._builder.create_boolean(), metadata

    def _convert_double_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_precision_metadata(column))
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_double(), metadata

    def _get_precision_metadata(self, column):
        return {
            AvroMetaDataKeyEnum.PRECISION: column.type.precision,
            AvroMetaDataKeyEnum.SCALE: column.type.scale,
        }

    def _convert_float_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_precision_metadata(column))
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_float(), metadata

    def _convert_decimal_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata.update(self._get_precision_metadata(column))
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        metadata.update({AvroMetaDataKeyEnum.FIXED_POINT: True})
        return self._builder.create_double(), metadata

    def _convert_string_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        return self._builder.create_string(), metadata

    def _convert_char_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeyEnum.FIX_LEN] = column.type.length
        return self._builder.create_string(), metadata

    def _convert_varchar_type(self, column):
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeyEnum.MAX_LEN] = column.type.length
        return self._builder.create_string(), metadata

    def _convert_date_type(self, column):
        """Avro currently doesn't support date, so map the
        date sql column type to string (ISO 8601 format)
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeyEnum.DATE] = True
        return self._builder.create_string(), metadata

    def _convert_datetime_type(self, column):
        """Avro currently doesn't support datetime, so map the
        datetime sql column type to string (ISO 8601 format)
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeyEnum.DATETIME] = True
        return self._builder.create_string(), metadata

    def _convert_time_type(self, column):
        """Avro currently doesn't support time, so map the
        time sql column type to string (ISO 8601 format)
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeyEnum.TIME] = True
        return self._builder.create_string(), metadata

    def _convert_year_type(self, column):
        """Avro currently doesn't support year, so map the
        year sql column type to long (year number)
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeyEnum.YEAR] = True
        return self._builder.create_long(), metadata

    def _convert_timestamp_type(self, column):
        """Avro currently doesn't support timestamp, so map the
        timestamp sql column type to long (unix timestamp)
        """
        metadata = self._get_primary_key_metadata(column.primary_key_order)
        metadata[AvroMetaDataKeyEnum.TIMESTAMP] = True
        return self._builder.create_long(), metadata

    def _convert_enum_type(self, column):
        return self._builder.begin_enum(
            self.get_enum_type_name(column),
            column.type.values
        ).end(), {}

    @classmethod
    def get_enum_type_name(cls, column):
        return column.name + '_enum'
