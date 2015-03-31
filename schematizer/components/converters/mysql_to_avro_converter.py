# -*- coding: utf-8 -*-
from schematizer.avro_builder import AvroSchemaBuilder
from schematizer.components.converters.converter_base import BaseConverter
from schematizer.components.converters.converter_base import MetaDataKeyEnum
from schematizer.components.converters.converter_base \
    import SchemaConversionException
from schematizer.components.converters.converter_base \
    import UnsupportedTypeException
from schematizer.models import mysql_data_types
from schematizer.models import SchemaKindEnum
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

        self._builder.begin()
        record_json = self._builder.create_record(
            table.name,
            [self._create_avro_field(col) for col in table.columns],
            namespace=namespace,
            aliases=aliases,
            doc=table.doc
        )
        self._builder.end()
        return record_json

    def _create_avro_field(self, column):
        if not column.is_nullable and column.default_value is None:
            raise SchemaConversionException(
                "Column {0} is not nullable and must have a non-null "
                "default value.".format(column.name)
            )

        field_type, field_metadata = self._create_avro_field_type(column)
        if column.is_nullable:
            field_type = self._builder.create_optional_type(
                field_type,
                column.default_value
            )

        return self._builder.create_field(
            column.name,
            field_type,
            has_default=True,
            default_value=column.default_value,
            aliases=column.metadata.get(MetaDataKey.ALIASES),
            doc=column.doc,
            **field_metadata
        )

    def _create_avro_field_type(self, column):
        type_cls = column.type.__class__
        converter = (self._type_converters.get(type_cls)
                     or self._type_converters.get(type_cls.__bases__[0]))
        if converter:
            return converter(column)

        raise UnsupportedTypeException(
            "Unable to convert MySQL data type {0} to Avro schema type."
            .format(column.type)
        )

    @property
    def _type_converters(self):
        return {
            mysql_data_types.MySQLInteger: self._convert_integer_type,
            mysql_data_types.MySQLBigInt: self._convert_bigint_type,
            mysql_data_types.MySQLRealNumber: self._convert_real_number_type,
            mysql_data_types.MySQLFloat: self._convert_float_type,
            mysql_data_types.MySQLString: self._convert_string_type,
            mysql_data_types.MySQLChar: self._convert_char_type,
            mysql_data_types.MySQLVarChar: self._convert_varchar_type,
            mysql_data_types.MySQLTimestamp: self._convert_timestamp_type,
            mysql_data_types.MySQLEnum: self._convert_enum_type,
        }

    def _convert_integer_type(self, column):
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_int(), metadata

    def _get_primary_key_metadata(self, is_primary_key):
        return {MetaDataKeyEnum.PRIMARY_KEY: True} if is_primary_key else {}

    def _get_unsigned_metadata(self, is_unsigned):
        return {MetaDataKeyEnum.UNSIGNED: is_unsigned} if is_unsigned else {}

    def _convert_bigint_type(self, column):
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_long(), metadata

    def _convert_real_number_type(self, column):
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        metadata.update(self._get_precision_metadata(column))
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_double(), metadata

    def _get_precision_metadata(self, column):
        return {
            MetaDataKeyEnum.LENGTH: column.type.length,
            MetaDataKeyEnum.DECIMAL: column.type.decimal,
        }

    def _convert_float_type(self, column):
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        metadata.update(self._get_precision_metadata(column))
        metadata.update(self._get_unsigned_metadata(column.type.is_unsigned))
        return self._builder.create_float(), metadata

    def _convert_string_type(self, column):
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        return self._builder.create_string(), metadata

    def _convert_char_type(self, column):
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        metadata[MetaDataKeyEnum.FIX_LEN] = column.type.length
        return self._builder.create_string(), metadata

    def _convert_varchar_type(self, column):
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        metadata[MetaDataKeyEnum.MAX_LEN] = column.type.length
        return self._builder.create_string(), metadata

    def _convert_timestamp_type(self, column):
        """Avro currently doesn't support timestamp, so map the
        timestamp sql column type to long (unix timestamp)
        """
        metadata = self._get_primary_key_metadata(column.is_primary_key)
        metadata[MetaDataKeyEnum.TIMESTAMP] = True
        return self._builder.create_long(), metadata

    def _convert_enum_type(self, column):
        return self._builder.create_enum(
            self.get_enum_type_name(column),
            column.type.values
        ), {}

    @classmethod
    def get_enum_type_name(cls, column):
        return column.name + '_enum'
