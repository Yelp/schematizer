# -*- coding: utf-8 -*-
from schematizer.avro_builder import AvroSchemaBuilder
from schematizer.converter_base import AvroConvertible
from schematizer.converter_base import MetaDataKeyEnum
from schematizer.converter_base import SchemaConversionException
from schematizer.converter_base import UnsupportedTypeException
from schematizer.sql_entities import MetaDataKey
from schematizer.sql_entities import SQLTable


class MySqlConverter(AvroConvertible):
    """Converter that converts MySQL table schema to other type of schemas.

    Currently it implements AvroConvertible, which supports converting from
    Internal SQLTable schema to Avro schema.
    """

    def create_avro_schema(self, src_schema):
        """Convert MySql table definition to Avro schema. The src_schema
        is the SQLTable object that represents a MySQL table.
        """
        if not src_schema:
            return None
        if not isinstance(src_schema, SQLTable):
            raise SchemaConversionException('SQLTable is expected.')

        table = src_schema
        namespace = table.metadata.get(MetaDataKey.NAMESPACE, '')
        aliases = table.metadata.get(MetaDataKey.ALIASES)

        builder = AvroSchemaBuilder()
        fields = [self.create_avro_field(builder, col, namespace)
                  for col in table.columns]
        record_json = builder.create_record(
            table.name,
            fields,
            namespace=namespace,
            aliases=aliases,
            doc=table.doc
        )
        return record_json

    def create_avro_field(self, schema_builder, column, namespace=None):
        schema_type = self.create_avro_field_type(
            schema_builder,
            column,
            namespace
        )
        field_type = (schema_builder.create_optional_type(schema_type)
                      if column.is_nullable else schema_type)
        column_metadata = self.build_avro_field_metadata(column)
        return schema_builder.create_field(
            column.name,
            field_type,
            has_default=True,
            default_value=column.default_value,
            aliases=column.metadata.get(MetaDataKey.ALIASES),
            doc=column.doc,
            **column_metadata
        )

    def create_avro_field_type(self, schema_builder, column, namespace=None):
        data_type = column.type
        type_name = data_type.type_name
        if type_name in ('char', 'varchar', 'text'):
            return schema_builder.create_string()

        if type_name in ('bigint', 'timestamp'):
            # Avro currently doesn't support timestamp, so map the
            # timestamp sql column type to long (unix timestamp)
            return schema_builder.create_long()

        if type_name in ('int', 'integer', 'tinyint', 'smallint', 'mediumint'):
            return schema_builder.create_int()

        if type_name in ('float',):
            return schema_builder.create_float()

        if type_name in ('double', 'real', 'decimal', 'numeric'):
            return schema_builder.create_double()

        if type_name in ('enum',):
            return schema_builder.create_enum(
                self.get_enum_type_name(column),
                data_type.values,
                namespace
            )

        if type_name in ('binary',):
            return schema_builder.create_fixed(
                self.get_fixed_type_name(column),
                data_type.length,
                namespace
            )

        raise UnsupportedTypeException(
            'Unable to convert sql data type {0} to Avro schema type.'.format(
                data_type
            )
        )

    @classmethod
    def get_enum_type_name(cls, column):
        return column.name + '_enum'

    @classmethod
    def get_fixed_type_name(cls, column):
        return column.name + '_fixed'

    def build_avro_field_metadata(self, column):
        metadata = {}
        if column.is_primary_key:
            metadata[MetaDataKeyEnum.PRIMARY_KEY] = True

        data_type = column.type
        if data_type.type_name in ('varchar',):
            metadata[MetaDataKeyEnum.MAX_LEN] = data_type.length

        if data_type.type_name in ('char',):
            metadata[MetaDataKeyEnum.FIX_LEN] = data_type.length

        if data_type.type_name in ('float', 'double', 'real', 'decimal',
                                   'numeric'):
            if data_type.length is not None:
                metadata[MetaDataKeyEnum.LENGTH] = data_type.length
            if data_type.decimal is not None:
                metadata[MetaDataKeyEnum.DECIMAL] = data_type.decimal

        if data_type.type_name in ('timestamp',):
            metadata[MetaDataKeyEnum.TIMESTAMP] = True

        if column.get_attribute('unsigned'):
            metadata[MetaDataKeyEnum.UNSIGNED] = True

        return metadata
