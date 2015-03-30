# -*- coding: utf-8 -*-
from schematizer.components.converters.converter_base import BaseConverter
from schematizer.models.enums import SchemaKindEnum


class MySqlConverter(BaseConverter):

    source_type = SchemaKindEnum.MySQL
    target_type = SchemaKindEnum.Avro

    def convert(self, src_schema):
        pass
