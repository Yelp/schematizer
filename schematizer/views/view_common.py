# -*- coding: utf-8 -*-
from schematizer import models
from schematizer.api.exceptions import exceptions_v1
from schematizer.components import mysql_handlers
from schematizer.components.converters import converter_base


def convert_to_avro_from_mysql(mysql_statements, schema_repo):
    try:
        sql_table = mysql_handlers.create_sql_table_from_mysql_stmts(
            mysql_statements
        )
        return schema_repo.convert_schema(
            models.SchemaKindEnum.MySQL,
            models.SchemaKindEnum.Avro,
            sql_table
        )
    except (mysql_handlers.MySQLHandlerException,
            converter_base.SchemaConversionException) as e:
        raise exceptions_v1.invalid_schema_exception(e.message)
