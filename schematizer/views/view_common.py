# -*- coding: utf-8 -*-
from schematizer import models
from schematizer.api.exceptions import exceptions_v1
from schematizer.components.converters import converter_base
from schematizer.components.handlers import sql_handler
from schematizer.components.handlers import sql_handler_base


def convert_to_avro_from_mysql(mysql_statements, schema_repo):
    try:
        sql_table = sql_handler.create_sql_table_from_sql_stmts(
            mysql_statements,
            sql_handler_base.SQLDialect.MySQL
        )
        return schema_repo.convert_schema(
            models.SchemaKindEnum.MySQL,
            models.SchemaKindEnum.Avro,
            sql_table
        )
    except (sql_handler_base.SQLHandlerException,
            converter_base.SchemaConversionException) as e:
        raise exceptions_v1.invalid_schema_exception(e.message)
