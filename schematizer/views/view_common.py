# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer import models
from schematizer.api.exceptions import exceptions_v1
from schematizer.components.converters import converter_base
from schematizer.components.handlers import sql_handler
from schematizer.components.handlers import sql_handler_base
from schematizer.config import log
from schematizer.utils.utils import get_current_func_arg_name_values


def convert_to_avro_from_mysql(
    schema_repo,
    new_create_table_stmt,
    old_create_table_stmt=None,
    alter_table_stmt=None
):
    if bool(old_create_table_stmt) ^ bool(alter_table_stmt):
        raise exceptions_v1.invalid_request_exception(
            'Both old_create_table_stmt and alter_table_stmt must be provided.'
        )

    try:
        mysql_statements = [
            old_create_table_stmt, alter_table_stmt, new_create_table_stmt
        ]
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
        log.exception('{0}'.format(get_current_func_arg_name_values()))
        raise exceptions_v1.invalid_schema_exception(e.message)
