# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import inspect
import sys

import sqlparse
from sqlparse import sql
from sqlparse import tokens as T

from schematizer.components.handlers.sql_handler_base import SQLDialect
from schematizer.components.handlers.sql_handler_base import SQLHandlerBase
from schematizer.components.handlers.sql_handler_base import \
    SQLHandlerException
from schematizer.config import log
from schematizer.models import mysql_data_types as data_types
from schematizer.models import sql_entities


class ParsedMySQLProcessor(object):
    """This class contains the utility functions to construct SQLTable
    from parsed MySQL table statements (create or alter).
    """

    def __init__(self):
        _mysql_types = inspect.getmembers(
            sys.modules['schematizer.models.mysql_data_types'],
            inspect.isclass
        )
        self._mysql_type_to_class_map = dict(
            (typ.type_name, typ) for _, typ in _mysql_types
            if issubclass(typ, sql_entities.SQLColumnDataType)
        )

    def create_sql_table_from_create_table_stmt(self, parsed_stmt):
        """Constructs the SQLTable from the given create-table statement
        (parsed mysql statement). If the given sql statement is not a
        create-table statement, it returns None.
        """
        if not self.is_create_table_sql(parsed_stmt):
            raise ValueError("parsed_stmt should be a create-table statement. "
                             "Value: {0}".format(parsed_stmt))
        table = sql_entities.SQLTable(self._get_table_name(parsed_stmt))
        table.columns.extend(self._get_columns(parsed_stmt))
        return table

    def is_create_table_sql(self, parsed_sql):
        if parsed_sql.get_type() != 'CREATE':
            return False

        token = parsed_sql.token_next_by_type(1, T.Keyword)
        if token.normalized not in ('TABLE', 'TEMPORARY'):
            return False

        if token.normalized == 'TABLE':
            return True

        index = parsed_sql.token_index(token)
        token = parsed_sql.token_next_by_type(index + 1, T.Keyword)
        return token.normalized == 'TABLE'

    def _get_table_name(self, stmt):
        return stmt.token_next_by_instance(0, sql.TableName).value

    def _get_columns(self, stmt):
        columns = []
        for token in self._get_create_definition_tokens(stmt):
            if isinstance(token, sql.ColumnsDefinition):
                columns.extend(self._construct_column(t) for t in token.tokens)
            else:
                self._process_other_create_definition(token, columns)
        return columns

    def _get_create_definition_tokens(self, stmt):
        lparen_token = stmt.token_next_by_type(0, T.Punctuation)
        if not lparen_token or lparen_token.value != '(':
            yield

        index = stmt.token_index(lparen_token)
        def_tokens = []
        for token in stmt.tokens[index + 1:]:
            if token.value == ')':
                break

            if isinstance(token, sql.ColumnsDefinition):
                yield token
            elif token.match(T.Punctuation, ','):
                yield def_tokens
                def_tokens = []
            elif not token.is_whitespace():
                def_tokens.append(token)

        if def_tokens:
            yield def_tokens

    def _construct_column(self, col_token):
        name_token = col_token.token_next_by_instance(0, sql.ColumnName)
        column_type = self._get_column_type(col_token)
        return sql_entities.SQLColumn(
            column_name=name_token.value,
            column_type=column_type,
            is_nullable=self._is_column_nullable(col_token),
            default_value=self._get_default_value(col_token, column_type),
            attributes=None,
            doc=None
        )

    def _get_column_type(self, col_token):
        type_name = col_token.token_next_by_instance(0, sql.ColumnType).value
        typ = self._mysql_type_to_class_map.get(type_name.lower())
        if not typ:
            raise SQLHandlerException(
                "Unknown MySQL column type {0}.".format(type_name)
            )

        for create_func in self._create_type_funcs:
            col_type = create_func(typ, col_token)
            if col_type:
                return col_type

        raise SQLHandlerException(
            "Unable to create MySQL column type {0}".format(type_name)
        )

    @property
    def _create_type_funcs(self):
        return [
            self._create_bit_type,
            self._create_integer_type,
            self._create_boolean_type,
            self._create_real_number_type,
            self._create_string_type,
            self._create_binary_type,
            self._create_enum_type,
            self._create_set_type,
            self._create_date_type,
            self._create_year_type,
            self._create_time_type,
            self._create_timestamp_type,
            self._create_datetime_type
        ]

    def _create_bit_type(self, col_type_cls, col_token):
        if not issubclass(col_type_cls, data_types.MySQLBit):
            return None

        length = None
        len_token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        if len_token:
            token = len_token.token_next_by_type(0, T.Number.Integer)
            length = token.value

        return col_type_cls(length)

    def _create_integer_type(self, col_type_cls, col_token):
        if not issubclass(col_type_cls, data_types.MySQLIntegerType):
            return None

        length = None
        len_token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        if len_token:
            token = len_token.token_next_by_type(0, T.Number.Integer)
            length = token.value

        attributes = col_token.token_next_by_instance(0, sql.ColumnAttributes)
        attr_token = self._get_attribute_token('unsigned', attributes)
        is_unsigned = attr_token is not None

        return col_type_cls(length, unsigned=is_unsigned)

    def _create_boolean_type(self, col_type_cls, col_token):
        if not issubclass(col_type_cls, data_types.MySQLBool):
            return None

        return col_type_cls()

    def _create_real_number_type(self, col_type_cls, col_token):
        if not issubclass(col_type_cls, data_types.MySQLRealNumber):
            return None

        # The floating-point types have optional precision and scale settings,
        # i.e., double, float(10), double(10, 2) are all valid column types.
        # The fixed-point types have optional scale setting, i.e. decimal(10),
        # numeric(10, 2) are valid column types.
        # If the precision and/or scale is not specified, None is used to
        # indicate no specific value is provided, and let the users of this
        # object to decide what to do with it.
        precision, scale = None, None
        len_token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        if len_token:
            token = len_token.token_next_by_type(0, T.Number.Integer)
            precision = token.value

            index = len_token.token_index(token)
            token = len_token.token_next_by_type(index + 1, T.Number.Integer)
            scale = token.value if token else None

        attributes = col_token.token_next_by_instance(0, sql.ColumnAttributes)
        attr_token = self._get_attribute_token('unsigned', attributes)
        is_unsigned = attr_token is not None

        return col_type_cls(precision, scale, unsigned=is_unsigned)

    def _create_string_type(self, col_type_cls, col_token):
        if not issubclass(col_type_cls, data_types.MySQLString):
            return None

        attributes = col_token.token_next_by_instance(0, sql.ColumnAttributes)
        is_binary = self._get_attribute_token('binary', attributes) is not None
        collate = self._get_attribute_value('collate', attributes)
        char_set = self._get_char_set_value(attributes)

        if col_type_cls in (data_types.MySQLChar, data_types.MySQLVarChar):
            token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
            token = token.token_next_by_type(0, T.Number.Integer)
            return col_type_cls(
                token.value,
                binary=is_binary,
                char_set=char_set,
                collate=collate
            )
        return col_type_cls(
            binary=is_binary,
            char_set=char_set,
            collate=collate
        )

    def _create_binary_type(self, col_type_cls, col_token):
        if not issubclass(col_type_cls, data_types.MySQLBinaryBase):
            return None

        if col_type_cls in (data_types.MySQLBinary, data_types.MySQLVarBinary):
            token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
            token = token.token_next_by_type(0, T.Number.Integer)
            return col_type_cls(token.value)
        return col_type_cls()

    def _create_date_type(self, col_type_cls, col_token):
        if col_type_cls is not data_types.MySQLDate:
            return None

        return col_type_cls()

    def _create_year_type(self, col_type_cls, col_token):
        if col_type_cls is not data_types.MySQLYear:
            return None

        return col_type_cls()

    def _create_time_type(self, col_type_cls, col_token):
        if col_type_cls is not data_types.MySQLTime:
            return None

        fsp = 0
        len_token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        if len_token:
            token = len_token.token_next_by_type(0, T.Number.Integer)
            fsp = token.value if token else 0

        return col_type_cls(fsp)

    def _create_timestamp_type(self, col_type_cls, col_token):
        if col_type_cls is not data_types.MySQLTimestamp:
            return None

        fsp = 0
        len_token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        if len_token:
            token = len_token.token_next_by_type(0, T.Number.Integer)
            fsp = token.value if token else 0

        return col_type_cls(fsp)

    def _create_datetime_type(self, col_type_cls, col_token):
        if col_type_cls is not data_types.MySQLDateTime:
            return None

        fsp = 0
        len_token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        if len_token:
            token = len_token.token_next_by_type(0, T.Number.Integer)
            fsp = token.value if token else 0

        return col_type_cls(fsp)

    def _create_enum_type(self, col_type_cls, col_token):
        if col_type_cls is not data_types.MySQLEnum:
            return None

        token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        values = [t.value for t in token.tokens if t.ttype != T.Punctuation]
        return col_type_cls(values)

    def _create_set_type(self, col_type_cls, col_token):
        if col_type_cls is not data_types.MySQLSet:
            return None

        len_token = col_token.token_next_by_instance(0, sql.ColumnTypeLength)
        values = [token.value for token in len_token.tokens
                  if token.ttype != T.Punctuation]
        return col_type_cls(values)

    def _get_attribute_token(self, attribute_name, attributes):
        return next((attr for attr in attributes.tokens
                     if isinstance(attr, sql.Attribute) and
                     attr.tokens[0].normalized == attribute_name.upper()),
                    None)

    def _get_attribute_value(self, attribute_name, attributes):
        attr = self._get_attribute_token(attribute_name, attributes)
        return attr.tokens[1].value if attr else None

    def _get_default_value(self, col_token, column_type):
        attributes = col_token.token_next_by_instance(0, sql.ColumnAttributes)
        attr_value = self._get_attribute_value('default', attributes)
        return column_type.convert_str_to_type_val(attr_value)

    def _is_column_nullable(self, col_token):
        attributes = col_token.token_next_by_instance(0, sql.ColumnAttributes)
        return self._get_attribute_token('not null', attributes) is None

    def _get_char_set_value(self, attributes):
        """Currently the `character set` is not grouped as Attribute, so it
        is processed separately"""
        token = attributes.token_next_match(0, T.Name.Builtin, 'CHARACTER')
        if not token:
            return None

        index = attributes.token_index(token)
        token = attributes.token_next(index)
        if not token or token.value != u'SET':
            return None

        index = attributes.token_index(token)
        token = attributes.token_next(index)
        return token.value if token.ttype == T.Name else None

    def _process_other_create_definition(self, def_tokens, columns):
        primary_keys = self._get_primary_key(def_tokens)
        self._set_column_primary_keys(primary_keys, columns)

    def _get_primary_key(self, def_tokens):
        EXPECT_PRIMARY = 0
        EXPECT_KEY = 1
        EXPECT_COLUMN = 2
        state = EXPECT_PRIMARY
        for token in def_tokens:
            if state == EXPECT_PRIMARY and token.match(T.Keyword, 'PRIMARY'):
                state = EXPECT_KEY
            elif state == EXPECT_KEY and token.value.upper() == 'KEY':
                state = EXPECT_COLUMN
            elif state == EXPECT_COLUMN and isinstance(token, sql.Parenthesis):
                return [t.value for t in token.tokens[1:-1]
                        if t.ttype == T.Name]
        return []

    def _set_column_primary_keys(self, primary_keys, columns):
        for idx, primary_key in enumerate(primary_keys, 1):
            for col in columns:
                if primary_key == col.name:
                    col.primary_key_order = idx
                    break


class MySQLHandler(SQLHandlerBase):

    dialect = SQLDialect.MySQL

    def __init__(self):
        super(MySQLHandler, self).__init__()
        self.processor = ParsedMySQLProcessor()

    def _parse(self, sql):
        return sqlparse.parse(sql, dialect='mysql')[0] if sql else None

    def _create_sql_table(self, parsed_sqls):
        last_stmt = parsed_sqls[-1] if parsed_sqls else None
        if last_stmt:
            return self.processor.create_sql_table_from_create_table_stmt(
                last_stmt
            )
        raise SQLHandlerException(
            "Unable to process MySQL statements {0}.".format(parsed_sqls)
        )


class LoggingMySQLHandler(MySQLHandler):

    def _parse(self, sql):
        try:
            return super(LoggingMySQLHandler, self)._parse(sql)
        except:
            log.exception("Parsing MySQL error: {}".format(sql))
            raise
