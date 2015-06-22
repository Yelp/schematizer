# -*- coding: utf-8 -*-
import abc


class SQLDialect(object):

    SQL = 'sql'
    MySQL = 'mysql'


class SQLHandlerBase(object):

    __metaclass__ = abc.ABCMeta

    dialect = None

    def create_sql_table_from_sql_stmts(self, sqls):
        """Parse and process the raw table related SQL statements of specific
        SQL dialect, and generate the corresponding SQLTable object.
        """
        parsed_sqls = [self._parse(sql) for sql in sqls]
        table = self._create_sql_table(parsed_sqls)
        if not table.columns:
            raise SQLHandlerException(
                "No column exists in the table. Raw sqls: {0}".format(sqls)
            )
        return table

    def _parse(self, sql):
        raise NotImplementedError()

    def _create_sql_table(self, parsed_sqls):
        raise NotImplementedError()


class SQLHandlerException(Exception):
    pass
