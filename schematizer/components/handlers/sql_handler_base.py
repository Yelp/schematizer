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
        return self._create_sql_table(parsed_sqls)

    def _parse(self, sql):
        raise NotImplementedError()

    def _create_sql_table(self, parsed_sqls):
        raise NotImplementedError()


class SQLHandlerException(Exception):
    pass


class SQLTableBuilderBase(object):
    """Interface that constructs SQLTable from parsed SQL statements
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, parsed_sqls):
        super(SQLTableBuilderBase, self).__init__()
        self.parsed_sqls = parsed_sqls

    @abc.abstractproperty
    def can_handle(self):
        """Whether this handler is able to process given SQL statements
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError()
