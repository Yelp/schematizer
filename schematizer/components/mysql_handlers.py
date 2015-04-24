import abc
import sqlparse


class MySQLHandlerException(Exception):
    pass


def create_sql_table_from_mysql_stmts(sqls):
    """Parse and process the given table schema related MySQL statements
    and generate the corresponding SQLTable object. The first handler that
    can handle given sql statements will process and return the result.
    """
    parsed_sqls = [sqlparse.parse(sql) for sql in sqls]

    for handler_cls in mysql_handlers:
        handler = handler_cls(parsed_sqls)
        if handler.can_handle:
            return handler.run()
    raise MySQLHandlerException(
        "Unable to process MySQL statements {0}.".format(sqls)
    )


class MySQLHandlerBase(object):
    """MySQL handler Interface that processes parsed MySQL statements
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, parsed_sqls):
        super(MySQLHandlerBase, self).__init__()
        self.parsed_sqls = parsed_sqls

    @abc.abstractproperty
    def can_handle(self):
        """Whether this handler is able to process given MySQL statements
        """
        raise NotImplementedError()

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError()


class GetFinalCreateTableOnly(MySQLHandlerBase):
    """MySQL handler that processes the last create-table statement in
    the given sql statements if the last statement is a create-table
    statement.
    """

    @property
    def can_handle(self):
        if not self.parsed_sqls:
            return False
        return self.is_create_table_sql(self.parsed_sqls[-1])

    def run(self):
        # TODO [clin|DATAPIPE-124]:
        # generate SQLTable from parsed sql statement
        raise NotImplementedError()

    def is_create_table_sql(self, parsed_sql):
        raise NotImplementedError()


mysql_handlers = [
    GetFinalCreateTableOnly
]
