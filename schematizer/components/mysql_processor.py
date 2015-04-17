import abc
import sqlparse


def process_table_schema_mysql(sqls):
    """Parse and process the given table schema related MySQL statements
    and generate the corresponding SQLTable object. The first processor
    that can handle given sql statements will process and return the result.
    The processors are sorted by their priorities: one with higher priority
    will run first.
    """
    parsed_sqls = [sqlparse.parse(sql) for sql in sqls]

    for processor_cls in mysql_processors:
        processor = processor_cls(parsed_sqls)
        if processor.can_handle:
            return processor.run()
    raise Exception("Unable to process MySQL statements {0}.".format(sqls))


class MySQLProcessorBase(object):
    """Interface that processes given list of MySQL statements
    """

    __metaclass__ = abc.ABCMeta

    enabled = False
    priority = 0

    def __init__(self, parsed_sqls):
        super(MySQLProcessorBase, self).__init__()
        self.parsed_sqls = parsed_sqls

    @abc.abstractproperty
    def can_handle(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def run(self):
        raise NotImplementedError()


class GetFinalCreateTableOnly(MySQLProcessorBase):
    """MySQL processor that processes the last create-table statement in
    the given sql statements if the last statement is a create-table
    statement.
    """

    enabled = True
    priority = 1

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


def load_processors():
    enabled_processors = [processor for processor
                          in MySQLProcessorBase.__subclasses__()
                          if processor.enabled]
    enabled_processors.sort(key=lambda o: o.priority, reverse=True)
    return enabled_processors


mysql_processors = load_processors()
