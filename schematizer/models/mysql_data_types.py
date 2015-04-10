# -*- coding: utf-8 -*-
"""
This module contains the MySQL specific column data types. Refer to
http://dev.mysql.com/doc/refman/5.6/en/create-table.html for data
types definition.
"""
from schematizer.models.sql_entities import SQLAttribute
from schematizer.models.sql_entities import SQLColumnDataType


class MySQLInteger(SQLColumnDataType):
    """Base class for MySQL integer data type"""

    def __init__(self, length, unsigned=False):
        attributes = ([SQLAttribute('unsigned', None, False)]
                      if unsigned else None)
        super(MySQLInteger, self).__init__(attributes)
        self.length = length

    @property
    def is_unsigned(self):
        return self.attribute_exists('unsigned')


class MySQLTinyInt(MySQLInteger):

    type_name = 'tinyint'


class MySQLSmallInt(MySQLInteger):

    type_name = 'smallint'


class MySQLMediumInt(MySQLInteger):

    type_name = 'mediumint'


class MySQLInt(MySQLInteger):

    type_name = 'int'


class MySQLBigInt(MySQLInteger):

    type_name = 'bigint'


class MySQLRealNumber(SQLColumnDataType):
    """Base class for MySQL real number data types"""

    def __init__(self, precision, scale, unsigned=False):
        attributes = ([SQLAttribute('unsigned', None, False)]
                      if unsigned else None)
        super(MySQLRealNumber, self).__init__(attributes)
        self.precision = precision
        self.scale = scale

    @property
    def is_unsigned(self):
        return self.attribute_exists('unsigned')


class MySQLDouble(MySQLRealNumber):

    type_name = 'double'


class MySQLReal(MySQLDouble):

    type_name = 'real'


class MySQLFloat(MySQLRealNumber):

    type_name = 'float'


class MySQLDecimal(MySQLRealNumber):

    type_name = 'decimal'


class MySQLNumeric(MySQLDecimal):

    type_name = 'numeric'


class MySQLString(SQLColumnDataType):
    """Base class for MySQL string data types"""

    def __init__(self, binary=False, char_set=None, collate=None):
        attributes = None
        if binary:
            attributes = attributes or []
            attributes.append(SQLAttribute('binary', None, False))
        if char_set:
            attributes = attributes or []
            attributes.append(SQLAttribute('character set', char_set, True))
        if collate:
            attributes = attributes or []
            attributes.append(SQLAttribute('collate', collate, True))
        super(MySQLString, self).__init__(attributes)


class MySQLChar(MySQLString):

    type_name = 'char'

    def __init__(self, length, binary=False, char_set=None, collate=None):
        super(MySQLChar, self).__init__(binary, char_set, collate)
        self.length = length


class MySQLVarChar(MySQLString):

    type_name = 'varchar'

    def __init__(self, length, binary=False, char_set=None, collate=None):
        super(MySQLVarChar, self).__init__(binary, char_set, collate)
        self.length = length


class MySQLTinyText(MySQLString):

    type_name = 'tinytext'


class MySQLText(MySQLString):

    type_name = 'text'


class MySQLMediumText(MySQLString):

    type_name = 'mediumtext'


class MySQLLongText(MySQLString):

    type_name = 'longtext'


class MySQLBinaryBase(SQLColumnDataType):
    """Base class for MySQL binary data types"""

    def __init__(self):
        super(MySQLBinaryBase, self).__init__()


class MySQLBinary(MySQLBinaryBase):

    type_name = 'binary'

    def __init__(self, length):
        super(MySQLBinary, self).__init__()
        self.length = length


class MySQLVarBinary(MySQLBinaryBase):

    type_name = 'varbinary'

    def __init__(self, length):
        super(MySQLVarBinary, self).__init__()
        self.length = length


class MySQLTinyBlob(MySQLBinaryBase):

    type_name = 'tinyblob'


class MySQLBlob(MySQLBinaryBase):

    type_name = 'blob'


class MySQLMediumBlob(MySQLBinaryBase):

    type_name = 'mediumblob'


class MySQLLongBlob(MySQLBinaryBase):

    type_name = 'longblob'


class MySQLDateAndTime(SQLColumnDataType):
    """Base class for MySQL date/time data types"""

    def __init__(self):
        super(MySQLDateAndTime, self).__init__()


class MySQLDate(MySQLDateAndTime):

    type_name = 'date'


class MySQLYear(MySQLDateAndTime):

    type_name = 'year'


class MySQLTime(MySQLDateAndTime):

    type_name = 'time'

    def __init__(self, fsp=None):
        super(MySQLTime, self).__init__()
        self.fsp = fsp


class MySQLTimestamp(MySQLDateAndTime):

    type_name = 'timestamp'

    def __init__(self, fsp=None):
        super(MySQLTimestamp, self).__init__()
        self.fsp = fsp


class MySQLDateTime(MySQLDateAndTime):

    type_name = 'datetime'

    def __init__(self, fsp=None):
        super(MySQLDateTime, self).__init__()
        self.fsp = fsp


class MySQLEnum(SQLColumnDataType):

    type_name = 'enum'

    def __init__(self, values):
        super(MySQLEnum, self).__init__()
        self.values = values  # list of enum values


class MySQLSet(SQLColumnDataType):

    type_name = 'set'

    def __init__(self, values):
        super(MySQLSet, self).__init__()
        self.values = values  # list of set values
