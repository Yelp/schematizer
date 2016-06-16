# -*- coding: utf-8 -*-
"""
This module contains the MySQL specific column data types. Refer to
http://dev.mysql.com/doc/refman/5.5/en/create-table.html and
http://dev.mysql.com/doc/refman/5.5/en/data-types.html for data
type definitions.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.sql_entities import SQLAttribute
from schematizer.models.sql_entities import SQLColumnDataType


class MySQLBit(SQLColumnDataType):
    """Class for MySQL bit data type. Refer to
    https://dev.mysql.com/doc/refman/5.5/en/bit-type.html for type definition.
    """

    type_name = 'bit'

    def __init__(self, length):
        super(MySQLBit, self).__init__()
        self.length = length

    def _convert_str_to_type_val(self, val_string):
        return int(val_string, base=2)


class MySQLIntegerType(SQLColumnDataType):
    """Base class for MySQL integer data type. Refer to
    https://dev.mysql.com/doc/refman/5.5/en/integer-types.html and
    http://dev.mysql.com/doc/refman/5.5/en/numeric-type-overview.html for type
    definitions.
    """

    def __init__(self, length, unsigned=False):
        attributes = ([SQLAttribute('unsigned')] if unsigned else None)
        super(MySQLIntegerType, self).__init__(attributes)
        self.length = length

    @property
    def is_unsigned(self):
        return self.attribute_exists('unsigned')

    def _convert_str_to_type_val(self, val_string):
        return int(val_string)


class MySQLTinyInt(MySQLIntegerType):

    type_name = 'tinyint'


class MySQLSmallInt(MySQLIntegerType):

    type_name = 'smallint'


class MySQLMediumInt(MySQLIntegerType):

    type_name = 'mediumint'


class MySQLInt(MySQLIntegerType):

    type_name = 'int'


class MySQLInteger(MySQLInt):
    """integer is an alias for int in MySQL"""

    type_name = 'integer'


class MySQLBigInt(MySQLIntegerType):

    type_name = 'bigint'


class MySQLBool(SQLColumnDataType):
    """ BOOL is a synonym for TINYINT(1) in MySQL, see
    http://dev.mysql.com/doc/refman/5.5/en/numeric-type-overview.html for
    more information. Note that we do not subclass MySQLTinyInt because
    we have a different signature for __init__
    """

    type_name = 'bool'

    def __init__(self):
        super(MySQLBool, self).__init__()
        self.length = 1

    def _convert_str_to_type_val(self, val_string):
        # MySQL considers any non-zero value as 'True' and aliases the TRUE
        # and FALSE keywords to 1 and 0 respectively.
        # For more info see:
        # http://dev.mysql.com/doc/refman/5.5/en/numeric-type-overview.html
        return not val_string.lower() in ('false', '0')


class MySQLBoolean(MySQLBool):
    """ BOOLEAN is a synonym for BOOL """

    type_name = 'boolean'


class MySQLRealNumber(SQLColumnDataType):
    """Base class for MySQL real number data types. Refer to
    https://dev.mysql.com/doc/refman/5.5/en/fixed-point-types.html and
    https://dev.mysql.com/doc/refman/5.5/en/floating-point-types.html for
    type definitions.
    """

    def __init__(self, precision, scale, unsigned=False):
        attributes = ([SQLAttribute('unsigned')] if unsigned else None)
        super(MySQLRealNumber, self).__init__(attributes)
        self.precision = precision
        self.scale = scale

    @property
    def is_unsigned(self):
        return self.attribute_exists('unsigned')

    def _convert_str_to_type_val(self, val_string):
        return float(val_string)


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
    """Base class for MySQL string data types. Refer to
    https://dev.mysql.com/doc/refman/5.5/en/string-types.html for type
    definitions. Refer to
    http://dev.mysql.com/doc/refman/5.6/en/string-type-overview.html for
    length specifications.
    """

    def __init__(self, length, binary=False, char_set=None, collate=None):
        self.length = length

        attributes = None
        if binary:
            attributes = attributes or []
            attributes.append(SQLAttribute('binary'))
        if char_set:
            attributes = attributes or []
            attributes.append(
                SQLAttribute.create_with_value('character set', char_set)
            )
        if collate:
            attributes = attributes or []
            attributes.append(
                SQLAttribute.create_with_value('collate', collate)
            )
        super(MySQLString, self).__init__(attributes)

    def _convert_str_to_type_val(self, val_string):
        return val_string


class MySQLChar(MySQLString):

    type_name = 'char'


class MySQLVarChar(MySQLString):

    type_name = 'varchar'


class MySQLTinyText(MySQLString):

    type_name = 'tinytext'

    def __init__(self, binary=False, char_set=None, collate=None):
        super(MySQLTinyText, self).__init__(
            length=255,
            binary=binary,
            char_set=char_set,
            collate=collate
        )


class MySQLText(MySQLString):

    type_name = 'text'

    def __init__(self, binary=False, char_set=None, collate=None):
        super(MySQLText, self).__init__(
            length=65535,
            binary=binary,
            char_set=char_set,
            collate=collate
        )


class MySQLMediumText(MySQLString):

    type_name = 'mediumtext'

    def __init__(self, binary=False, char_set=None, collate=None):
        super(MySQLMediumText, self).__init__(
            length=16777215,
            binary=binary,
            char_set=char_set,
            collate=collate
        )


class MySQLLongText(MySQLString):

    type_name = 'longtext'

    def __init__(self, binary=False, char_set=None, collate=None):
        super(MySQLLongText, self).__init__(
            length=429496725,
            binary=binary,
            char_set=char_set,
            collate=collate
        )


class MySQLBinaryBase(SQLColumnDataType):
    """Base class for MySQL binary data types. See
    https://dev.mysql.com/doc/refman/5.5/en/blob.html and
    https://dev.mysql.com/doc/refman/5.5/en/binary-varbinary.html for
    type definitions.
    """

    def __init__(self):
        super(MySQLBinaryBase, self).__init__()

    def _convert_str_to_type_val(self, val_string):
        return val_string


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
    """Base class for MySQL date/time data types. Refer to
    https://dev.mysql.com/doc/refman/5.5/en/date-and-time-types.html for
    type definitions.
    """

    def __init__(self):
        super(MySQLDateAndTime, self).__init__()

    def _convert_str_to_type_val(self, val_string):
        return val_string


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
    """ Refer to http://dev.mysql.com/doc/refman/5.5/en/enum.html for type
    definition.
    """

    type_name = 'enum'

    def __init__(self, values):
        super(MySQLEnum, self).__init__()
        self.values = values  # list of enum values

    def _convert_str_to_type_val(self, val_string):
        return val_string


class MySQLSet(SQLColumnDataType):
    """ Refer to http://dev.mysql.com/doc/refman/5.5/en/set.html for type
    definition.
    """

    type_name = 'set'

    def __init__(self, values):
        super(MySQLSet, self).__init__()
        self.values = values  # list of set values

    def _convert_str_to_type_val(self, val_string):
        return val_string
