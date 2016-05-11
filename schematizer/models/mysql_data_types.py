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

    def __eq__(self, other):
        return (super(MySQLBit, self).__eq__(other) and
                self.length == other.length)

    def _convert_str_to_type_val(self, val_string):
        return int(val_string, base=2)

    def __hash__(self):
        return hash((self.type_name, self.length))


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

    def __eq__(self, other):
        return (super(MySQLIntegerType, self).__eq__(other) and
                self.length == other.length)

    @property
    def is_unsigned(self):
        return self.attribute_exists('unsigned')

    def _convert_str_to_type_val(self, val_string):
        return int(val_string)

    def __hash__(self):
        return hash((self.type_name, self.length))


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

    def __hash__(self):
        return hash((self.type_name, self.length))


class MySQLBoolean(MySQLBool):
    """ BOOLEAN is a synonym for BOOL """

    type_name = 'boolean'

    def __hash__(self):
        return hash(self.type_name)


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

    def __eq__(self, other):
        return (super(MySQLRealNumber, self).__eq__(other) and
                self.precision == other.precision and
                self.scale == other.scale)

    @property
    def is_unsigned(self):
        return self.attribute_exists('unsigned')

    def _convert_str_to_type_val(self, val_string):
        return float(val_string)

    def __hash__(self):
        return hash((self.type_name, self.precision, self.scale))


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
    definitions.
    """

    def __init__(self, binary=False, char_set=None, collate=None):
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

    def __init__(self, length, binary=False, char_set=None, collate=None):
        super(MySQLChar, self).__init__(binary, char_set, collate)
        self.length = length

    def __eq__(self, other):
        return (super(MySQLChar, self).__eq__(other) and
                self.length == other.length)

    def __hash__(self):
        return hash((self.type_name, self.length))


class MySQLVarChar(MySQLString):

    type_name = 'varchar'

    def __init__(self, length, binary=False, char_set=None, collate=None):
        super(MySQLVarChar, self).__init__(binary, char_set, collate)
        self.length = length

    def __eq__(self, other):
        return (super(MySQLVarChar, self).__eq__(other) and
                self.length == other.length)

    def __hash__(self):
        return hash((self.type_name, self.length))


class MySQLTinyText(MySQLString):

    type_name = 'tinytext'

    def __hash__(self):
        return hash(self.type_name)


class MySQLText(MySQLString):

    type_name = 'text'

    def __hash__(self):
        return hash(self.type_name)


class MySQLMediumText(MySQLString):

    type_name = 'mediumtext'

    def __hash__(self):
        return hash(self.type_name)


class MySQLLongText(MySQLString):

    type_name = 'longtext'

    def __hash__(self):
        return hash(self.type_name)


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

    def __eq__(self, other):
        return (super(MySQLBinary, self).__eq__(other) and
                self.length == other.length)

    def __hash__(self):
        return hash((self.type_name, self.length))


class MySQLVarBinary(MySQLBinaryBase):

    type_name = 'varbinary'

    def __init__(self, length):
        super(MySQLVarBinary, self).__init__()
        self.length = length

    def __eq__(self, other):
        return (super(MySQLVarBinary, self).__eq__(other) and
                self.length == other.length)

    def __hash__(self):
        return hash((self.type_name, self.length))


class MySQLTinyBlob(MySQLBinaryBase):

    type_name = 'tinyblob'

    def __hash__(self):
        return hash(self.type_name)


class MySQLBlob(MySQLBinaryBase):

    type_name = 'blob'

    def __hash__(self):
        return hash(self.type_name)


class MySQLMediumBlob(MySQLBinaryBase):

    type_name = 'mediumblob'

    def __hash__(self):
        return hash(self.type_name)


class MySQLLongBlob(MySQLBinaryBase):

    type_name = 'longblob'

    def __hash__(self):
        return hash(self.type_name)


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

    def __hash__(self):
        return hash(self.type_name)


class MySQLYear(MySQLDateAndTime):

    type_name = 'year'

    def __hash__(self):
        return hash(self.type_name)


class MySQLTime(MySQLDateAndTime):

    type_name = 'time'

    def __init__(self, fsp=None):
        super(MySQLTime, self).__init__()
        self.fsp = fsp

    def __eq__(self, other):
        return (super(MySQLTime, self).__eq__(other) and
                self.fsp == other.fsp)

    def __hash__(self):
        return hash((self.type_name, self.fsp))


class MySQLTimestamp(MySQLDateAndTime):

    type_name = 'timestamp'

    def __init__(self, fsp=None):
        super(MySQLTimestamp, self).__init__()
        self.fsp = fsp

    def __eq__(self, other):
        return (super(MySQLTimestamp, self).__eq__(other) and
                self.fsp == other.fsp)

    def __hash__(self):
        return hash((self.type_name, self.fsp))


class MySQLDateTime(MySQLDateAndTime):

    type_name = 'datetime'

    def __init__(self, fsp=None):
        super(MySQLDateTime, self).__init__()
        self.fsp = fsp

    def __eq__(self, other):
        return (super(MySQLDateTime, self).__eq__(other) and
                self.fsp == other.fsp)

    def __hash__(self):
        return hash((self.type_name, self.fsp))


class MySQLEnum(SQLColumnDataType):
    """ Refer to http://dev.mysql.com/doc/refman/5.5/en/enum.html for type
    definition.
    """

    type_name = 'enum'

    def __init__(self, values, char_set=None, collate=None):
        attributes = None
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
        super(MySQLEnum, self).__init__(attributes)
        self.values = values  # list of enum values

    def __eq__(self, other):
        return (super(MySQLEnum, self).__eq__(other) and
                self.values == other.values)

    def _convert_str_to_type_val(self, val_string):
        return val_string

    def __hash__(self):
        return hash(tuple([self.type_name, tuple(self.values)]))


class MySQLSet(SQLColumnDataType):
    """ Refer to http://dev.mysql.com/doc/refman/5.5/en/set.html for type
    definition.
    """

    type_name = 'set'

    def __init__(self, values, char_set=None, collate=None):
        attributes = None
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
        super(MySQLSet, self).__init__(attributes)
        self.values = values  # list of set values

    def __eq__(self, other):
        return (super(MySQLSet, self).__eq__(other) and
                self.values == other.values)

    def _convert_str_to_type_val(self, val_string):
        return val_string

    def __hash__(self):
        return hash(tuple([self.type_name, tuple(self.values)]))
