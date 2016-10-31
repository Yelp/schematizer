# -*- coding: utf-8 -*-
"""
This module contains the Redshift specific column data types. Refer to
http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
for data types definition.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.sql_entities import SQLColumnDataType


class RedshiftIntegerType(SQLColumnDataType):
    """Base class for Redshift integer data type"""

    def __init__(self):
        super(RedshiftIntegerType, self).__init__()


class RedshiftSmallInt(RedshiftIntegerType):

    type_name = 'smallint'


class RedshiftInt2(RedshiftSmallInt):
    """int2 is an alias of smallint in Redshift"""

    type_name = 'int2'


class RedshiftInteger(RedshiftIntegerType):

    type_name = 'integer'


class RedshiftInt4(RedshiftInteger):
    """int4 is an alias of int/integer in Redshift"""

    type_name = 'int4'


class RedshiftBigInt(RedshiftIntegerType):

    type_name = 'bigint'


class RedshiftInt8(RedshiftBigInt):
    """int8 is an alias of bigint in Redshift"""

    type_name = 'int8'


class RedshiftFloatingPointNumeric(SQLColumnDataType):
    """Base class for non-parametric floating point numeric types"""

    def __init__(self):
        super(RedshiftFloatingPointNumeric, self).__init__()


class RedshiftUDPNumeric(SQLColumnDataType):
    """Base class for user-defined parametric floating point numeric types"""

    def __init__(self, precision, scale):
        super(RedshiftUDPNumeric, self).__init__()
        self.precision = precision
        self.scale = scale


class RedshiftReal(RedshiftFloatingPointNumeric):

    type_name = 'real'


class RedshiftFloat4(RedshiftReal):

    type_name = 'float4'


class RedshiftDouble(RedshiftFloatingPointNumeric):

    type_name = 'double precision'


class RedshiftFloat(RedshiftDouble):

    type_name = 'float'


class RedshiftFloat8(RedshiftDouble):

    type_name = 'float8'


class RedshiftDecimal(RedshiftUDPNumeric):

    type_name = 'decimal'

    def __init__(self, precision, scale):
        super(RedshiftDecimal, self).__init__(precision, scale)


class RedshiftNumeric(RedshiftDecimal):

    type_name = 'numeric'


class RedshiftString(SQLColumnDataType):
    """Base class for Redshift string data types"""

    def __init__(self, length):
        super(RedshiftString, self).__init__()
        self.length = length


class RedshiftChar(RedshiftString):

    type_name = 'char'


class RedshiftCharacter(RedshiftChar):

    type_name = 'character'


class RedshiftNChar(RedshiftChar):

    type_name = 'nchar'


class RedshiftBPChar(RedshiftChar):

    type_name = 'bpchar'

    def __init__(self):
        super(RedshiftBPChar, self).__init__(256)


class RedshiftVarChar(RedshiftString):

    type_name = 'varchar'


class RedshiftCharacterVarying(RedshiftVarChar):

    type_name = 'character varying'


class RedshiftNVarChar(RedshiftVarChar):

    type_name = 'nvarchar'


class RedshiftText(RedshiftVarChar):

    type_name = 'text'

    def __init__(self):
        super(RedshiftText, self).__init__(256)


class RedshiftDateAndTime(SQLColumnDataType):
    """Base class for Redshift date/time data types"""
    pass


class RedshiftDate(RedshiftDateAndTime):

    type_name = 'date'


class RedshiftTimestamp(RedshiftDateAndTime):

    type_name = 'timestamp'


class RedshiftTimestampTz(RedshiftDateAndTime):

    type_name = 'timestamptz'


class RedshiftBoolean(SQLColumnDataType):

    type_name = 'boolean'


class RedshiftBool(RedshiftBoolean):

    type_name = 'bool'
