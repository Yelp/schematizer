# -*- coding: utf-8 -*-
"""
This module contains the Redshift specific column data types. Refer to
http://docs.aws.amazon.com/redshift/latest/dg/c_Supported_data_types.html
for data types definition.
"""
from collections import namedtuple
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


class RedshiftRealNumber(SQLColumnDataType):
    """Base class for Redshift real number data types"""

    def __init__(self, precision=None, scale=None):
        super(RedshiftRealNumber, self).__init__()
        self.precision = precision
        self.scale = scale


class RedshiftReal(RedshiftRealNumber):

    type_name = 'real'

    def __init__(self):
        super(RedshiftReal, self).__init__()


class RedshiftFloat4(RedshiftReal):

    type_name = 'float4'


class RedshiftDouble(RedshiftRealNumber):

    type_name = 'double precision'

    def __init__(self):
        super(RedshiftDouble, self).__init__()


class RedshiftFloat(RedshiftDouble):

    type_name = 'float'


class RedshiftFloat8(RedshiftDouble):

    type_name = 'float8'


class RedshiftDecimal(RedshiftRealNumber):

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

    def __init__(self, length):
        super(RedshiftChar, self).__init__(length)


class RedshiftCharacter(RedshiftChar):

    type_name = 'character'


class RedshiftNChar(RedshiftChar):

    type_name = 'nchar'


class RedshiftBPChar(RedshiftChar):

    type_name = 'bpchar'

    def __init__(self):
        super(RedshiftBPChar).__init__(256)


class RedshiftVarChar(RedshiftString):

    type_name = 'varchar'

    def __init__(self, length):
        super(RedshiftVarChar, self).__init__(length)


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


class RedshiftBoolean(SQLColumnDataType):

    type_name = 'boolean'


class RedshiftBool(RedshiftBoolean):

    type_name = 'bool'


DbPermission = namedtuple(
    'DbPermission',
    'object_name for_user_group user_or_group_name permission'
)
