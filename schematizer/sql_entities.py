# -*- coding: utf-8 -*-
"""
This module contains the internal data structure to hold the information
of parsed SQL schemas.
"""
from collections import namedtuple


class SQLTable(object):
    """Internal data structure that represents a general sql table.
    It is intended to support sql table definition in general, not
    MySQL specific.
    """

    def __init__(self, table_name, columns=None, doc=None, **metadata):
        self.name = table_name
        self.columns = columns or []
        self.doc = doc
        # any additional metadata that does not belong to sql table
        # definition but would like to be tracked.
        self.metadata = metadata

    def __eq__(self, other):
        return (isinstance(other, SQLTable)
                and self.name == other.name
                and self.columns == other.columns
                and self.metadata == other.metadata)


SQLAttribute = namedtuple('SQLAttribute', 'key value has_value')


class SQLColumnDataType(object):
    """Internal data structure that contains column data type information.
    For example:

    date == SQLColumnDataType('date')
    varchar(256) == SQLColumnDataType('varchar', length=256)
    decimal(10, 2) == SQLColumnDataType('decimal', length=10, decimal=2)
    int(11) unsigned == SQLColumnDataType('decimal', length=10,
                            attributes=[SQLAttribute('unsigned', None, False])
    enum('a', 'b') == SQLColumnDataType('enum', values=['a', 'b'])
    """

    def __init__(self, type_name, length=None, decimal=None, values=None,
                 attributes=None):
        self.type_name = type_name
        self.length = length
        self.decimal = decimal
        self.values = values  # list of enum or set values
        self.attributes = set(attributes or [])
        self._attributes_lookup = dict((attr.key, attr)
                                       for attr in self.attributes)

    def get_attribute(self, key):
        return self._attributes_lookup.get(key)

    def __eq__(self, other):
        return (isinstance(other, SQLColumnDataType)
                and self.type_name == other.type_name
                and self.length == other.length
                and self.values == other.values
                and self.attributes == other.attributes)


class SQLColumn(object):
    """Internal data structure that represents a general sql column.
    It is intended to support sql column definition in general, not
    MySQL specific.
    """

    def __init__(self, column_name, column_type, is_primary_key=False,
                 attributes=None, doc=None, **metadata):
        self.name = column_name
        self.type = column_type
        self.is_primary_key = is_primary_key
        self.doc = doc
        # attributes contains information such as nullable, default value, etc.
        self.attributes = set(attributes or [])
        self._attributes_lookup = dict((attr.key, attr)
                                       for attr in self.attributes)
        # any additional metadata that does not belong to sql column
        # definition but would like to be tracked, such as alias
        self.metadata = metadata

    def get_attribute(self, key):
        return self._attributes_lookup.get(key)

    @property
    def default_value(self):
        default = self.get_attribute('default')
        return default.value if default and default.has_value else None

    @property
    def is_nullable(self):
        return not self.get_attribute('not null')

    def __eq__(self, other):
        return (isinstance(other, SQLColumn)
                and self.name == other.name
                and self.type == other.type
                and self.is_primary_key == other.is_primary_key
                and self.attributes == other.attributes
                and self.metadata == other.metadata)


DbPermission = namedtuple(
    'DbPermission',
    'object_name for_user_group user_or_group_name permission'
)


class MetaDataKey(object):
    """Key of metadata attributes"""

    NAMESPACE = 'namespace'
    ALIASES = 'aliases'
    PERMISSION = 'permission'
