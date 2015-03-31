# -*- coding: utf-8 -*-
"""
This module contains the internal data structure to hold the information
of parsed SQL schemas.
"""
from collections import namedtuple


class SQLTable(object):
    """Internal data structure that represents a general sql table.
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


class SQLColumn(object):
    """Internal data structure that represents a general sql column.
    It is intended to support sql column definition in general. The
    column type could be database specific.
    """

    def __init__(self, column_name, column_type, is_primary_key=False,
                 is_nullable=True, default_value=None,
                 attributes=None, doc=None, **metadata):
        self.name = column_name
        self.type = column_type
        self.is_primary_key = is_primary_key
        self.is_nullable = is_nullable
        self.default_value = default_value
        self.doc = doc
        # attributes contains column settings except default value and nullable
        self.attributes = set(attributes or [])
        self._attributes_lookup = dict((attr.name, attr)
                                       for attr in self.attributes)
        # any additional metadata that does not belong to sql column
        # definition but would like to be tracked, such as alias
        self.metadata = metadata

    def get_attribute(self, key):
        return self._attributes_lookup.get(key)

    def __eq__(self, other):
        return (isinstance(other, SQLColumn)
                and self.name == other.name
                and self.type == other.type
                and self.is_primary_key == other.is_primary_key
                and self.is_nullable == other.is_nullable
                and self.default_value == other.default_value
                and self.attributes == other.attributes
                and self.metadata == other.metadata)


SQLAttribute = namedtuple('SQLAttribute', 'name value has_value')


class SQLColumnDataType(object):
    """Internal data structure that contains column data type information.
    """

    type_name = None

    def __init__(self, attributes=None):
        self.attributes = set(attributes or [])
        self._attributes_lookup = dict((attr.name, attr)
                                       for attr in self.attributes)

    def attribute_exists(self, name):
        return name in self._attributes_lookup

    def get_attribute(self, name):
        return self._attributes_lookup.get(name)

    def __eq__(self, other):
        return (isinstance(other, SQLColumnDataType)
                and self.attributes == other.attributes)


class MetaDataKey(object):
    """Key of metadata attributes"""

    NAMESPACE = 'namespace'
    ALIASES = 'aliases'
    PERMISSION = 'permission'
