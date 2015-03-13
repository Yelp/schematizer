# -*- coding: utf-8 -*-
import copy

from schematizer.sql_entities import SQLAttribute
from schematizer.sql_entities import SQLColumn
from schematizer.sql_entities import SQLColumnDataType
from schematizer.sql_entities import SQLTable


class TestSQLTable(object):

    def test_initialize(self):
        metadata = {'aliases': ['a', 'b']}
        actual = SQLTable('foo', **metadata)
        assert 'foo' == actual.name
        assert [] == actual.columns
        assert actual.doc is None
        assert metadata == actual.metadata

    @property
    def column(self):
        return SQLColumn('foo', SQLColumnDataType('typ', length=10))

    def test_equal(self):
        table_one = SQLTable('foo', [self.column])
        table_two = SQLTable('foo', [self.column])
        assert table_one == table_two

    def test_not_equal_with_other_type(self):
        assert not (SQLTable('foo', [self.column]) == 'test string')

    def test_not_equal_with_diff_column(self):
        column_one = SQLColumn('c1', SQLColumnDataType('typ', 10))
        column_two = SQLColumn('c2', SQLColumnDataType('int'))
        assert not (SQLTable('foo', [column_one, column_two]) ==
                    SQLTable('foo', [column_two, column_one]))

    def test_not_equal_with_diff_metadata(self):
        assert not (SQLColumn('foo', [self.column], custom_one=1) ==
                    SQLColumn('foo', [self.column], custom_two=1))


class TestSQLColumnDataType(object):

    def test_get_attribute(self):
        attributes = [
            SQLAttribute('attr1', 'value1', True),
            SQLAttribute('attr2', None, False)
        ]
        expected_type_name = 'foo'
        expected_length = 10
        expected_decimal = 2
        expected_values = ['a', 'b']
        actual = SQLColumnDataType(
            expected_type_name,
            expected_length,
            expected_decimal,
            expected_values,
            attributes
        )
        assert expected_type_name == actual.type_name
        assert expected_length == actual.length
        assert expected_decimal == actual.decimal
        assert expected_values == actual.values
        for expected_attr in attributes:
            assert expected_attr == actual.get_attribute(expected_attr.key)

    def test_get_attribute_with_undefined_key(self):
        actual = SQLColumnDataType('foo', [SQLAttribute('key', 'value', True)])
        assert actual.get_attribute('undefined_key') is None

    def test_equal(self):
        attributes = [
            SQLAttribute('attr1', 'value1', True),
            SQLAttribute('attr2', None, False)
        ]
        data_type_one = SQLColumnDataType('foo', 10, 2, ['a', 'b'], attributes)
        data_type_two = copy.deepcopy(data_type_one)
        assert data_type_one == data_type_two

    def test_not_equal_with_other_type(self):
        assert not (SQLColumnDataType('foo') == 'test string')

    def test_not_equal_with_diff_type_name(self):
        assert not (SQLColumnDataType('foo') == SQLColumnDataType('bar'))

    def test_not_equal_with_diff_length(self):
        assert not (SQLColumnDataType('foo', length=10) ==
                    SQLColumnDataType('foo', length=20))

    def test_not_equal_with_diff_values(self):
        assert not (SQLColumnDataType('foo', values=['a']) ==
                    SQLColumnDataType('foo', values=['a', 'b']))

    def test_not_equal_with_diff_attributes(self):
        attribute1 = SQLAttribute('attr1', 'value1', True)
        attribute2 = SQLAttribute('attr2', None, False)
        assert not (SQLColumnDataType('foo', attributes=[attribute1]) ==
                    SQLColumnDataType('foo', attributes=[attribute2]))


class TestSQLColumn(object):

    @property
    def column_type(self):
        return SQLColumnDataType('typ', length=10)

    def create_column_with_attributes(self, attributes):
        return SQLColumn('foo', self.column_type, attributes=attributes)

    def test_get_attribute(self):
        attributes = [
            SQLAttribute('attr1', 'value1', True),
            SQLAttribute('attr2', None, False)
        ]
        actual = self.create_column_with_attributes(attributes)
        for expected_attr in attributes:
            assert expected_attr == actual.get_attribute(expected_attr.key)

    def test_get_attribute_with_undefined_key(self):
        attributes = [SQLAttribute('attr1', 'value1', True)]
        actual = self.create_column_with_attributes(attributes)
        assert actual.get_attribute('undefined_key') is None

    def test_metadata(self):
        metadata = {'key1': 'value1', 'key2': 'value2'}
        actual = SQLColumn('foo', self.column_type, **metadata)
        assert metadata == actual.metadata

    def test_default_value(self):
        attributes = [SQLAttribute('default', 'value1', True)]
        actual = self.create_column_with_attributes(attributes)
        assert 'value1' == actual.default_value

        # default value is None
        attributes = [SQLAttribute('default', None, True)]
        actual = self.create_column_with_attributes(attributes)
        assert actual.default_value is None

        # no default value
        attributes = [SQLAttribute('default', '', False)]
        actual = self.create_column_with_attributes(attributes)
        assert actual.default_value is None

        # default value attribute does not exist
        attributes = [SQLAttribute('key', 'value', True)]
        actual = self.create_column_with_attributes(attributes)
        assert actual.default_value is None

    def test_is_nullable(self):
        attributes = [SQLAttribute('not null', None, False)]
        actual = self.create_column_with_attributes(attributes)
        assert not actual.is_nullable

        attributes = [SQLAttribute('key', None, False)]
        actual = self.create_column_with_attributes(attributes)
        assert actual.is_nullable

    def test_equal(self):
        attributes = [
            SQLAttribute('attr1', 'value1', True),
            SQLAttribute('attr2', None, False)
        ]
        column_one = SQLColumn(
            'foo',
            self.column_type,
            is_primary_key=True,
            attributes=attributes,
            custom_one=1,
            custom_two=2
        )
        column_two = SQLColumn(
            'foo',
            self.column_type,
            is_primary_key=True,
            attributes=attributes,
            custom_two=2,
            custom_one=1
        )
        assert column_one == column_two

    def test_not_equal_with_other_type(self):
        assert not (SQLColumn('foo', self.column_type) == 'test string')

    def test_not_equal_with_diff_column_name(self):
        assert not (SQLColumn('foo', None) == SQLColumn('bar', None))

    def test_not_equal_with_diff_column_type(self):
        assert not (SQLColumn('foo', SQLColumnDataType('int')) ==
                    SQLColumn('foo', SQLColumnDataType('long')))

    def test_not_equal_with_diff_primary_key_flag(self):
        assert not \
            (SQLColumn('foo', self.column_type, is_primary_key=False) ==
             SQLColumn('foo', self.column_type, is_primary_key=True))

    def test_not_equal_with_diff_attributes(self):
        attribute1 = SQLAttribute('attr1', 'value1', True)
        attribute2 = SQLAttribute('attr2', None, False)
        assert not \
            (SQLColumn('foo', self.column_type, attributes=[attribute1]) ==
             SQLColumn('foo', self.column_type, attributes=[attribute2]))

    def test_not_equal_with_diff_metadata(self):
        assert not (SQLColumn('foo', self.column_type, custom_one=1) ==
                    SQLColumn('foo', self.column_type, custom_two=1))
