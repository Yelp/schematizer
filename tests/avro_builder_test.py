# -*- coding: utf-8 -*-
import pytest
from avro import schema

from schematizer.avro_builder import ALREADY_OPTIONAL_TYPE_ERR
from schematizer.avro_builder import AvroSchemaBuilder
from schematizer.avro_builder import FIELD_SORT_ASCENDING
from schematizer.avro_builder import INVALID_ALIASES
from schematizer.avro_builder import INVALID_DEFAULT_VALUE
from schematizer.avro_builder import INVALID_FIXED_SIZE
from schematizer.avro_builder import INVALID_NAME
from schematizer.avro_builder import INVALID_NAMESPACE
from schematizer.avro_builder import INVALID_SYMBOLS_LIST
from schematizer.avro_builder import MISSING_AVRO_SCHEMA
from schematizer.avro_builder import MISSING_FIELD
from schematizer.avro_builder import all_unique_values
from schematizer.avro_builder import is_string_list


class TestAvroSchemaBuilder(object):

    @pytest.fixture
    def schema_builder(self):
        return AvroSchemaBuilder(track_created_names=True)

    @property
    def name(self):
        return 'foo'

    @property
    def namespace(self):
        return 'ns'

    @property
    def fullname(self):
        return self.namespace + '.' + self.name

    @property
    def aliases(self):
        return ['new_foo']

    @property
    def doc(self):
        return 'sample doc'

    @property
    def metadata(self):
        return {'key1': 'val1', 'key2': 'val2'}

    @property
    def invalid_formatted_schema_names(self):
        return ['1foo', '1-foo', 'foo.', 'foo.1foo']

    @property
    def invalid_schema_names(self):
        missing_name = None
        yield missing_name

        reserved_name = 'int'
        yield reserved_name

    @property
    def invalid_aliases_list(self):
        return ['a', ['a', 1]]

    def test_create_enum(self, schema_builder):
        expected_symbols = ['a', 'b']
        actual_json = schema_builder.create_enum(self.name, expected_symbols)
        self.verify_schema_attr('name', self.name, actual_json)
        self.verify_schema_attr('type', schema_builder.enum_type, actual_json)
        self.verify_schema_attr('symbols', expected_symbols, actual_json)
        self.verify_schema_attr('namespace', None, actual_json)
        self.verify_schema_attr('aliases', None, actual_json)
        self.verify_schema_attr('doc', None, actual_json)
        self.verify_schema_metadata(None, actual_json)

    def test_create_enum_with_optional_attributes(self, schema_builder):
        expected_symbols = ['a', 'b']
        actual_json = schema_builder.create_enum(
            self.name,
            expected_symbols,
            self.namespace,
            self.aliases,
            self.doc,
            **self.metadata
        )
        self.verify_schema_attr('name', self.name, actual_json)
        self.verify_schema_attr('type', schema_builder.enum_type, actual_json)
        self.verify_schema_attr('symbols', expected_symbols, actual_json)
        self.verify_schema_attr('namespace', self.namespace, actual_json)
        self.verify_schema_attr('aliases', self.aliases, actual_json)
        self.verify_schema_attr('doc', self.doc, actual_json)
        self.verify_schema_metadata(self.metadata, actual_json)

    def test_create_enum_with_invalid_name(self, schema_builder):
        for invalid_name in self.invalid_formatted_schema_names:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_enum(invalid_name, ['a'])
            assert INVALID_NAME.format(invalid_name) == str(ex.value)

        for invalid_name in self.invalid_schema_names:
            with pytest.raises(schema.SchemaParseException):
                schema_builder.create_enum(invalid_name, ['a'])

    def test_create_enum_with_invalid_namespace(self, schema_builder):
        for invalid_namespace in self.invalid_formatted_schema_names:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_enum(self.name, ['a'], invalid_namespace)
            assert INVALID_NAMESPACE.format(invalid_namespace) == str(ex.value)

    def test_create_enum_with_dup_name(self, schema_builder):
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_enum(self.name, ['a'], self.namespace)
            duplicate_name = self.fullname
            schema_builder.create_enum(duplicate_name, ['b'])

    def test_create_enum_with_invalid_aliases(self, schema_builder):
        for invalid_aliases in self.invalid_aliases_list:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_enum(
                    self.name,
                    ['a'],
                    aliases=invalid_aliases
                )
            assert INVALID_ALIASES.format(invalid_aliases) == str(ex.value)

    def test_create_enum_with_invalid_symbols(self, schema_builder):
        invalid_symbols_list = [None, '', 'a', ['a', 1], [1, 2, 3]]
        for symbols in invalid_symbols_list:
            with pytest.raises(schema.AvroException) as ex:
                schema_builder.create_enum(self.name, symbols)
            assert INVALID_SYMBOLS_LIST.format(symbols) == str(ex.value)

    def test_create_fixed(self, schema_builder):
        expected_size = 16
        actual_json = schema_builder.create_fixed(self.name, expected_size)
        self.verify_schema_attr('name', self.name, actual_json)
        self.verify_schema_attr('type', schema_builder.fixed_type, actual_json)
        self.verify_schema_attr('size', expected_size, actual_json)
        self.verify_schema_attr('namespace', None, actual_json)
        self.verify_schema_attr('aliases', None, actual_json)
        self.verify_schema_metadata(None, actual_json)

    def test_create_fixed_with_optional_attributes(self, schema_builder):
        expected_size = 16
        actual_json = schema_builder.create_fixed(
            self.name,
            expected_size,
            self.namespace,
            self.aliases,
            **self.metadata
        )
        self.verify_schema_attr('name', self.name, actual_json)
        self.verify_schema_attr('type', schema_builder.fixed_type, actual_json)
        self.verify_schema_attr('size', expected_size, actual_json)
        self.verify_schema_attr('namespace', self.namespace, actual_json)
        self.verify_schema_attr('aliases', self.aliases, actual_json)
        self.verify_schema_metadata(self.metadata, actual_json)

    def test_create_fixed_with_invalid_name(self, schema_builder):
        for invalid_name in self.invalid_formatted_schema_names:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_fixed(invalid_name, 16)
            assert INVALID_NAME.format(invalid_name) == str(ex.value)

        for invalid_name in self.invalid_schema_names:
            with pytest.raises(schema.SchemaParseException):
                schema_builder.create_fixed(invalid_name, 16)

    def test_create_fixed_with_invalid_namespace(self, schema_builder):
        for invalid_namespace in self.invalid_formatted_schema_names:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_fixed(self.name, 16, invalid_namespace)
            assert INVALID_NAMESPACE.format(invalid_namespace) == str(ex.value)

    def test_create_fixed_with_dup_name(self, schema_builder):
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_enum(self.name, ['a'], self.namespace)
            duplicate_name = self.fullname
            schema_builder.create_fixed(duplicate_name, 16)

    def test_create_fixed_with_invalid_aliases(self, schema_builder):
        for invalid_aliases in self.invalid_aliases_list:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_fixed(
                    self.name,
                    16,
                    aliases=invalid_aliases
                )
            assert INVALID_ALIASES.format(invalid_aliases) == str(ex.value)

    def test_create_fixed_with_invalid_size(self, schema_builder):
        invalid_sizes = [0, -1, None]
        for invalid_size in invalid_sizes:
            with pytest.raises(schema.AvroException) as ex:
                schema_builder.create_fixed(self.name, invalid_size)
            assert INVALID_FIXED_SIZE.format(invalid_size) == str(ex.value)

    def valid_schema_types(self, schema_builder):
        primitive_type = 'int'
        yield primitive_type, primitive_type

        schema_builder.reset()
        complex_type = {
            'name': self.name,
            'size': 16,
            'type': 'fixed',
            'namespace': self.namespace
        }
        yield complex_type, complex_type

        schema_builder.reset()
        actual_type = schema_builder.create_fixed(
            self.name,
            16,
            self.namespace
        )
        pre_defined_type = self.fullname
        yield pre_defined_type, actual_type

    @property
    def invalid_schema_types(self):
        undefined_schema_name = 'unknown'
        yield undefined_schema_name

        non_avro_schema = {'foo': 'bar'}
        yield non_avro_schema

        named_schema_without_name = {'name': '', 'type': 'fixed', 'size': 16}
        yield named_schema_without_name

        invalid_schema = {'name': 'foo', 'type': 'enum', 'symbols': ['a', 'a']}
        yield invalid_schema

    def test_create_array(self, schema_builder):
        for typ, expected_type in self.valid_schema_types(schema_builder):
            actual_json = schema_builder.create_array(typ)
            self.verify_schema_attr(
                'type',
                schema_builder.array_type,
                actual_json
            )
            self.verify_schema_attr('items', expected_type, actual_json)
            self.verify_schema_metadata(None, actual_json)

    def test_create_array_with_optional_attributes(self, schema_builder):
        items_type = schema_builder.create_int()
        actual_json = schema_builder.create_array(items_type, **self.metadata)
        self.verify_schema_attr('type', schema_builder.array_type, actual_json)
        self.verify_schema_attr('items', items_type, actual_json)
        self.verify_schema_metadata(self.metadata, actual_json)

    def test_create_array_with_invalid_items_type(self, schema_builder):
        for invalid_schema in self.invalid_schema_types:
            with pytest.raises(schema.AvroException):
                schema_builder.create_array(invalid_schema)

    def test_create_map(self, schema_builder):
        for typ, expected_type in self.valid_schema_types(schema_builder):
            actual_json = schema_builder.create_map(typ)
            self.verify_schema_attr(
                'type',
                schema_builder.map_type,
                actual_json
            )
            self.verify_schema_attr('values', expected_type, actual_json)
            self.verify_schema_metadata(None, actual_json)

    def test_create_map_with_optional_attributes(self, schema_builder):
        values_type = schema_builder.create_int()
        actual_json = schema_builder.create_map(values_type, **self.metadata)
        self.verify_schema_attr('type', 'map', actual_json)
        self.verify_schema_attr('values', values_type, actual_json)
        self.verify_schema_metadata(self.metadata, actual_json)

    def test_create_map_with_invalid_values_type(self, schema_builder):
        for invalid_schema in self.invalid_schema_types:
            with pytest.raises(schema.AvroException):
                schema_builder.create_map(invalid_schema)

    def test_create_field(self, schema_builder):
        for typ, expected_type in self.valid_schema_types(schema_builder):
            actual_json = schema_builder.create_field(self.name, typ)
            self.verify_schema_attr('name', self.name, actual_json)
            self.verify_schema_attr('type', expected_type, actual_json)
            self.verify_schema_attr('order', None, actual_json)
            self.verify_schema_attr('aliases', None, actual_json)
            self.verify_schema_attr('doc', None, actual_json)
            self.verify_schema_metadata(None, actual_json)

    def test_create_field_with_optional_attributes(self, schema_builder):
        expected_type = schema_builder.create_int()
        expected_sort = FIELD_SORT_ASCENDING
        actual_json = schema_builder.create_field(
            self.name,
            expected_type,
            sort_order=expected_sort,
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        )
        self.verify_schema_attr('name', self.name, actual_json)
        self.verify_schema_attr('type', expected_type, actual_json)
        self.verify_schema_attr('order', expected_sort, actual_json)
        self.verify_schema_attr('aliases', self.aliases, actual_json)
        self.verify_schema_attr('doc', self.doc, actual_json)
        self.verify_schema_metadata(self.metadata, actual_json)

    def test_create_field_with_default_value(self, schema_builder):
        expected_default_value = 10
        actual_json = schema_builder.create_field(
            self.name,
            schema_builder.create_int(),
            has_default=True,
            default_value=expected_default_value
        )
        self.verify_schema_attr('default', expected_default_value, actual_json)

    def test_create_field_with_no_has_default(self, schema_builder):
        actual_json = schema_builder.create_field(
            self.name,
            schema_builder.create_int(),
            has_default=False,
            default_value=10
        )
        assert 'default' not in actual_json

    def test_create_field_with_union_type_default_value(self, schema_builder):
        int_type = schema_builder.create_int()
        null_type = schema_builder.create_null()
        field_type_and_default = [
            (schema_builder.create_union(int_type), 10),
            (schema_builder.create_union(null_type, int_type), None),
            (schema_builder.create_union(int_type, null_type), 10),
        ]
        for field_type, expected_default in field_type_and_default:
            actual_json = schema_builder.create_field(
                self.name,
                field_type,
                has_default=True,
                default_value=expected_default
            )
            self.verify_schema_attr('default', expected_default, actual_json)

    def test_create_field_with_invalid_name(self, schema_builder):
        for invalid_name in self.invalid_formatted_schema_names:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_field(invalid_name, 'int')
            assert INVALID_NAME.format(invalid_name) == str(ex.value)

        with pytest.raises(schema.SchemaParseException) as ex:
            missing_name = None
            schema_builder.create_field(missing_name, 'int')
        assert INVALID_NAME.format(missing_name) == str(ex.value)

    def test_create_field_with_invalid_type(self, schema_builder):
        for invalid_schema in self.invalid_schema_types:
            with pytest.raises(schema.SchemaParseException):
                schema_builder.create_field(self.name, invalid_schema)

    def test_create_field_with_invalid_sort_order(self, schema_builder):
        typ = schema_builder.create_int()
        sort_order = 'increase'
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_field(self.name, typ, sort_order=sort_order)

    def test_create_field_with_invalid_default(self, schema_builder):
        int_type = schema_builder.create_int()
        null_type = schema_builder.create_null()
        field_type_and_default = [
            (int_type, None),
            (schema_builder.create_union(null_type, int_type), 10),
            (schema_builder.create_union(int_type, null_type), None),
        ]
        for field_type, bad_default in field_type_and_default:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_field(
                    self.name,
                    field_type,
                    has_default=True,
                    default_value=bad_default
                )
                expected_err = INVALID_DEFAULT_VALUE.format(
                    bad_default,
                    type=field_type
                )
                assert expected_err == str(ex.value)

    @property
    def field_one(self):
        return {'name': 'foo', 'type': 'int'}

    @property
    def field_two(self):
        return {'name': 'bar', 'type': 'int'}

    def test_create_record(self, schema_builder):
        actual_json = schema_builder.create_record(
            self.name,
            [self.field_one, self.field_two]
        )
        self.verify_schema_attr('name', self.name, actual_json)
        self.verify_schema_attr(
            'type',
            schema_builder.record_type,
            actual_json
        )
        assert self.field_one == actual_json['fields'][0]
        assert self.field_two == actual_json['fields'][1]
        self.verify_schema_attr('namespace', None, actual_json)
        self.verify_schema_attr('aliases', None, actual_json)
        self.verify_schema_attr('doc', None, actual_json)
        self.verify_schema_metadata(None, actual_json)

    def test_create_record_with_optional_attributes(self, schema_builder):
        actual_json = schema_builder.create_record(
            self.name,
            [self.field_one, self.field_two],
            namespace=self.namespace,
            aliases=self.aliases,
            doc=self.doc,
            **self.metadata
        )
        self.verify_schema_attr('name', self.name, actual_json)
        self.verify_schema_attr(
            'type',
            schema_builder.record_type,
            actual_json
        )
        assert self.field_one == actual_json['fields'][0]
        assert self.field_two == actual_json['fields'][1]
        self.verify_schema_attr('namespace', self.namespace, actual_json)
        self.verify_schema_attr('aliases', self.aliases, actual_json)
        self.verify_schema_attr('doc', self.doc, actual_json)
        self.verify_schema_metadata(self.metadata, actual_json)

    def test_create_record_with_invalid_name(self, schema_builder):
        for invalid_name in self.invalid_formatted_schema_names:
            with pytest.raises(schema.SchemaParseException) as ex:
                schema_builder.create_record(invalid_name, [self.field_one])
            assert INVALID_NAME.format(invalid_name) == str(ex.value)

        for invalid_name in self.invalid_schema_names:
            with pytest.raises(schema.SchemaParseException):
                schema_builder.create_record(invalid_name, [self.field_one])

    def test_create_record_with_dup_name(self, schema_builder):
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_enum(self.name, ['a'], self.namespace)
            duplicate_name = self.fullname
            schema_builder.create_record(duplicate_name, [self.field_one])

    def test_create_record_with_missing_fields(self, schema_builder):
        with pytest.raises(schema.SchemaParseException) as ex:
            schema_builder.create_record(self.name, None)
        assert MISSING_FIELD == str(ex.value)

        with pytest.raises(schema.SchemaParseException) as ex:
            schema_builder.create_record(self.name, [])
        assert MISSING_FIELD == str(ex.value)

    def test_create_record_with_invalid_fields(self, schema_builder):
        invalid_field_list = self.field_one
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_record(self.name, invalid_field_list)

        fields_with_dup_names = [self.field_one, self.field_one]
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_record(self.name, fields_with_dup_names)

        for invalid_type in self.invalid_schema_types:
            invalid_fields = [{'name': 'foo', 'type': invalid_type}]
            with pytest.raises(schema.SchemaParseException):
                schema_builder.create_record(self.name, invalid_fields)

    def test_create_union(self, schema_builder):
        schema1 = schema_builder.create_int()
        schema2 = schema_builder.create_map('int')
        # union schema is created from two schemas
        actual_json = schema_builder.create_union(schema1, schema2)
        assert [schema1, schema2] == actual_json

        # union schema is created from a schema and pre-defined schema name
        schema2 = schema_builder.create_enum(self.name, ['a'], self.namespace)
        actual_json = schema_builder.create_union(schema1, self.fullname)
        assert [schema1, schema2] == actual_json

    def test_create_union_with_nested_union_schema(self, schema_builder):
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_union(
                schema_builder.create_union(schema_builder.create_int())
            )

        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_union([])

    def test_create_union_with_invalid_schema(self, schema_builder):
        with pytest.raises(schema.SchemaParseException) as ex:
            schema_builder.create_union()
        assert MISSING_AVRO_SCHEMA == str(ex.value)

        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_union(None)

        for invalid_schema in self.invalid_schema_types:
            with pytest.raises(schema.SchemaParseException):
                schema_builder.create_union(invalid_schema)

    def test_create_union_with_dup_primitive_schemas(self, schema_builder):
        typ = 'int'
        dup_typ = 'int'
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_union(typ, dup_typ)

    def test_create_union_with_dup_named_schemas(self, schema_builder):
        typ = {'name': self.name, 'type': 'fixed', 'size': 16}
        dup_typ = {'name': self.name, 'type': 'enum', 'symbols': ['a']}
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_union(typ, dup_typ)

    def test_create_union_with_dup_complex_schemas(self, schema_builder):
        typ = {'type': 'map', 'values': 'int'}
        dup_typ = {'type': 'map', 'values': 'string'}
        with pytest.raises(schema.SchemaParseException):
            schema_builder.create_union(typ, dup_typ)

    def test_create_optional_type(self, schema_builder):
        # non-union schema type
        expected_type = schema_builder.create_int()
        actual_json = schema_builder.create_optional_type(expected_type)
        assert 2 == len(actual_json)
        assert schema_builder.null_type == actual_json[0]
        assert expected_type == actual_json[1]

        # union schema type
        expected_type = [schema_builder.create_int()]
        actual_json = schema_builder.create_optional_type(expected_type)
        assert 2 == len(actual_json)
        assert schema_builder.null_type == actual_json[0]
        assert expected_type[0] == actual_json[1]

    def test_create_optional_type_with_default_value(self, schema_builder):
        # non-union schema type
        expected_type = schema_builder.create_int()
        actual_json = schema_builder.create_optional_type(expected_type, 10)
        assert 2 == len(actual_json)
        assert expected_type == actual_json[0]
        assert schema_builder.null_type == actual_json[1]

        # union schema type
        expected_type = [schema_builder.create_int()]
        actual_json = schema_builder.create_optional_type(expected_type, 10)
        assert 2 == len(actual_json)
        assert expected_type[0] == actual_json[0]
        assert schema_builder.null_type == actual_json[1]

    def test_create_optional_type_with_invalid_type(self, schema_builder):
        typ = [schema_builder.create_null(), schema_builder.create_int()]
        with pytest.raises(schema.SchemaParseException) as ex:
            schema_builder.create_optional_type(typ)
        assert ALREADY_OPTIONAL_TYPE_ERR.format(typ) == str(ex.value)

    def verify_schema_attr(self, attr_name, expected_value, actual_json):
        if expected_value is None:
            assert actual_json.get(attr_name) is None
        assert expected_value == actual_json.get(attr_name)

    def verify_schema_metadata(self, expected_metadata, actual_json):
        if not expected_metadata:
            return

        for key, val in expected_metadata.iteritems():
            assert val == actual_json.get(key)


class TestUtilFunc(object):

    def test_is_string_list(self):
        assert is_string_list(['a', 'b'])
        assert is_string_list([])
        assert not is_string_list(['a', 1])
        assert not is_string_list('')
        assert not is_string_list(None)

    def test_all_unique_values(self):
        assert all_unique_values(['a', 'b', 'c'])
        assert all_unique_values((1, 2, 3))
        assert all_unique_values([])
        assert not all_unique_values(['a', 'b', 'b'])
        assert not all_unique_values(None)
        assert not all_unique_values('abc')
