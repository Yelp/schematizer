# -*- coding: utf-8 -*-
from contextlib import nested

import mock
import pytest
from avro import schema

from schematizer.schema_resolution import SchemaCompatibilityValidator
from schematizer.schema_resolution import SchemaResolution


class AvroSchemaFactory(object):

    test_namespace = 'ns_test'

    def create_primitive_schema(self, primitive_type):
        return schema.PrimitiveSchema(primitive_type)

    def create_enum_schema(self, name, symbols, aliases=None):
        return schema.EnumSchema(
            name,
            self.test_namespace,
            symbols,
            names=schema.Names(),
            other_props={'aliases': aliases}
        )

    def create_fixed_schema(self, name, size, aliases=None):
        return schema.FixedSchema(
            name,
            self.test_namespace,
            size,
            names=schema.Names(),
            other_props={'aliases': aliases}
        )

    def create_map_schema(self, values_schema):
        return schema.MapSchema(values_schema.to_json(), names=schema.Names())

    def create_array_schema(self, items_schema):
        return schema.ArraySchema(items_schema.to_json(), names=schema.Names())

    def create_field_schema(self, name, type_schema, has_default=False,
                            aliases=None):
        return schema.Field(
            type_schema.to_json(),
            name,
            has_default,
            default=mock.Mock() if has_default else None,
            names=schema.Names(),
            other_props={'aliases': aliases}
        )

    def create_record_schema(self, name, fields, aliases=None):
        return schema.RecordSchema(
            name,
            self.test_namespace,
            [field.to_json() for field in fields],
            names=schema.Names(),
            other_props={'aliases': aliases}
        )

    def create_union_schema(self, *avro_schemas):
        return schema.UnionSchema(
            [avro_schema.to_json() for avro_schema in avro_schemas],
            names=schema.Names()
        )


class TestSchemaResolution(object):

    @pytest.fixture
    def resolver(self):
        return SchemaResolution()

    @property
    def schema_factory(self):
        return AvroSchemaFactory()

    def test_is_promotable(self, resolver):
        assert resolver.is_promotable(
            self.schema_factory.create_primitive_schema('int'),
            self.schema_factory.create_primitive_schema('long')
        )

    def test_is_promotable_with_unpromotable_type(self, resolver):
        assert not resolver.is_promotable(
            self.schema_factory.create_primitive_schema('double'),
            self.schema_factory.create_primitive_schema('float')
        )

    def test_is_promotable_with_non_primitive_type(self, resolver):
        assert not resolver.is_promotable(
            self.schema_factory.create_primitive_schema('int'),
            self.schema_factory.create_enum_schema('foo', ['a'])
        )
        assert not resolver.is_promotable(
            self.schema_factory.create_enum_schema('foo', ['a']),
            self.schema_factory.create_primitive_schema('int')
        )

    def test_resolve_primitive_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_primitive_schema('int'),
            self.schema_factory.create_primitive_schema('int')
        )

    def test_resolve_promotable_primitive_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_primitive_schema('int'),
            self.schema_factory.create_primitive_schema('double')
        )

    def test_resolve_not_promotable_schema(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_primitive_schema('double'),
            self.schema_factory.create_primitive_schema('int')
        )

    def test_resolve_enum_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_enum_schema('foo', ['a']),
            self.schema_factory.create_enum_schema('foo', ['a', 'b'])
        )

    def test_resolve_enum_schema_with_diff_symbols(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_enum_schema('foo', ['a', 'b']),
            self.schema_factory.create_enum_schema('foo', ['a', 'c'])
        )

    def test_resolve_fixed_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_fixed_schema('foo', 16),
            self.schema_factory.create_fixed_schema('foo', 16)
        )

    def test_resolve_fixed_schema_with_diff_size(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_fixed_schema('foo', 16),
            self.schema_factory.create_fixed_schema('foo', 32)
        )

    def test_resolve_map_schema(self, resolver):
        int_schema = self.schema_factory.create_primitive_schema('int')
        w_schema = self.schema_factory.create_map_schema(int_schema)
        r_schema = self.schema_factory.create_map_schema(int_schema)
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_map_schema_with_unresolvable_values(self,
                                                         resolver):
        w_schema = self.schema_factory.create_map_schema(
            self.schema_factory.create_primitive_schema('long')
        )
        r_schema = self.schema_factory.create_map_schema(
            self.schema_factory.create_primitive_schema('int')
        )
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_array_schema(self, resolver):
        fixed_schema = self.schema_factory.create_fixed_schema('foo', 16)
        w_schema = self.schema_factory.create_array_schema(fixed_schema)
        r_schema = self.schema_factory.create_array_schema(fixed_schema)
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_array_schema_with_unresolvable_items(self,
                                                          resolver):
        w_schema = self.schema_factory.create_array_schema(
            self.schema_factory.create_fixed_schema('foo', 16)
        )
        r_schema = self.schema_factory.create_array_schema(
            self.schema_factory.create_fixed_schema('foo', 32)
        )
        assert not resolver.resolve_schema(w_schema, r_schema)

    @property
    def field1(self):
        return self.schema_factory.create_field_schema(
            'field1',
            self.schema_factory.create_primitive_schema('int')
        )

    @property
    def field2(self):
        return self.schema_factory.create_field_schema(
            'field2',
            self.schema_factory.create_enum_schema('foo', ['a', 'b'])
        )

    @property
    def field3(self):
        return self.schema_factory.create_field_schema(
            'field3',
            self.schema_factory.create_primitive_schema('string'),
            has_default=True
        )

    def test_resolve_record_schema(self, resolver):
        w_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1, self.field2]
        )
        r_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field2, self.field1]
        )
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_record_schema_with_extra_reader_field(self, resolver):
        w_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1, self.field2]
        )
        r_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1, self.field2, self.field3]
        )
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_record_schema_with_extra_reader_field_without_default(
            self,
            resolver
    ):
        w_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1]
        )
        r_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1, self.field2]
        )
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_record_schema_with_extra_writer_field(self, resolver):
        w_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1, self.field2]
        )
        r_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1]
        )
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_record_schema_with_field_aliases(self, resolver):
        w_field = self.schema_factory.create_field_schema(
            'w_field',
            self.schema_factory.create_primitive_schema('int')
        )
        w_schema = self.schema_factory.create_record_schema('foo', [w_field])

        r_field = self.schema_factory.create_field_schema(
            'r_field',
            self.schema_factory.create_primitive_schema('int'),
            aliases=['w_field']
        )
        r_schema = self.schema_factory.create_record_schema('foo', [r_field])
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_record_schema_with_un_resolvable_field(self, resolver):
        w_field = self.schema_factory.create_field_schema(
            'w_field',
            self.schema_factory.create_primitive_schema('int')
        )
        w_schema = self.schema_factory.create_record_schema('foo', [w_field])

        r_field = self.schema_factory.create_field_schema(
            'w_field',
            self.schema_factory.create_primitive_schema('string')
        )
        r_schema = self.schema_factory.create_record_schema('foo', [r_field])
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_record_schema_with_no_match_field_alias(self, resolver):
        w_field = self.schema_factory.create_field_schema(
            'w_field',
            self.schema_factory.create_primitive_schema('int')
        )
        w_schema = self.schema_factory.create_record_schema('foo', [w_field])

        r_field = self.schema_factory.create_field_schema(
            'r_field',
            self.schema_factory.create_primitive_schema('int'),
            aliases=['bar']
        )
        r_schema = self.schema_factory.create_record_schema('foo', [r_field])
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_record_schema_with_multi_match_fields(self, resolver):
        w1_field = self.schema_factory.create_field_schema(
            'w_field',
            self.schema_factory.create_primitive_schema('int')
        )
        w2_field = self.schema_factory.create_field_schema(
            'r_field',
            self.schema_factory.create_primitive_schema('int')
        )
        w_schema = self.schema_factory.create_record_schema(
            'foo',
            [w1_field, w2_field]
        )

        r_field = self.schema_factory.create_field_schema(
            'r_field',
            self.schema_factory.create_primitive_schema('int'),
            aliases=['w_field']
        )
        r_schema = self.schema_factory.create_record_schema('foo', [r_field])
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_union_schema_with_both_unions(self, resolver):
        w_schema = self.schema_factory.create_union_schema(
            self.schema_factory.create_primitive_schema('int'),
            self.schema_factory.create_array_schema(
                self.schema_factory.create_primitive_schema('float')
            )
        )
        r_schema = self.schema_factory.create_union_schema(
            self.schema_factory.create_array_schema(
                self.schema_factory.create_primitive_schema('double')
            ),
            self.schema_factory.create_primitive_schema('long')
        )
        assert resolver.resolve_schema(w_schema, r_schema)

        resolver.reset()
        r_schema = self.schema_factory.create_union_schema(
            self.schema_factory.create_primitive_schema('double'),
            self.schema_factory.create_primitive_schema('null')
        )
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_union_schema_with_writer_union(self, resolver):
        r_schema = self.schema_factory.create_primitive_schema('double')
        w_schema = self.schema_factory.create_union_schema(
            self.schema_factory.create_primitive_schema('int'),
            self.schema_factory.create_primitive_schema('long')
        )
        assert resolver.resolve_schema(w_schema, r_schema)

        resolver.reset()
        w_schema = self.schema_factory.create_union_schema(
            self.schema_factory.create_primitive_schema('int'),
            self.schema_factory.create_array_schema(
                self.schema_factory.create_primitive_schema('string')
            )
        )
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_union_schema_with_reader_union(self, resolver):
        w_schema = self.schema_factory.create_primitive_schema('int')
        r_schema = self.schema_factory.create_union_schema(
            self.schema_factory.create_primitive_schema('double'),
            self.schema_factory.create_primitive_schema('null')
        )
        assert resolver.resolve_schema(w_schema, r_schema)

        resolver.reset()
        r_schema = self.schema_factory.create_union_schema(
            self.schema_factory.create_primitive_schema('string'),
            self.schema_factory.create_primitive_schema('bytes')
        )
        assert not resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_named_schema(self, resolver):
        # both schemas have the same namespace
        w_schema = self.schema_factory.create_enum_schema('foo', ['a'])
        r_schema = self.schema_factory.create_enum_schema('bar', ['a'])
        assert not resolver.resolve_schema(w_schema, r_schema)

        # writer schema and reader schema have different namespace
        w_schema = self.schema_factory.create_enum_schema('foo', ['a'])
        r_schema = self.schema_factory.create_enum_schema('bar.foo', ['a'])
        assert not resolver.resolve_schema(w_schema, r_schema)

        # both schemas have the same namespace
        name = self.schema_factory.test_namespace + '.' + 'foo'
        w_schema = self.schema_factory.create_enum_schema('foo', ['a'])
        r_schema = self.schema_factory.create_enum_schema(name, ['a'])
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_named_schema_with_aliases(self, resolver):
        w_schema = self.schema_factory.create_fixed_schema(
            'foo',
            16,
            aliases=['bar']
        )
        r_schema = self.schema_factory.create_fixed_schema(
            'foo_new',
            16,
            aliases=['foo', 'new']
        )
        assert resolver.resolve_schema(w_schema, r_schema)

    def test_resolve_named_schema_with_no_match_aliases(self, resolver):
        w_schema = self.schema_factory.create_fixed_schema(
            'foo',
            16,
            aliases=['bar']
        )
        r_schema = self.schema_factory.create_fixed_schema(
            'foo_new',
            16,
            aliases=['bar']
        )
        assert not resolver.resolve_schema(w_schema, r_schema)

    @property
    def primitive_schema(self):
        return self.schema_factory.create_primitive_schema('int')

    @property
    def fixed_schema(self):
        return self.schema_factory.create_fixed_schema('foo', 16)

    @property
    def enum_schema(self):
        return self.schema_factory.create_enum_schema('bar', ['a', 'b'])

    @property
    def map_schema(self):
        return self.schema_factory.create_map_schema(self.primitive_schema)

    @property
    def array_schema(self):
        return self.schema_factory.create_array_schema(self.enum_schema)

    @property
    def record_schema(self):
        return self.schema_factory.create_record_schema(
            'foo_tbl',
            [self.schema_factory.create_field_schema('fld', self.fixed_schema)]
        )

    @property
    def union_schema(self):
        return self.schema_factory.create_union_schema(self.fixed_schema)

    def test_resolve_primitive_schema_with_unsupported_schemas(self, resolver):
        self.resolve_unsupported_schemas(
            self.primitive_schema,
            self.enum_schema,
            resolver.resolve_primitive_schema
        )

    def test_resolve_enum_schema_with_unsupported_schemas(self, resolver):
        self.resolve_unsupported_schemas(
            self.enum_schema,
            self.fixed_schema,
            resolver.resolve_enum_schema
        )

    def test_resolve_fixed_schema_with_unsupported_schemas(self, resolver):
        self.resolve_unsupported_schemas(
            self.fixed_schema,
            self.primitive_schema,
            resolver.resolve_fixed_schema
        )

    def test_resolve_map_schema_with_unsupported_schemas(self, resolver):
        self.resolve_unsupported_schemas(
            self.map_schema,
            self.primitive_schema,
            resolver.resolve_map_schema
        )

    def test_resolve_array_schema_with_unsupported_schemas(self, resolver):
        self.resolve_unsupported_schemas(
            self.array_schema,
            self.map_schema,
            resolver.resolve_array_schema
        )

    def test_resolve_record_schema_with_unsupported_schemas(self, resolver):
        self.resolve_unsupported_schemas(
            self.record_schema,
            self.primitive_schema,
            resolver.resolve_record_schema
        )

    def test_resolve_union_schema_with_unsupported_schemas(self, resolver):
        self.resolve_unsupported_schemas(
            self.union_schema,
            self.record_schema,
            resolver.resolve_union_schema,
        )

    def resolve_unsupported_schemas(
            self,
            target_schema,
            non_target_schema,
            resolver_func
    ):
        assert not resolver_func(target_schema, non_target_schema)
        assert not resolver_func(non_target_schema, target_schema)
        assert not resolver_func(non_target_schema, non_target_schema)

    def default_mock_call_counts(self):
        keys = ['primitive', 'enum', 'fixed', 'map', 'array',
                'record', 'union']
        return dict.fromkeys(keys, 0*len(keys))

    def test_resolve_primitive_schema_resolver(self, resolver):
        expected_call_counts = self.default_mock_call_counts()
        expected_call_counts['primitive'] = 1
        self.verify_resolve_schema_resolvers(
            resolver,
            self.primitive_schema,
            expected_call_counts
        )

    def test_resolve_enum_schema_resolver(self, resolver):
        expected_call_counts = self.default_mock_call_counts()
        expected_call_counts['enum'] = 1
        self.verify_resolve_schema_resolvers(
            resolver,
            self.enum_schema,
            expected_call_counts
        )

    def test_resolve_fixed_schema_resolver(self, resolver):
        expected_call_counts = self.default_mock_call_counts()
        expected_call_counts['fixed'] = 1
        self.verify_resolve_schema_resolvers(
            resolver,
            self.fixed_schema,
            expected_call_counts
        )

    def test_resolve_map_schema_resolver(self, resolver):
        expected_call_counts = self.default_mock_call_counts()
        expected_call_counts['map'] = 1
        self.verify_resolve_schema_resolvers(
            resolver,
            self.map_schema,
            expected_call_counts
        )

    def test_resolve_array_schema_resolver(self, resolver):
        expected_call_counts = self.default_mock_call_counts()
        expected_call_counts['array'] = 1
        self.verify_resolve_schema_resolvers(
            resolver,
            self.array_schema,
            expected_call_counts
        )

    def test_resolve_record_schema_resolver(self, resolver):
        expected_call_counts = self.default_mock_call_counts()
        expected_call_counts['record'] = 1
        self.verify_resolve_schema_resolvers(
            resolver,
            self.record_schema,
            expected_call_counts
        )

    def test_resolve_union_schema_resolver(self, resolver):
        expected_call_counts = self.default_mock_call_counts()
        expected_call_counts['union'] = 1
        self.verify_resolve_schema_resolvers(
            resolver,
            self.union_schema,
            expected_call_counts
        )

    def verify_resolve_schema_resolvers(self, resolver, test_schema,
                                        expected_call_counts):
        with nested(
            mock.patch.object(SchemaResolution, 'resolve_primitive_schema'),
            mock.patch.object(SchemaResolution, 'resolve_enum_schema'),
            mock.patch.object(SchemaResolution, 'resolve_fixed_schema'),
            mock.patch.object(SchemaResolution, 'resolve_map_schema'),
            mock.patch.object(SchemaResolution, 'resolve_array_schema'),
            mock.patch.object(SchemaResolution, 'resolve_record_schema'),
            mock.patch.object(SchemaResolution, 'resolve_union_schema')
        ) as (mock_primitive_resolver,
              mock_enum_resolver,
              mock_fixed_resolver,
              mock_map_resolver,
              mock_array_resolver,
              mock_record_resolver,
              mock_union_resolver):
            resolver.resolve_schema(test_schema, test_schema)
            actual_call_counts = {
                'primitive': mock_primitive_resolver.call_count,
                'enum': mock_enum_resolver.call_count,
                'fixed': mock_fixed_resolver.call_count,
                'map': mock_map_resolver.call_count,
                'array': mock_array_resolver.call_count,
                'record': mock_record_resolver.call_count,
                'union': mock_union_resolver.call_count
            }
            assert expected_call_counts == actual_call_counts


class TestSchemaCompatibilityValidator(object):

    @pytest.fixture
    def validator(self):
        return SchemaCompatibilityValidator()

    @property
    def schema_factory(self):
        return AvroSchemaFactory()

    def test_is_backward_compatible(self, validator):
        with mock.patch.object(
                SchemaResolution,
                'resolve_schema',
                autospec=True
        ) as mock_resolve_func:
            w_schema = self.schema_factory.create_record_schema(
                'foo_table',
                [self.schema_factory.create_field_schema(
                    'field',
                    self.schema_factory.create_primitive_schema('int')
                )]
            )
            r_schema = self.schema_factory.create_record_schema(
                'foo_table',
                [self.schema_factory.create_field_schema(
                    'field',
                    self.schema_factory.create_primitive_schema('long')
                )]
            )
            validator.is_backward_compatible(w_schema, r_schema)
            # Ignore the first arg which is the SchemaResolution object
            mock_resolve_func.assert_called_once_with(
                mock.ANY,
                w_schema,
                r_schema
            )
