# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import copy

import mock
import pytest
from avro import schema

from schematizer.logic.schema_resolution import SchemaCompatibilityValidator
from schematizer.logic.schema_resolution import SchemaResolution


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
                            default_value=None, aliases=None):
        return schema.Field(
            type_schema.to_json(),
            name,
            has_default,
            default=default_value if has_default else None,
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

    def create_bytes_decimal_schema(
        self,
        precision,
        scale=0,
        other_props=None
    ):
        return schema.BytesDecimalSchema(precision, scale, other_props)

    def create_fixed_decimal_schema(
        self,
        size,
        name,
        precision,
        scale=0,
        aliases=None
    ):
        return schema.FixedDecimalSchema(
            size,
            name,
            precision,
            scale,
            self.test_namespace,
            names=schema.Names(),
            other_props={'aliases': aliases}
        )

    def create_date_schema(self):
        return schema.DateSchema()

    def create_time_millis_schema(self):
        return schema.TimeMillisSchema()

    def create_time_micros_schema(self):
        return schema.TimeMicrosSchema()

    def create_timestamp_millis_schema(self):
        return schema.TimestampMillisSchema()

    def create_timestamp_micros_schema(self):
        return schema.TimestampMicrosSchema()


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

    def test_resolve_map_schema_with_unresolvable_values(
        self,
        resolver
    ):
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

    def test_resolve_array_schema_with_unresolvable_items(
        self,
        resolver
    ):
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
            has_default=True,
            default_value='default_string'
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

    def test_resolve_bytes_decimal_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_bytes_decimal_schema(4, 2),
            self.schema_factory.create_bytes_decimal_schema(4, 2)
        )

    def test_resolve_bytes_decimal_with_diff_precision(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_bytes_decimal_schema(4, 2),
            self.schema_factory.create_bytes_decimal_schema(3, 2)
        )

    def test_resolve_bytes_decimal_with_diff_scale(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_bytes_decimal_schema(4, 2),
            self.schema_factory.create_bytes_decimal_schema(4, 1)
        )

    def test_resolve_fixed_decimal_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 2),
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 2)
        )

    def test_resolve_fixed_decimal_schema_with_diff_name(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_fixed_decimal_schema(16, 'foo1', 4, 2),
            self.schema_factory.create_fixed_decimal_schema(16, 'foo2', 4, 2)
        )

    def test_resolve_fixed_decimal_schema_with_diff_size(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 2),
            self.schema_factory.create_fixed_decimal_schema(14, 'foo', 4, 2)
        )

    def test_resolve_fixed_decimal_schema_with_diff_precision(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 2),
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 5, 2)
        )

    def test_resolve_fixed_decimal_schema_with_diff_scale(self, resolver):
        assert not resolver.resolve_schema(
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 2),
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 1)
        )

    def test_resolve_date_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_date_schema(),
            self.schema_factory.create_date_schema()
        )

    def test_resolve_time_millis_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_time_millis_schema(),
            self.schema_factory.create_time_millis_schema()
        )

    def test_resolve_time_micros_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_time_micros_schema(),
            self.schema_factory.create_time_micros_schema()
        )

    def test_resolve_timestamp_millis_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_timestamp_millis_schema(),
            self.schema_factory.create_timestamp_millis_schema()

        )

    def test_resolve_timestamp_micros_schema(self, resolver):
        assert resolver.resolve_schema(
            self.schema_factory.create_timestamp_micros_schema(),
            self.schema_factory.create_timestamp_micros_schema()
        )

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

    @property
    def bytes_decimal_schema(self):
        return self.schema_factory.create_bytes_decimal_schema(4, 2)

    @property
    def fixed_decimal_schema(self):
        return self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 2)

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

    def test_resolve_schema_with_cache(self, resolver):
        w_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field1, self.field2]
        )
        r_schema = self.schema_factory.create_record_schema(
            'foo_table',
            [self.field2, self.field1]
        )
        w_schema_copy = copy.deepcopy(w_schema)
        with mock.patch.object(
            SchemaResolution,
            'resolve_record_schema',
            return_value=True
        ) as mock_record_resolver:
            assert resolver.resolve_schema(w_schema, r_schema)
            assert 1 == mock_record_resolver.call_count

            # The second time should not call the resolver function; instead,
            # it uses the results from the cache.
            mock_record_resolver.reset_mock()
            assert resolver.resolve_schema(w_schema_copy, r_schema)
            assert 0 == mock_record_resolver.call_count

    def test_freeze_object(self, resolver):
        obj1 = {
            'foo': 100,
            'bar': set(['beer', 'cocktail', 'wine']),
            'zoo':
            {'dog': 'bark', 'cat': 'meow', 'cow': 'moo'}
        }
        obj2 = {
            'foo': 100,
            'zoo': {'cow': 'moo', 'dog': 'bark', 'cat': 'meow'},
            'bar': set(['wine', 'beer', 'cocktail'])
        }
        frozen_obj1 = resolver.freeze_object(obj1)
        frozen_obj2 = resolver.freeze_object(obj2)
        test_dict = {frozen_obj1: 'good'}
        assert 'good' == test_dict[frozen_obj2]

    @property
    def logical_types(self):
        return [
            self.schema_factory.create_bytes_decimal_schema(4, 2),
            self.schema_factory.create_fixed_decimal_schema(16, 'foo', 4, 2),
            self.schema_factory.create_date_schema(),
            self.schema_factory.create_time_micros_schema(),
            self.schema_factory.create_time_millis_schema(),
            self.schema_factory.create_timestamp_micros_schema(),
            self.schema_factory.create_timestamp_millis_schema()
        ]

    def test_fail_resolve_different_between_logical_types(self, resolver):
        for i, writer_sch in enumerate(self.logical_types):
            for j, reader_sch in enumerate(self.logical_types):
                if i == j:
                    continue  # same logical type; skip
                assert not resolver.resolve_schema(writer_sch, reader_sch)

    def test_fail_resolve_logical_type_and_non_logical_type(self, resolver):
        non_logical_types = [
            self.primitive_schema,
            self.enum_schema,
            self.fixed_schema,
            self.map_schema,
            self.array_schema,
            self.record_schema,
            self.union_schema
        ]
        for logical in self.logical_types:
            for non_logical in non_logical_types:
                assert not resolver.resolve_schema(logical, non_logical)
                assert not resolver.resolve_schema(non_logical, logical)


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
            validator.is_backward_compatible(
                w_schema.to_json(),
                r_schema.to_json()
            )
            # Ignore the first arg which is the SchemaResolution object
            mock_resolve_func.assert_called_once_with(
                mock.ANY,
                w_schema,
                r_schema
            )
