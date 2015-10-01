# -*- coding: utf-8 -*-
"""
This module implements Avro schema resolution based on the resolution rules:
http://avro.apache.org/docs/1.7.6/spec.html#Schema+Resolution

It also contains Avro schema validator which checks if a writer schema is
compatible with a reader schema.

Note that Python Avro library also has schema resolution function
`avro.io.DatumReader.match_schemas`, but it is missing some of the resolution
rules. Therefore, a separate SchemaResolution class is implemented here.
"""
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import defaultdict

from avro import schema


class SchemaCompatibilityValidator(object):

    @classmethod
    def is_backward_compatible(cls, writer_schema_json, reader_schema_json):
        """Whether the data serialized with given writer_schema can be
        deserialized using given reader schema
        """
        writer_schema = schema.make_avsc_object(writer_schema_json)
        reader_schema = schema.make_avsc_object(reader_schema_json)
        resolver = SchemaResolution()
        return resolver.resolve_schema(writer_schema, reader_schema)


class SchemaResolution(object):
    """Class that implements Avro schema resolution based on the rules:
    http://avro.apache.org/docs/1.7.6/spec.html#Schema+Resolution.
    `resolve_schema` checks if the data serialized with writer schema can be
    deserialized with reader schema.

    All the resolver functions take *Schema instances as input parameters
    instead of json-formatted Avro schema.

    Internal cache `_resolved_schemas_map` is used to track resolved schemas
    so far so that the schema that has been seen does not have to be resolved
    again.
    """

    def __init__(self):
        self._resolved_schemas_map = {}

    def reset(self):
        """Clear all the writer schema and reader schema pairs that's been
        resolved so far. It should be called before starting a new resolution
        of writer schema and reader schema so that the resolved schema pairs
        from previous resolution will not mix with the resolved schema pairs
        from the current resolution.
        """
        self._resolved_schemas_map = {}

    def _base_resolve(self, writer_schema, reader_schema, target_schema_type):
        return all(isinstance(sch, target_schema_type)
                   for sch in (writer_schema, reader_schema))

    def resolve_primitive_schema(self, writer_schema, reader_schema):
        if not self._base_resolve(
            writer_schema,
            reader_schema,
            schema.PrimitiveSchema
        ):
            return False

        # Either writer schema type is the same as reader schema type or
        # writer schema type can be promoted to reader schema type.
        return (writer_schema.fullname == reader_schema.fullname or
                self.is_promotable(writer_schema, reader_schema))

    def is_promotable(self, writer_schema, reader_schema):
        if not self._base_resolve(
            writer_schema,
            reader_schema,
            schema.PrimitiveSchema
        ):
            return False

        promotable_map = defaultdict(tuple, {
            'int': ('long', 'float', 'double'),
            'long': ('float', 'double'),
            'float': ('double',),
        })
        promotable_types = promotable_map[writer_schema.fullname]
        return reader_schema.fullname in promotable_types

    def _resolve_named_schema(self, writer_schema, reader_schema,
                              target_schema_type):
        if not self._base_resolve(
            writer_schema,
            reader_schema,
            target_schema_type
        ):
            return False

        # Name equality is defined on fullname.
        # Either fullname of both schemas are the same, or fullname of
        # writer schema is one of the reader schema alias (fullname qualified)
        if writer_schema.fullname == reader_schema.fullname:
            return True

        aliases = reader_schema.get_prop('aliases') or []
        namespace, _, _ = reader_schema.fullname.rpartition('.')
        return writer_schema.fullname in self._get_full_aliases(
            aliases,
            namespace
        )

    def _get_full_aliases(self, aliases, namespace):
        for alias in aliases:
            yield alias if '.' in alias else '.'.join([namespace, alias])

    def resolve_enum_schema(self, writer_schema, reader_schema):
        if not self._resolve_named_schema(
            writer_schema,
            reader_schema,
            schema.EnumSchema
        ):
            return False

        # reader symbols must contain all the writer symbols.
        return set(writer_schema.symbols).issubset(
            set(reader_schema.symbols)
        )

    def resolve_fixed_schema(self, writer_schema, reader_schema):
        if not self._resolve_named_schema(
            writer_schema,
            reader_schema,
            schema.FixedSchema
        ):
            return False

        # writer schema size must be equal to the reader schema size.
        return writer_schema.size == reader_schema.size

    def resolve_map_schema(self, writer_schema, reader_schema):
        if not self._base_resolve(
            writer_schema,
            reader_schema,
            schema.MapSchema
        ):
            return False
        return self.resolve_schema(writer_schema.values, reader_schema.values)

    def resolve_array_schema(self, writer_schema, reader_schema):
        if not self._base_resolve(
            writer_schema,
            reader_schema,
            schema.ArraySchema
        ):
            return False
        return self.resolve_schema(writer_schema.items, reader_schema.items)

    def resolve_record_schema(self, writer_schema, reader_schema):
        if not self._resolve_named_schema(
            writer_schema,
            reader_schema,
            schema.RecordSchema
        ):
            return False

        # Each writer schema field either can be missing in the reader schema,
        # or its type must be resolved to the type of the corresponding reader
        # schema field.
        # If a reader schema field does not exist in the writer schema,
        # it must have a default value.
        for reader_field in reader_schema.fields:
            writer_field = writer_schema.fields_dict.get(reader_field.name)
            for reader_field_alias in (reader_field.get_prop('aliases') or []):
                alias_field = writer_schema.fields_dict.get(reader_field_alias)
                if writer_field and writer_field != alias_field:
                    # reader field matches multiple writer fields
                    return False
                writer_field = writer_field or alias_field

            if not writer_field and not reader_field.has_default:
                return False
            if (writer_field and
                    not self.resolve_schema(writer_field.type,
                                            reader_field.type)):
                return False
        return True

    def resolve_union_schema(self, writer_schema, reader_schema):
        writer_schema_is_union = isinstance(writer_schema, schema.UnionSchema)
        reader_schema_is_union = isinstance(reader_schema, schema.UnionSchema)

        if not writer_schema_is_union and not reader_schema_is_union:
            return False

        writer_sub_schemas = (writer_schema.schemas if writer_schema_is_union
                              else [writer_schema])
        reader_sub_schemas = (reader_schema.schemas if reader_schema_is_union
                              else [reader_schema])

        # Each writer schema's sub schema must be resolved to one of reader
        # schema's sub schemas
        for w_sub_schema in writer_sub_schemas:
            if all(not self.resolve_schema(w_sub_schema, r_sub_schema)
                   for r_sub_schema in reader_sub_schemas):
                return False
        return True

    @property
    def resolvers(self):
        return {
            schema.PrimitiveSchema: self.resolve_primitive_schema,
            schema.EnumSchema: self.resolve_enum_schema,
            schema.FixedSchema: self.resolve_fixed_schema,
            schema.MapSchema: self.resolve_map_schema,
            schema.ArraySchema: self.resolve_array_schema,
            schema.RecordSchema: self.resolve_record_schema,
            schema.UnionSchema: self.resolve_union_schema
        }

    def resolve_schema(self, writer_schema, reader_schema):
        """Check if writer schema can be resolved to the reader schema"""
        resolver = self.resolvers.get(writer_schema.__class__)
        if (isinstance(writer_schema.type, schema.UnionSchema) or
                isinstance(reader_schema, schema.UnionSchema)):
            resolver = self.resolvers.get(schema.UnionSchema)

        key = self._create_key(writer_schema, reader_schema)
        is_resolved = self._resolved_schemas_map.get(key)
        if is_resolved is None:
            is_resolved = resolver(writer_schema, reader_schema)
            self._resolved_schemas_map[key] = is_resolved
        return is_resolved

    def _create_key(self, writer_schema, reader_schema):
        """Create the dictionary key from the given writer schema and
        reader schema. We'd like to compare the actual json content
        instead of object id.

        Since dictionary and set are unordered, it creates a frozenset
        from the sorted dictionary and sorted set.
        """
        return (self.freeze_object(writer_schema.to_json()),
                self.freeze_object(reader_schema.to_json()))

    def freeze_object(self, obj):
        if isinstance(obj, (tuple, list)):
            return tuple([self.freeze_object(item) for item in obj])

        if isinstance(obj, set):
            return tuple([self.freeze_object(item) for item in sorted(obj)])

        if isinstance(obj, dict):
            new_dict = dict((k, self.freeze_object(v)) for k, v in obj.items())
            return frozenset(sorted(new_dict.items()))

        return obj
