# -*- coding: utf-8 -*-
"""
This module provides an AvroSchemaBuilder, which facilitates creating an
Avro schema.
"""
import re

from avro import io
from avro import schema


FIELD_SORT_ASCENDING = 'ascending'
FIELD_SORT_DESCENDING = 'descending'
FIELD_SORT_IGNORE = 'ignore'
FIELD_SORT_ORDER = (
    FIELD_SORT_ASCENDING,
    FIELD_SORT_DESCENDING,
    FIELD_SORT_IGNORE
)


def readonly(value):
    return property(lambda cls: value)


def create_primitive_type(type_name):
    return lambda cls: type_name


class AvroSchemaBuilderMeta(type):

    def __new__(mcs, name, bases, dct):
        # define `create_*` function for primitive types
        for typ in schema.PRIMITIVE_TYPES:
            dct['create_{0}'.format(typ)] = create_primitive_type(typ)

        # define `*_type` properties for primitive and complex types
        for typ in schema.VALID_TYPES:
            dct['{0}_type'.format(typ)] = readonly(typ)

        return super(AvroSchemaBuilderMeta, mcs).__new__(mcs, name, bases, dct)


class AvroSchemaBuilder(object):
    """
    AvroSchemaBuilder creates json-formatted Avro schemas. It has bound
    method create_* for each schema type. For example, `create_int()`
    returns an Avro primitive type `int`. For each schema type, it also
    has *_type property that returns a type string and can be used to
    set `type` attribute of a schema. For example, `null_type` is string
    `null` and `record_type` is string `record`.

    Each create_* returns schema json object. This class acts as a wrapper
    to manipulate Avro schemas without directly dealing with json object or
    schema classes from Python Avro library.
    """

    __metaclass__ = AvroSchemaBuilderMeta

    _name_match_regex = re.compile('^[A-Za-z_]+\w*$')

    def __init__(self, track_created_names=False):
        """track_created_names flag controls whether previous created named
        schemas are tracked. If tracking is enabled, the later schemas can
        refer previous schemas by their full name and it will also check if
        schemas with duplicate name are defined.
        """
        self.track_created_names = track_created_names
        self._schema_names = schema.Names()

    def reset(self):
        self._schema_names = schema.Names()

    def create_enum(self, name, symbols, namespace=None, aliases=None,
                    doc=None, **metadata):
        self._validate_name(name, namespace, aliases)

        if (not symbols
                or not is_string_list(symbols)
                or not all_unique_values(symbols)):
            raise schema.AvroException(INVALID_SYMBOLS_LIST.format(symbols))

        other_props = metadata.copy()
        other_props.update(self._create_aliases_prop(aliases))
        schema_obj = schema.EnumSchema(
            name,
            namespace,
            symbols,
            self._get_schema_names(),
            doc,
            other_props
        )
        return schema_obj.to_json()

    def create_fixed(self, name, size, namespace=None, aliases=None,
                     **metadata):
        self._validate_name(name, namespace, aliases)

        if not isinstance(size, int) or size <= 0:
            raise schema.AvroException(INVALID_FIXED_SIZE.format(size))

        other_props = metadata.copy()
        other_props.update(self._create_aliases_prop(aliases))
        schema_obj = schema.FixedSchema(
            name,
            namespace,
            size,
            self._get_schema_names(),
            other_props
        )
        return schema_obj.to_json()

    def create_array(self, items_type, **metadata):
        schema_obj = schema.ArraySchema(
            items_type,
            self._get_schema_names(),
            metadata
        )
        return schema_obj.to_json()

    def create_map(self, values_type, **metadata):
        schema_obj = schema.MapSchema(
            values_type,
            self._get_schema_names(),
            metadata
        )
        return schema_obj.to_json()

    def create_field(self, name, typ, has_default=False, default_value=None,
                     sort_order=None, aliases=None, doc=None, **metadata):
        self._validate_name(name, aliases=aliases)

        other_props = metadata.copy()
        other_props.update(self._create_aliases_prop(aliases))
        field_obj = schema.Field(
            typ,
            name,
            has_default,
            default_value,
            sort_order,
            self._get_schema_names(),
            doc,
            other_props
        )
        field_json = field_obj.to_json()
        # Check whether default value is valid if it's provided
        if (has_default and not self._is_valid_default_value(
                field_json.get('type'),
                default_value
        )):
            raise schema.SchemaParseException(
                INVALID_DEFAULT_VALUE.format(default_value, type=typ)
            )
        return field_json

    def create_record(self, name, fields, namespace=None, aliases=None,
                      doc=None, **metadata):
        self._validate_name(name, namespace, aliases)

        if not fields:
            raise schema.SchemaParseException(MISSING_FIELD)

        other_props = metadata.copy()
        other_props.update(self._create_aliases_prop(aliases))
        schema_obj = schema.RecordSchema(
            name,
            namespace,
            fields,
            names=self._get_schema_names(),
            doc=doc,
            other_props=other_props
        )
        return schema_obj.to_json()

    def create_union(self, *avro_schemas):
        if not avro_schemas:
            raise schema.SchemaParseException(MISSING_AVRO_SCHEMA)

        schema_obj = schema.UnionSchema(
            list(avro_schemas),
            self._get_schema_names()
        )
        return schema_obj.to_json()

    def create_optional_type(self, schema_type, default_value=None):
        """Create an Avro schema that represents the optional `schema_type`.
        The optional type is a union schema type with `null` primitive type.
        The given default value is used to determine whether the `null` type
        should be the first item in the union type.

        If given schema_type is already an union type but doesn't have null
        type in it, it will return a new union type with null type. If given
        schema_type is an union type with null type in it, it throws exception.
        """
        null_type = self.create_null()

        schema_types = [schema_type]
        if isinstance(schema_type, list):
            if any(typ == null_type for typ in schema_type):
                raise schema.SchemaParseException(
                    ALREADY_OPTIONAL_TYPE_ERR.format(schema_type)
                )
            schema_types = []
            schema_types.extend(schema_type)

        if default_value is None:
            schema_types.insert(0, null_type)
        else:
            schema_types.append(null_type)

        schema_obj = schema.UnionSchema(schema_types, schema.Names())
        return schema_obj.to_json()

    def _get_schema_names(self):
        return (self._schema_names
                if self.track_created_names
                else schema.Names())

    @classmethod
    def _create_aliases_prop(cls, aliases):
        return {'aliases': aliases} if aliases else {}

    def _validate_name(self, name, namespace=None, aliases=None):
        """Verify if the name, namespace, and aliases follow the Avro
        naming rules. It doesn't validate duplicate names, which is done
        when constructing the *Schema object.
        """
        if not self.is_valid_name(name):
            raise schema.SchemaParseException(INVALID_NAME.format(name))

        if namespace and not self.is_valid_name(namespace):
            raise schema.SchemaParseException(
                INVALID_NAMESPACE.format(namespace)
            )

        if aliases and not is_string_list(aliases):
            raise schema.SchemaParseException(
                INVALID_ALIASES.format(aliases)
            )

    @classmethod
    def is_valid_name(cls, name_to_validate):
        if not name_to_validate:
            return False

        names = name_to_validate.split('.')
        return all(cls._name_match_regex.match(name) for name in names)

    @classmethod
    def _is_valid_default_value(cls, schema_type, value):
        """Verify whether given value is a valid default value for the
        specified schema type. It assumes the given schema_type is a valid
        Avro schema. It utilizes `avro.io.validate` function from the Python
        Avro library for the validation.
        """
        schema_obj = schema.make_avsc_object(schema_type)
        field_type = (schema_obj.schemas[0]
                      if isinstance(schema_obj, schema.UnionSchema)
                      else schema_obj)
        return io.validate(field_type, value)


def is_string_list(value):
    return (isinstance(value, list)
            and all(isinstance(val, basestring) for val in value))


def all_unique_values(values):
    return hasattr(values, '__iter__') and len(set(values)) == len(values)


# Error messages
ALREADY_OPTIONAL_TYPE_ERR = 'schema is already an optional type. Value: {0}'
INVALID_ALIASES = 'aliases is not a string list. Value: {0}'
INVALID_AVRO_SCHEMA = 'Invalid or not pre-defined Avro schema. Value: {0}'
INVALID_DEFAULT_VALUE = 'Invalid default value for type {type}. Value: {0}'
INVALID_FIXED_SIZE = 'size must be a positive integer. Value: {0}'
INVALID_NAME = 'Invalid name. Value: {0}'
INVALID_NAMESPACE = 'Invalid namespace. Value: {0}'
INVALID_SYMBOLS_LIST = 'symbols must be a list of unique strings. Value: {0}'
MISSING_AVRO_SCHEMA = 'At least one Avro schema must be provided.'
MISSING_FIELD = 'At least one field must be provided.'
