# -*- coding: utf-8 -*-
from cached_property import cached_property
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import String

from schematizer.models.base_model import BaseModel
from schematizer.models.database import Base
from schematizer.models.types.time import build_time_column


class AvroSchemaElement(Base, BaseModel):
    """The key to a schema element of enclosing avro schema and the
    associated documentation of the element. Each key of same Avro
    schema must be unique.

    If the element is a named Avro schema, the key of the element is
    its fullname because it should be unique in the entire schema. If
    the element is non-named complex Avro schema, including array and
    map, the key is the key of their enclosing schema followed by their
    type ("array" or "map"). For the field in a Record schema, the key
    is the the fullname of the record followed by the field name.

    For example, here is a record schema:
    {
        "type": "record",
        "name": "foo",
        "field": [
            {"name": "bar", "type": {"type": "array", "items": "int"}},
            {"name": "baz", "type": {
                "type": "record",
                "name": "foo_foo",
                "fields": [
                    {"name": "bar",
                     "type": {"type": "enum", "name": "abc", "symbols": ["a"]}}
                ]
            }}
        ]
    }
    The key of each element is:
        - record schema "foo": "foo"
        - field "bar" in "foo": "foo|bar"
        - array schema of field "bar": "foo|bar|array"
        - record schema "foo_foo": "foo_foo"
        - field "bar" in "foo_foo": "foo_foo|bar"
        - enum "abc": "abc"
    """

    __tablename__ = 'avro_schema_element'

    id = Column(Integer, primary_key=True)

    # Id of avro schema which this element belongs to
    avro_schema_id = Column(
        Integer,
        ForeignKey('avro_schema.id'),
        nullable=False
    )

    # Unique key to avro schema element of the enclosing avro schema
    key = Column(String, nullable=False)

    # Avro type of the element, such as record, field, etc.
    element_type = Column(String, nullable=False)

    # Documentation of this element
    doc = Column(String, nullable=False)

    # Timestamp when the entry is created
    created_at = build_time_column(
        default_now=True,
        nullable=False
    )

    # Timestamp when the entry is last updated
    updated_at = build_time_column(
        default_now=True,
        onupdate_now=True,
        nullable=False
    )

    def to_dict(self):
        return {
            'id': self.id,
            'schema_id': self.avro_schema_id,
            'element_type': self.element_type,
            'key': self.key,
            'doc': self.doc,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }

    _SCHEMA_KEY_DELIMITER = '|'

    @cached_property
    def sub_keys(self):
        """A list of sub-keys that compose the entire key"""
        return self.key.split(self._SCHEMA_KEY_DELIMITER)

    @classmethod
    def compose_key(cls, *sub_keys):
        """Compose the final key from the given sub keys. If a sub key is
        None, it will be ignored.
        """
        return cls._SCHEMA_KEY_DELIMITER.join(
            k for k in sub_keys if k is not None
        )
