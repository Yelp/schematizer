# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from collections import deque

import simplejson
from avro import schema
from sqlalchemy import Column
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import Text
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from schematizer.models.avro_schema_element import AvroSchemaElement
from schematizer.models.base_model import BaseModel
from schematizer.models.consumer import Consumer
from schematizer.models.database import Base
from schematizer.models.database import session
from schematizer.models.note import Note
from schematizer.models.note import ReferenceTypeEnum
from schematizer.models.producer import Producer
from schematizer.models.types.time import build_time_column


class AvroSchemaStatus(object):

    READ_AND_WRITE = 'RW'
    READ_ONLY = 'R'
    DISABLED = 'Disabled'


class AvroSchema(Base, BaseModel):

    __tablename__ = 'avro_schema'

    id = Column(Integer, primary_key=True)

    # The JSON string representation of the avro schema.
    avro_schema = Column('avro_schema', Text, nullable=False)

    # Id of the topic that the schema is associated to.
    # It is a foreign key to Topic table.
    topic_id = Column(
        Integer,
        ForeignKey('topic.id'),
        nullable=False
    )

    # The schema_id where this schema is derived from.
    base_schema_id = Column(Integer, ForeignKey('avro_schema.id'))

    # Schema status: RW (read/write), R (read-only), Disabled
    status = Column(
        Enum(
            AvroSchemaStatus.READ_AND_WRITE,
            AvroSchemaStatus.READ_ONLY,
            AvroSchemaStatus.DISABLED,
            name='status'
        ),
        default=AvroSchemaStatus.READ_AND_WRITE,
        nullable=False
    )

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

    producers = relationship(Producer, backref="avro_schema")

    consumers = relationship(Consumer, backref="avro_schema")

    avro_schema_elements = relationship(
        AvroSchemaElement,
        backref="avro_schema"
    )

    @property
    def note(self):
        note = session.query(
            Note
        ).filter(
            Note.reference_type == ReferenceTypeEnum.SCHEMA,
            Note.reference_id == self.id
        ).first()
        return note

    @property
    def avro_schema_json(self):
        return simplejson.loads(self.avro_schema)

    @avro_schema_json.setter
    def avro_schema_json(self, schema_json):
        self.avro_schema = simplejson.dumps(schema_json, sort_keys=True)

    @property
    def avro_schema_with_doc(self):
        """Get the JSON representation of the Avro schema with the
        documentation and element Id of each doc-eligible element.
        """
        key_to_element_map = dict(
            (o.key, o) for o in self.avro_schema_elements
        )
        avro_schema_obj = schema.make_avsc_object(self.avro_schema_json)

        schema_elements = deque([(avro_schema_obj, None)])
        while len(schema_elements) > 0:
            schema_obj, parent_key = schema_elements.popleft()
            element_cls = _schema_to_element_map.get(schema_obj.__class__)
            if not element_cls:
                continue
            _schema_element = element_cls(schema_obj, parent_key)
            self._add_doc_to_schema(_schema_element, key_to_element_map)

            parent_key = _schema_element.key
            for nested_schema in _schema_element.nested_schema_objects:
                schema_elements.append((nested_schema, parent_key))

        return avro_schema_obj.to_json()

    ELEMENT_ID_ATTR = 'element_id'
    DOC_ATTR = 'doc'

    def _add_doc_to_schema(self, schema_element, key_to_element_map):
        element = key_to_element_map.get(schema_element.key)
        if not element:
            return

        schema_element.schema_obj.set_prop(self.DOC_ATTR, element.doc)
        schema_element.schema_obj.set_prop(self.ELEMENT_ID_ATTR, element.id)

    @classmethod
    def create_schema_elements_from_json(cls, avro_schema_json):
        """Get all the schema elements that exist in the given schema JSON.
        :param avro_schema_json: JSON representation of an Avro schema
        :return: List of AvroSchemaElement objects
        """
        avro_schema_obj = schema.make_avsc_object(avro_schema_json)

        avro_schema_elements = []
        schema_elements = deque([(avro_schema_obj, None)])
        while len(schema_elements) > 0:
            schema_obj, parent_key = schema_elements.popleft()
            element_cls = _schema_to_element_map.get(schema_obj.__class__)
            if not element_cls:
                continue

            _schema_element = element_cls(schema_obj, parent_key)
            avro_schema_element = AvroSchemaElement(
                key=_schema_element.key,
                element_type=_schema_element.element_type,
                doc=schema_obj.get_prop('doc')
            )
            avro_schema_elements.append(avro_schema_element)

            parent_key = _schema_element.key
            for nested_schema in _schema_element.nested_schema_objects:
                schema_elements.append((nested_schema, parent_key))

        return avro_schema_elements

    @classmethod
    def verify_avro_schema(cls, avro_schema_json):
        """Verify whether the given JSON representation is a valid Avro schema.

        :param avro_schema_json: JSON representation of the Avro schema
        :return: A tuple (is_valid, error) in which the first element
        indicates whether the given JSON is a valid Avro schema, and the
        second element is the error if it is not valid.
        """
        try:
            schema.make_avsc_object(avro_schema_json)
            return True, None
        except Exception as e:
            return False, repr(e)

    @classmethod
    def verify_avro_schema_has_docs(cls, avro_schema_json):
        """ Verify if the given Avro schema has docs.
        According to avro spec `doc` is supported by `record` type,
        all fields within the `record` and `enum`.

        :param avro_schema_json: JSON representation of the Avro schema

        Raises a ValueError for avro_schema_json with missing docs:
        Throws Exception for invalid avro_schema_json:
        """
        avro_schema_elements = cls.create_schema_elements_from_json(
            avro_schema_json
        )
        missing_doc_fields = [avro_schema_element.key
                              for avro_schema_element in avro_schema_elements
                              if not avro_schema_element.has_doc()]

        if missing_doc_fields:
            # TODO DATAPIPE-970  implement better exception response during
            # registering avro schema with missing docs
            raise ValueError("Missing `doc` for field(s) {}".format(
                ', '.join(missing_doc_fields)
            ))


class _SchemaElement(object):
    """Helper class that wraps the avro schema object and its corresponding
    element type.
    """

    target_schema_type = None
    element_type = None
    has_doc = None

    def __init__(self, schema_obj, parent_key):
        if not isinstance(schema_obj, self.target_schema_type):
            raise ValueError("schema_obj must be {0}. Value: {1}".format(
                self.target_schema_type.__class__.__name__,
                schema_obj
            ))
        self.schema_obj = schema_obj
        self.parent_key = parent_key

    @property
    def key(self):
        raise NotImplementedError()

    @property
    def nested_schema_objects(self):
        return []


class _RecordSchemaElement(_SchemaElement):

    target_schema_type = schema.RecordSchema
    element_type = 'record'
    has_doc = True

    @property
    def key(self):
        return self.schema_obj.fullname

    @property
    def nested_schema_objects(self):
        return self.schema_obj.fields


class _FieldElement(_SchemaElement):

    target_schema_type = schema.Field
    element_type = 'field'

    @property
    def key(self):
        return AvroSchemaElement.compose_key(
            self.parent_key,
            self.schema_obj.name
        )

    @property
    def nested_schema_objects(self):
        return [self.schema_obj.type]


class _EnumSchemaElement(_SchemaElement):

    target_schema_type = schema.EnumSchema
    element_type = 'enum'

    @property
    def key(self):
        return self.schema_obj.fullname


class _FixedSchemaElement(_SchemaElement):

    target_schema_type = schema.FixedSchema
    element_type = 'fixed'

    @property
    def key(self):
        return self.schema_obj.fullname


class _ArraySchemaElement(_SchemaElement):

    target_schema_type = schema.ArraySchema
    element_type = 'array'

    @property
    def key(self):
        return AvroSchemaElement.compose_key(
            self.parent_key,
            self.element_type
        )

    @property
    def nested_schema_objects(self):
        return [self.schema_obj.items]


class _MapSchemaElement(_SchemaElement):

    target_schema_type = schema.MapSchema
    element_type = 'map'

    @property
    def key(self):
        return AvroSchemaElement.compose_key(
            self.parent_key,
            self.element_type
        )

    @property
    def nested_schema_objects(self):
        return [self.schema_obj.values]


_schema_to_element_map = dict(
    (o.target_schema_type, o) for o in _SchemaElement.__subclasses__()
)
