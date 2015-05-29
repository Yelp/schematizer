# -*- coding: utf-8 -*-
import simplejson
from yelp_lib.classutil import cached_property

from schematizer.api.exceptions import exceptions_v1


class RequestBase(object):

    @classmethod
    def create_from_string(cls, request_body_string):
        request_json = simplejson.loads(request_body_string)
        return cls(**request_json)


class AvroSchemaRequestBase(RequestBase):

    def __init__(self, schema):
        super(AvroSchemaRequestBase, self).__init__()
        self.schema = schema

    @cached_property
    def schema_json(self):
        try:
            return simplejson.loads(self.schema) if self.schema else None
        except simplejson.JSONDecodeError as e:
            raise exceptions_v1.invalid_schema_exception(repr(e))


class RegisterSchemaRequest(AvroSchemaRequestBase):

    def __init__(self, schema, namespace, source, source_owner_email,
                 base_schema_id=None):
        super(RegisterSchemaRequest, self).__init__(schema=schema)
        self.namespace = namespace
        self.source = source
        self.source_owner_email = source_owner_email
        self.base_schema_id = base_schema_id


class RegisterSchemaFromMySqlRequest(RequestBase):

    def __init__(self, mysql_statements, namespace, source,
                 source_owner_email):
        super(RegisterSchemaFromMySqlRequest, self).__init__()
        self.mysql_statements = mysql_statements
        self.namespace = namespace
        self.source = source
        self.source_owner_email = source_owner_email


class AvroSchemaCompatibilityRequest(AvroSchemaRequestBase):

    def __init__(self, schema, namespace, source):
        super(AvroSchemaCompatibilityRequest, self).__init__(schema=schema)
        self.namespace = namespace
        self.source = source


class MysqlSchemaCompatibilityRequest(RequestBase):

    def __init__(self, mysql_statements, namespace, source):
        super(MysqlSchemaCompatibilityRequest, self).__init__()
        self.mysql_statements = mysql_statements
        self.namespace = namespace
        self.source = source
