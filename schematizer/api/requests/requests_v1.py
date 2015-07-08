# -*- coding: utf-8 -*-
import simplejson
from cached_property import cached_property


class RequestBase(object):

    @classmethod
    def create_from_string(cls, request_body_string):
        request_json = simplejson.loads(request_body_string)
        return cls(**request_json)


class RegisterSchemaRequest(RequestBase):

    def __init__(self, schema, namespace, source, source_owner_email,
                 base_schema_id=None):
        super(RegisterSchemaRequest, self).__init__()
        self.schema = schema
        self.namespace = namespace
        self.source = source
        self.source_owner_email = source_owner_email
        self.base_schema_id = base_schema_id

    @cached_property
    def schema_json(self):
        return simplejson.loads(self.schema) if self.schema else None


class RegisterSchemaFromMySqlRequest(RequestBase):

    def __init__(
        self,
        new_create_table_stmt,
        namespace,
        source,
        source_owner_email,
        old_create_table_stmt=None,
        alter_table_stmt=None
    ):
        super(RegisterSchemaFromMySqlRequest, self).__init__()
        self.new_create_table_stmt = new_create_table_stmt
        self.old_create_table_stmt = old_create_table_stmt
        self.alter_table_stmt = alter_table_stmt
        self.namespace = namespace
        self.source = source
        self.source_owner_email = source_owner_email


class AvroSchemaCompatibilityRequest(RequestBase):

    def __init__(self, schema, namespace, source):
        super(AvroSchemaCompatibilityRequest, self).__init__()
        self.schema = schema
        self.namespace = namespace
        self.source = source

    @cached_property
    def schema_json(self):
        return simplejson.loads(self.schema) if self.schema else None


class MysqlSchemaCompatibilityRequest(RequestBase):

    def __init__(
        self,
        new_create_table_stmt,
        namespace,
        source,
        old_create_table_stmt=None,
        alter_table_stmt=None
    ):
        super(MysqlSchemaCompatibilityRequest, self).__init__()
        self.new_create_table_stmt = new_create_table_stmt
        self.old_create_table_stmt = old_create_table_stmt
        self.alter_table_stmt = alter_table_stmt
        self.namespace = namespace
        self.source = source
