# -*- coding: utf-8 -*-
import simplejson


class RequestBase(object):

    @classmethod
    def create_from_string(cls, request_body_string):
        request_json = simplejson.loads(request_body_string)
        return cls(**request_json)


class RegisterSchemaRequest(RequestBase):

    def __init__(self, schema, namespace, source, source_owner_email,
                 base_schema_id=None):
        super(RegisterSchemaRequest, self).__init__()
        if isinstance(schema, basestring):
            self.schema = simplejson.loads(schema)
        else:
            self.schema = schema
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


class AvroSchemaCompatibilityRequest(RequestBase):

    def __init__(self, schema, namespace, source):
        super(AvroSchemaCompatibilityRequest, self).__init__()
        self.schema = schema
        self.namespace = namespace
        self.source = source

    @property
    def schema_json(self):
        return simplejson.loads(self.schema) if self.schema else None


class MysqlSchemaCompatibilityRequest(RequestBase):

    def __init__(self, mysql_statements, namespace, source):
        super(MysqlSchemaCompatibilityRequest, self).__init__()
        self.mysql_statements = mysql_statements
        self.namespace = namespace
        self.source = source
