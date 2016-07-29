# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from datetime import datetime

import simplejson
from cached_property import cached_property

from schematizer.models.tuples import PageInfo


class RequestBase(object):

    @classmethod
    def create_from_string(cls, request_body_string):
        request_json = simplejson.loads(request_body_string)
        return cls(**request_json)

    @classmethod
    def _get_datetime(cls, request_timestamp):
        if request_timestamp is None:
            return None, None

        long_timestamp = long(request_timestamp)
        return long_timestamp, datetime.utcfromtimestamp(long_timestamp)


class RegisterSchemaRequest(RequestBase):

    def __init__(
        self,
        schema,
        namespace,
        source,
        source_owner_email,
        contains_pii=False,
        base_schema_id=None
    ):
        super(RegisterSchemaRequest, self).__init__()
        self.schema = schema
        self.namespace = namespace
        self.source = source
        self.source_owner_email = source_owner_email
        self.base_schema_id = base_schema_id
        self.contains_pii = contains_pii

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
        contains_pii=False,
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
        self.contains_pii = contains_pii


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


class CreateNoteRequest(RequestBase):

    def __init__(self, reference_id, reference_type, note, last_updated_by):
        super(CreateNoteRequest, self).__init__()
        self.reference_id = reference_id
        self.reference_type = reference_type
        self.note = note
        self.last_updated_by = last_updated_by


class UpdateNoteRequest(RequestBase):

    def __init__(self, note, last_updated_by):
        super(UpdateNoteRequest, self).__init__()
        self.note = note
        self.last_updated_by = last_updated_by


class UpdateCategoryRequest(RequestBase):

    def __init__(self, category):
        super(UpdateCategoryRequest, self).__init__()
        self.category = category


class GetSchemasRequest(RequestBase):

    def __init__(self, query_params):
        super(GetSchemasRequest, self).__init__()
        self.created_after, self.created_after_datetime = self._get_datetime(
            query_params.get('created_after')
        )
        self.page_info = PageInfo(
            query_params.get('count'),
            query_params.get('min_id')
        )
        self.include_disabled = query_params.get('include_disabled')


class GetTopicsRequest(RequestBase):

    def __init__(self, query_params):
        super(GetTopicsRequest, self).__init__()
        self.namespace = query_params.get('namespace')
        self.source = query_params.get('source')
        self.created_after, self.created_after_datetime = self._get_datetime(
            query_params.get('created_after')
        )
        self.page_info = PageInfo(
            query_params.get('count'),
            query_params.get('min_id')
        )


class GetRefreshesRequest(RequestBase):

    def __init__(self, query_params):
        super(GetRefreshesRequest, self).__init__()
        self.namespace = query_params.get('namespace')
        self.status = query_params.get('status')
        self.created_after, self.created_after_datetime = self._get_datetime(
            query_params.get('created_after')
        )


class CreateRefreshRequest(RequestBase):

    def __init__(
            self,
            offset=None,
            batch_size=None,
            priority=None,
            filter_condition=None,
            avg_rows_per_second_cap=None
    ):
        super(CreateRefreshRequest, self).__init__()
        self.offset = offset
        self.batch_size = batch_size
        self.priority = priority
        self.filter_condition = filter_condition
        self.avg_rows_per_second_cap = avg_rows_per_second_cap


class UpdateRefreshStatusRequest(RequestBase):

    def __init__(self, status, offset):
        super(UpdateRefreshStatusRequest, self).__init__()
        self.status = status
        self.offset = offset


class CreateDataTargetRequest(RequestBase):

    def __init__(self, target_type, destination):
        super(CreateDataTargetRequest, self).__init__()
        self.target_type = target_type
        self.destination = destination


class CreateConsumerGroupRequest(RequestBase):

    def __init__(self, group_name):
        super(CreateConsumerGroupRequest, self).__init__()
        self.group_name = group_name


class GetTopicsByDataTargetIdRequest(RequestBase):

    def __init__(self, query_params):
        super(GetTopicsByDataTargetIdRequest, self).__init__()
        self.created_after, self.created_after_datetime = self._get_datetime(
            query_params.get('created_after')
        )


class CreateConsumerGroupDataSourceRequest(RequestBase):

    def __init__(self, data_source_type, data_source_id):
        super(CreateConsumerGroupDataSourceRequest, self).__init__()
        self.data_source_type = data_source_type
        self.data_source_id = data_source_id
