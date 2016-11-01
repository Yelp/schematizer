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

from pyramid import httpexceptions


LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE = 'Latest schema is not found.'
LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE = 'Latest topic is not found.'
SCHEMA_NOT_FOUND_ERROR_MESSAGE = 'Schema is not found.'
SOURCE_NOT_FOUND_ERROR_MESSAGE = 'Source is not found.'
TOPIC_NOT_FOUND_ERROR_MESSAGE = 'Topic is not found.'
INVALID_AVRO_SCHEMA_ERROR = 'Invalid Avro schema.'
INVALID_REQUEST_ERROR = 'Invalid request.'
NOTE_NOT_FOUND_ERROR_MESSAGE = 'Note is not found.'
REFERENCE_NOT_FOUND_ERROR_MESSAGE = 'Reference object not found'
CATEGORY_NOT_FOUND_ERROR_MESSAGE = 'Category not found for the given source'
RESTRICTED_CHAR_ERROR_MESSAGE = (
    'Source name or Namespace name should not contain the '
    'restricted character: |'
)
NUMERIC_NAME_ERROR_MESSAGE = 'Source or Namespace name should not be numeric'
REFRESH_NOT_FOUND_ERROR_MESSAGE = 'Refresh not found for the given refresh id'
ENTITY_NOT_FOUND_ERROR = 'Entity not found.'
UNSUPPORTED_TARGET_SCHEMA_MESSAGE = 'Desired target schema type is unsupported'
EMPTY_SRC_NAME_ERROR = 'Source name must be non-empty.'


def invalid_schema_exception(err_message=INVALID_AVRO_SCHEMA_ERROR):
    return httpexceptions.exception_response(422, detail=err_message)


def empty_src_name_exception(err_message=EMPTY_SRC_NAME_ERROR):
    return httpexceptions.exception_response(422, detail=err_message)


def entity_not_found_exception(err_message=ENTITY_NOT_FOUND_ERROR):
    return httpexceptions.exception_response(404, detail=err_message)


def schema_not_found_exception(err_message=SCHEMA_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def source_not_found_exception(err_message=SOURCE_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def topic_not_found_exception(err_message=TOPIC_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def latest_topic_not_found_exception(
    err_message=LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE
):
    return httpexceptions.exception_response(404, detail=err_message)


def latest_schema_not_found_exception(
    err_message=LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE
):
    return httpexceptions.exception_response(404, detail=err_message)


def invalid_request_exception(err_message=INVALID_REQUEST_ERROR):
    return httpexceptions.exception_response(400, detail=err_message)


def note_not_found_exception(err_message=NOTE_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def reference_not_found_exception(
    err_message=REFERENCE_NOT_FOUND_ERROR_MESSAGE
):
    return httpexceptions.exception_response(404, detail=err_message)


def category_not_found_exception(err_message=CATEGORY_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def restricted_char_exception(
        err_message=RESTRICTED_CHAR_ERROR_MESSAGE
):
    return httpexceptions.exception_response(400, detail=err_message)


def numeric_name_exception(
        err_message=NUMERIC_NAME_ERROR_MESSAGE
):
    return httpexceptions.exception_response(400, detail=err_message)


def refresh_not_found_exception(
        err_message=REFRESH_NOT_FOUND_ERROR_MESSAGE
):
    return httpexceptions.exception_response(404, detail=err_message)


def unsupported_target_schema_exception(
    err_message=UNSUPPORTED_TARGET_SCHEMA_MESSAGE
):
    return httpexceptions.exception_response(501, detail=err_message)
