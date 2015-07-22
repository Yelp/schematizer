# -*- coding: utf-8 -*-
from pyramid import httpexceptions


LATEST_SCHEMA_NOT_FOUND_ERROR_MESSAGE = 'Latest schema is not found.'
LATEST_TOPIC_NOT_FOUND_ERROR_MESSAGE = 'Latest topic is not found.'
NAMESPACE_NOT_FOUND_ERROR_MESSAGE = 'Namespace is not found.'
SCHEMA_NOT_FOUND_ERROR_MESSAGE = 'Schema is not found.'
SOURCE_NOT_FOUND_ERROR_MESSAGE = 'Source is not found.'
TOPIC_NOT_FOUND_ERROR_MESSAGE = 'Topic is not found.'
INVALID_AVRO_SCHEMA_ERROR = 'Invalid Avro schema.'
INVALID_REQUEST_ERROR = 'Invalid request.'
NOTE_NOT_FOUND_ERROR_MESSAGE = 'Note is not found.'
REFERENCE_NOT_FOUND_ERROR_MESSAGE = 'Reference object not found'


def invalid_schema_exception(err_message=INVALID_AVRO_SCHEMA_ERROR):
    return httpexceptions.exception_response(422, detail=err_message)


def schema_not_found_exception(err_message=SCHEMA_NOT_FOUND_ERROR_MESSAGE):
    return httpexceptions.exception_response(404, detail=err_message)


def namespace_not_found_exception(
    err_message=NAMESPACE_NOT_FOUND_ERROR_MESSAGE
):
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
