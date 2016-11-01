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

from pyramid.view import view_config

from schematizer.api.decorators import log_api
from schematizer.api.decorators import transform_api_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.api.responses import responses_v1
from schematizer.logic import doc_tool
from schematizer.logic import schema_repository
from schematizer.models.note import ReferenceTypeEnum


@view_config(
    route_name='api.v1.create_note',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def create_note(request):
    req = requests_v1.CreateNoteRequest(**request.json_body)
    assert_reference_exists(req.reference_type, req.reference_id)
    note = doc_tool.create_note(
        reference_type=req.reference_type,
        reference_id=req.reference_id,
        note_text=req.note,
        last_updated_by=req.last_updated_by
    )
    return responses_v1.get_note_response_from_note(note)


def assert_reference_exists(reference_type, reference_id):
    """Checks to make sure that the reference for this note exists.
    If it does not, raise an exception
    """
    if (
        reference_type == ReferenceTypeEnum.SCHEMA and
        schema_repository.get_schema_by_id(reference_id) is not None
    ) or (
        reference_type == ReferenceTypeEnum.SCHEMA_ELEMENT and
        schema_repository.get_schema_element_by_id(reference_id) is not None
    ):
        # Valid. Do nothing
        return
    raise exceptions_v1.reference_not_found_exception()


@view_config(
    route_name='api.v1.update_note',
    request_method='POST',
    renderer='json'
)
@transform_api_response()
@log_api()
def update_note(request):
    req = requests_v1.UpdateNoteRequest(**request.json_body)
    note_id_str = request.matchdict.get('note_id')
    note_id = int(note_id_str)
    note = doc_tool.get_note_by_id(note_id)
    # Raise an exception if the note cannot be found
    if note is None:
        raise exceptions_v1.note_not_found_exception()
    doc_tool.update_note(
        id=note_id,
        note_text=req.note,
        last_updated_by=req.last_updated_by
    )
    return responses_v1.get_note_response_from_note(note)
