# -*- coding: utf-8 -*-
from pyramid.view import view_config

from schematizer.api.decorators import transform_response
from schematizer.api.exceptions import exceptions_v1
from schematizer.api.requests import requests_v1
from schematizer.logic import doc_tool
from schematizer.logic import schema_repository
from schematizer.models.note import ReferenceTypeEnum


@view_config(
    route_name='api.v1.create_note',
    request_method='POST',
    renderer='json'
)
@transform_response()
def create_note(request):
    try:
        req = requests_v1.CreateNoteRequest(**request.json_body)
        return doc_tool.create_note(
            reference_id=req.reference_id,
            reference_type=req.reference_type,
            note_text=req.note,
            last_updated_by=req.last_updated_by
        ).to_dict()
    except Exception as e:
        raise exceptions_v1.invalid_request_exception(e.message)


def assert_reference_exists(reference_id, reference_type):
    if (
        reference_type == ReferenceTypeEnum.SCHEMA and
        schema_repository.get_schema_by_id(reference_id) is not None
    ) or (
        reference_type == ReferenceTypeEnum.SCHEMA_ELEMENT and
        schema_repository.get_schema_element_by_id(reference_id) is not None
    ):
        return
    raise exceptions_v1.reference_not_found_exception()


@view_config(
    route_name='api.v1.update_note',
    request_method='POST',
    renderer='json'
)
@transform_response()
def update_note(request):
    req = requests_v1.UpdateNoteRequest(**request.json_body)
    note_id = request.matchdict.get('note_id')
    note = doc_tool.get_note_by_id(note_id)
    # Raise an exception if the note cannot be found
    if note is None:
        raise exceptions_v1.note_not_found_exception()
    doc_tool.update_note(
        note_id=note_id,
        note_text=req.note,
        last_updated_by=req.last_updated_by
    )
    return note
