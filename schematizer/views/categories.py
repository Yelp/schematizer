# -*- coding: utf-8 -*-
from pyramid.view import view_config

from schematizer.logic import doc_tool


@view_config(
    route_name='api.v1.list_categories',
    request_method='GET',
    renderer='json'
)
def list_categories(request):
    categories = doc_tool.get_source_categories()
    # Remove duplicates and return categories as a list
    return sorted(list(set(category.category for category in categories)))
