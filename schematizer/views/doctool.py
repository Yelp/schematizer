# -*- coding: utf-8 -*-
from pyramid.view import view_config


@view_config(
    route_name='doctool.index',
    request_method='GET',
    renderer='schematizer:templates/index.mako'
)
def doctool_index(request):
    return {'test_mako': 'TEST MAKO'}
