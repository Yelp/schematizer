# -*- coding: utf-8 -*-
"""
A simple view that returns the happy hour times
"""
import pyramid.exceptions
from pyramid.view import view_config


@view_config(route_name='api.hours', renderer='json')
def hours(request):
    # Extract the biz_id from the matched path dictionary.
    biz_id = request.matchdict.get('biz_id')

    # Format a simple response payload that will be rendered as JSON.
    if int(biz_id) == 1234:
        response = {
            'SUN': (None, None),
            'MON': (16, 18),
            'TUE': (16, 18),
            'WED': (16, 18),
            'THU': (16, 18),
            'FRI': (15, 19),
            'SAT': (None, None),
        }
    else:
        raise pyramid.exceptions.NotFound('%s not found' % biz_id)

    return response
