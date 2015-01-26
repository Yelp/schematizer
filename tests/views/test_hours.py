# -*- coding: utf-8 -*-
from example_happyhour.views import hours


def test_hours_by_biz_id(dummy_request):
    dummy_request.matchdict = {'biz_id': '1234'}
    response = hours.hours(dummy_request)
    assert sorted(response.keys()) == \
        ['FRI', 'MON', 'SAT', 'SUN', 'THU', 'TUE', 'WED']
    assert response['SUN'] == (None, None)
    assert response['MON'] == (16, 18)
    assert response['SAT'] == (None, None)