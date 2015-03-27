# -*- coding: utf-8 -*-
from schematizer.views import hello


def test_with_arguments(dummy_request):
    dummy_request.matchdict = {'name': 'Darwin'}
    response = hello.hello(dummy_request)
    assert response == {'message': 'Hello Darwin!'}
