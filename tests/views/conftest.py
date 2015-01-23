# -*- coding: utf-8 -*-
import pytest

from pyramid.testing import DummyRequest


@pytest.fixture
def dummy_request():
    return DummyRequest()
