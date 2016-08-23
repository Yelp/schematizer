# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.namespace import Namespace
from testing import asserts
from testing import factories
from tests.models.base_model_test import GetAllModelTestBase
from tests.models.testing_db import DBTestCase


class TestGetAllNamespaces(GetAllModelTestBase):

    def create_namespace(self, namespace_no):
        return factories.create_namespace('namespace_{}'.format(namespace_no))

    entity_model = Namespace
    create_entity_func = create_namespace
    assert_func_name = 'assert_equal_namespace'


class TestGetNamespaceById(DBTestCase):

    @pytest.fixture
    def namespace_foo(self):
        return factories.create_namespace('foo')

    def test_happy_case(self, namespace_foo):
        actual = Namespace.get_by_id(namespace_foo.id)
        asserts.assert_equal_namespace(actual, expected=namespace_foo)

    def test_non_existed_namespace(self):
        with pytest.raises(EntityNotFoundError):
            Namespace.get_by_id(obj_id=0)


class TestGetNamespaceByName(DBTestCase):

    @pytest.fixture
    def namespace_foo(self):
        return factories.create_namespace('foo')

    def test_happy_case(self, namespace_foo):
        actual = Namespace.get_by_name(namespace_foo.name)
        asserts.assert_equal_namespace(actual, expected=namespace_foo)

    def test_non_existed_namespace(self):
        with pytest.raises(EntityNotFoundError):
            Namespace.get_by_name(name='bad namespace')
