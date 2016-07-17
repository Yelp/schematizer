# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.namespace import Namespace
from testing import asserts
from testing import factories
from tests.models.testing_db import DBTestCase


class TestGetAll(DBTestCase):

    @pytest.fixture
    def namespace_foo(self):
        return factories.create_namespace('foo')

    def test_get_all_namespaces(self, namespace_foo):
        actual = Namespace.get_all()
        asserts.assert_equal_entity_set(
            actual_set=set(actual),
            expected_set={namespace_foo},
            assert_func=asserts.assert_equal_namespace,
            id_attr='id'
        )

    def test_get_consumer_group_by_id(self):
        actual = Namespace.get_all()
        assert actual == []


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
