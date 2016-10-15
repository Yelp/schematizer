# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models.exceptions import EntityNotFoundError
from schematizer.models.namespace import Namespace
from schematizer.models.page_info import PageInfo
from schematizer_testing import asserts
from schematizer_testing import factories
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


class TestGetSourcesByNamespace(DBTestCase):

    @pytest.fixture
    def namespace_name(self):
        return 'foo'

    @pytest.fixture
    def namespace(self, namespace_name):
        return factories.create_namespace(namespace_name)

    @pytest.fixture
    def sources(self, namespace_name):
        return [
            factories.create_source(namespace_name, 'source1'),
            factories.create_source(namespace_name, 'source2')
        ]

    def test_filter_by_count(self, namespace, sources):
        info = PageInfo(count=1)
        actual = namespace.get_sources(page_info=info)
        asserts.assert_equal_entity_list(
            actual, sources[0:1], asserts.assert_equal_source
        )

    def test_filter_by_min_id(self, namespace, sources):
        min_id = sources[0].id + 1
        info = PageInfo(min_id=min_id)
        actual = namespace.get_sources(page_info=info)
        asserts.assert_equal_source(actual[0], sources[1])

    def test_no_source(self, namespace):
        actual = namespace.get_sources()
        assert len(actual) == 0
