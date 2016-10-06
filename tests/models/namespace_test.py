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

class TestSourcesRelatedToNamespace(DBTestCase):
    @pytest.fixture
    def namespace_name(self):
        return 'foo'

    @pytest.fixture
    def namespace(self, namespace_name):
        return factories.create_namespace(namespace_name)


    @pytest.fixture
    def namespace_no_sources(self):
        return factories.create_namespace('non_sources')

    @pytest.fixture
    def sources(self, namespace_name):
        return [
            factories.create_source(namespace_name, 'source1'),
            factories.create_source(namespace_name, 'source2')
        ]

    def test_happy_case(self, namespace):
        info = PageInfo(min_id=0,count=1)
        sources = namespace.get_sources(page_info= info)
        assert len(sources) == 1
        assert sources[0].name == 'source1'
        new_min_id = sources[0].id + 1
        new_info = PageInfo(min_id=new_min_id)
        sources = namespace.get_sources(page_info=new_info)
        assert len(sources) == 1
        assert sources[0].name == 'source2'

    def test_non_related_sources(self, namespace_no_sources):
        sources = namespace_no_sources.get_sources()
        assert len(sources) == 0


