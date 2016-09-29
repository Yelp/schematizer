# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer.models.data_target import DataTarget
from schematizer.models.exceptions import EntityNotFoundError
from schematizer_testing import asserts
from schematizer_testing import factories
from tests.models.base_model_test import GetAllModelTestBase
from tests.models.testing_db import DBTestCase


class TestGetAllDataTargets(GetAllModelTestBase):

    def create_data_target(self, data_target_no):
        return factories.create_data_target(
            name='data_target_{}'.format(data_target_no),
            target_type='my_target_type',
            destination='destination_{}'.format(data_target_no)
        )

    entity_model = DataTarget
    create_entity_func = create_data_target
    assert_func_name = 'assert_equal_data_target'


class TestGetDataTargetByName(DBTestCase):

    @pytest.fixture
    def data_target_foo(self):
        return factories.create_data_target(
            'foo',
            'redshift',
            'foo_destination'
        )

    def test_happy_case(self, data_target_foo):
        actual = DataTarget.get_by_name(data_target_foo.name)
        asserts.assert_equal_namespace(actual, expected=data_target_foo)

    def test_non_existed_data_target(self):
        with pytest.raises(EntityNotFoundError):
            DataTarget.get_by_name(name='bad data target')
