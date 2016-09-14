# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.data_target import DataTarget
from schematizer_testing import factories
from tests.models.base_model_test import GetAllModelTestBase


class TestGetAllDataTargets(GetAllModelTestBase):

    def create_data_target(self, data_target_no):
        return factories.create_data_target(
            target_type='my_target_type',
            destination='destination_{}'.format(data_target_no)
        )

    entity_model = DataTarget
    create_entity_func = create_data_target
    assert_func_name = 'assert_equal_data_target'
