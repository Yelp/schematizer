# -*- coding: utf-8 -*-
import pytest

from testing import factories
from tests.models.testing_db import DBTestCase


class TestDataTargetModel(DBTestCase):

    @property
    def fake_target_type(self):
        return "test target type"

    @property
    def fake_destination(self):
        return "fake destination"

    @pytest.fixture
    def fake_data_target(self):
        return factories.DataTargetFactory.create_in_db(
            self.fake_target_type,
            self.fake_destination
        )

    def test_create_data_target(self, fake_data_target):
        assert fake_data_target.target_type == self.fake_target_type
        assert fake_data_target.destination == self.fake_destination
