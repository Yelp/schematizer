# -*- coding: utf-8 -*-
import pytest

from testing import factories
from tests.models.testing_db import DBTestCase


class TestConsumerGroupModel(DBTestCase):

    @property
    def fake_group_name(self):
        return "test group name"

    @property
    def fake_group_type(self):
        return "test group type"

    @pytest.fixture
    def fake_data_target(self):
        return factories.DataTargetFactory.create_in_db(
            "target",
            "destination"
        )

    @pytest.fixture
    def fake_consumer_group(self, fake_data_target):
        return factories.ConsumerGroupFactory.create_in_db(
            self.fake_group_name,
            self.fake_group_type,
            fake_data_target
        )

    def test_create_consumer_group(
        self,
        fake_data_target,
        fake_consumer_group
    ):
        assert fake_consumer_group.group_name == self.fake_group_name
        assert fake_consumer_group.group_type == self.fake_group_type
        assert fake_consumer_group.data_target_id == fake_data_target.id
