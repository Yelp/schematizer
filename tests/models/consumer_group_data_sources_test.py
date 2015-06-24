# -*- coding: utf-8 -*-
import pytest

from testing import factories
from tests.models.testing_db import DBTestCase


class TestConsumerGroupDataSourcesModel(DBTestCase):

    @classmethod
    def fake_data_source_type(self):
        return "test data source type"

    @classmethod
    def fake_data_source_id(self):
        return 35

    @pytest.fixture
    def fake_data_target(self):
        return factories.DataTargetFactory.create_in_db(
            "target",
            "destination"
        )

    @pytest.fixture
    def fake_consumer_group(self, fake_data_target):
        return factories.ConsumerGroupFactory.create_in_db(
            "group_name",
            "group_type",
            fake_data_target
        )

    @pytest.fixture
    def fake_consumer_group_data_sources(self, fake_consumer_group):
        return factories.ConsumerGroupDataSourcesFactory.create_in_db(
            fake_consumer_group,
            self.fake_data_source_type,
            self.fake_data_source_id
        )

    def test_create_consumer_group_data_sources(
        self,
        fake_consumer_group_data_sources,
        fake_consumer_group
    ):
        assert fake_consumer_group_data_sources.consumer_group_id == \
            fake_consumer_group.id
        assert fake_consumer_group_data_sources.data_source_type == \
            self.fake_data_source_type
        assert fake_consumer_group_data_sources.data_source_id == \
            self.fake_data_source_id
