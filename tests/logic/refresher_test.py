import pytest

from schematizer import models
from schematizer.logic import refresher
from testing import factories
from tests.models.testing_db import DBTestCase


class TestRefresher(DBTestCase):

    @property
    def table_identifier(self):
        return "db_table"

    @property
    def refresh_status(self):
        return 0

    @property
    def incomplete_status(self):
        return 500

    @pytest.fixture
    def refresh_info(self):
        return factories.create_refresh_info(
            self.table_identifier,
            self.refresh_status
        )

    @pytest.yield_fixture
    def multiple_refresh_info(self):
        factories.create_refresh_info(
            "db1_table1",
            self.incomplete_status
        )
        factories.create_refresh_info(
            "db2_table2",
            self.refresh_status
        )
        factories.create_refresh_info(
            "db3_table3",
            self.incomplete_status
        )
        yield

    def test_create_refresh_info(self):
        actual_info = refresher.create_refresh_info(
            self.table_identifier,
            self.refresh_status
        )
        expected_info = models.RefreshInfo(
            table_identifier=self.table_identifier,
            refresh_status=self.refresh_status
        )
        self.assert_equal_refresh_info_partial(expected_info, actual_info)

    def test_get_refresh_info_by_table(self, refresh_info):
        result_refresh_info = refresher.get_refresh_info_by_table(
            self.table_identifier
        )
        self.assert_equal_refresh_info(refresh_info, result_refresh_info)

    def test_update_refresh_info(self, refresh_info):
        new_status = 100
        refresher.update_refresh_info(
            self.table_identifier,
            new_status
        )
        actual_info = refresher.get_refresh_info_by_table(
            self.table_identifier
        )
        expected_info = models.RefreshInfo(
            table_identifier=self.table_identifier,
            refresh_status=new_status
        )
        self.assert_equal_refresh_info_partial(expected_info, actual_info)

    def test_list_incomplete_refreshes(self, multiple_refresh_info):
        incomplete_refreshes = refresher.list_incomplete_refreshes()
        first_expected_info = models.RefreshInfo(
            table_identifier="db1_table1",
            refresh_status=self.incomplete_status
        )
        second_expected_info = models.RefreshInfo(
            table_identifier="db3_table3",
            refresh_status=self.incomplete_status
        )
        assert len(incomplete_refreshes) == 2
        self.assert_equal_refresh_info_partial(
            first_expected_info,
            incomplete_refreshes[0]
        )
        self.assert_equal_refresh_info_partial(
            second_expected_info,
            incomplete_refreshes[1]
        )

    def assert_equal_refresh_info(self, expected, actual):
        assert expected.id == actual.id
        assert expected.last_refreshed_at == actual.last_refreshed_at
        self.assert_equal_refresh_info_partial(expected, actual)

    def assert_equal_refresh_info_partial(self, expected, actual):
        assert expected.table_identifier == actual.table_identifier
        assert expected.refresh_status == actual.refresh_status
