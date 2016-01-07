# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

import pytest

from schematizer import models
from schematizer.models import exceptions as sch_exc
from schematizer.models.database import session
from testing import asserts
from testing import factories
from tests.models.testing_db import DBTestCase


class TestGetModelById(DBTestCase):

    @pytest.fixture
    def dw_data_target(self):
        return factories.create_data_target(
            target_type='redshift',
            destination='dwv1.redshift.yelpcorp.com'
        )

    @pytest.fixture
    def dw_consumer_group(self, dw_data_target):
        return factories.create_consumer_group('dw', dw_data_target)

    @pytest.fixture
    def dw_consumer_group_data_source(self, dw_consumer_group, biz_source):
        return factories.create_consumer_group_data_source(
            dw_consumer_group,
            data_src_type=models.DataSourceTypeEnum.SOURCE,
            data_src_id=biz_source.id
        )

    def test_get_data_target_by_id(self, dw_data_target):
        actual = models.DataTarget.get_by_id(session, dw_data_target.id)
        asserts.assert_equal_data_target(actual, dw_data_target)

    def test_get_consumer_group_by_id(self, dw_consumer_group):
        actual = models.ConsumerGroup.get_by_id(session, dw_consumer_group.id)
        asserts.assert_equal_consumer_group(actual, dw_consumer_group)

    def test_get_consumer_group_data_source_by_id(
        self,
        dw_consumer_group_data_source
    ):
        actual = models.ConsumerGroupDataSource.get_by_id(
            session,
            dw_consumer_group_data_source.id
        )
        asserts.assert_equal_consumer_group_data_source(
            actual,
            dw_consumer_group_data_source
        )

    @pytest.mark.parametrize('model_cls', [
        models.DataTarget,
        models.ConsumerGroup,
        models.ConsumerGroupDataSource
    ])
    def test_get_invalid_id(self, model_cls):
        with pytest.raises(sch_exc.EntityNotFoundError):
            model_cls.get_by_id(session, 0)
