# -*- coding: utf-8 -*-
from datetime import datetime

from schematizer.models.database import session
from schematizer.models.domain import Domain
from testing import factories
from tests.models.testing_db import DBTestCase


class TestDomainModel(DBTestCase):

    def test_timestamp_field(self):
        domain = factories.DomainFactory.create_in_db(
            namespace='yelp',
            source='business'
        )
        domain_result = session.query(Domain).filter(
            Domain.id == domain.id,
        ).one()
        assert isinstance(domain_result.created_at, datetime)
        assert domain_result.created_at == factories.fake_created_at
        assert domain_result.namespace == domain.namespace
        assert domain_result.source == domain.source
