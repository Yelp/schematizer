# -*- coding: utf-8 -*-
from datetime import datetime

from schematizer.models.database import session
from schematizer.models.domain import Domain
from tests.models.testing_db import DBTestCase


class TestModels(DBTestCase):

    def test_timestamp_field(self):
        created_at = datetime.now()
        domain = Domain(
            namespace='yelp',
            source='business',
            owner_email='business@yelp.com',
            created_at=created_at
        )
        session.add(domain)
        session.flush()
        domain_result = session.query(Domain).filter(
            Domain.id == domain.id,
        ).one()
        assert isinstance(domain_result.created_at, datetime)
        assert domain_result.created_at == created_at
        assert domain_result.namespace == domain.namespace
        assert domain_result.source == domain.source
