# -*- coding: utf-8 -*-
from datetime import datetime
import pytest

from schematizer import models
from schematizer.models.database import session
from schematizer.models.domain import Domain
from tests.models.testing_db import DBTestCase


class DomainFactory(object):

    @classmethod
    def create_domain_db_object(
            cls,
            namespace='yelp',
            source='business',
            owner_email='business@yelp.com',
            created_at=datetime.now(),
            updated_at=datetime.now()
    ):
        domain = Domain(
            namespace=namespace,
            source=source,
            owner_email=owner_email,
            created_at=created_at,
            updated_at=updated_at
        )
        session.add(domain)
        session.flush()
        return domain

    @classmethod
    def create_domain_object(
            cls,
            namespace='yelp',
            source='business',
            owner_email='business@yelp.com',
            created_at=datetime.now(),
            updated_at=datetime.now()
    ):
        return Domain(
            namespace=namespace,
            source=source,
            owner_email=owner_email,
            created_at=created_at,
            updated_at=updated_at
        )


class TestDomainModel(DBTestCase):

    def test_timestamp_field(self):
        created_at = datetime.now()
        domain = DomainFactory.create_domain_db_object(
            namespace='yelp',
            source='business',
            owner_email='business@yelp.com',
            created_at=created_at
        )
        domain_result = session.query(Domain).filter(
            Domain.id == domain.id,
        ).one()
        assert isinstance(domain_result.created_at, datetime)
        assert domain_result.created_at == created_at
        assert domain_result.namespace == domain.namespace
        assert domain_result.source == domain.source

    @pytest.fixture
    def default_domain(self):
        return DomainFactory.create_domain_db_object()

    def test_get_source_by_namespace_and_source_name(self, default_domain):
        domain_result = models.domain.get_source_by_namespace_and_source_name(
            default_domain.namespace,
            default_domain.source
        )
        assert default_domain == domain_result

    def test_list_all_namespaces(self, default_domain):
        namespaces = models.domain.list_all_namespaces()
        assert [default_domain.namespace] == namespaces

    def test_list_sources_by_namespace(self, default_domain):
        domain_list = models.domain.list_sources_by_namespace(
            default_domain.namespace
        )
        assert [default_domain] == domain_list

    def test_list_all_sources(self, default_domain):
        domain_list = models.domain.list_all_sources()
        assert [default_domain] == domain_list

    def test_get_source_by_source_id(self, default_domain):
        domain_result = models.domain.get_source_by_source_id(
            default_domain.id
        )
        assert default_domain == domain_result
