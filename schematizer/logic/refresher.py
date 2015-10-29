# -*- coding: utf-8 -*-
from schematizer import models
from schematizer.models.database import session


def create_refresh_info(table_identifier, refresh_status):
    refresh_info = models.RefreshInfo(
        table_identifier=table_identifier,
        refresh_status=refresh_status,
    )
    session.add(refresh_info)
    session.flush()
    return refresh_info


def update_refresh_info(table_identifier, refresh_status):
    return session.query(
        models.RefreshInfo
    ).filter(
        models.RefreshInfo.table_identifier == table_identifier
    ).update(
        {
            models.RefreshInfo.refresh_status: refresh_status
        }
    )


def get_refresh_info_by_table(table_identifier):
    return session.query(
        models.RefreshInfo
    ).filter(
        models.RefreshInfo.table_identifier == table_identifier
    ).first()


def list_incomplete_refreshes():
    return session.query(
        models.RefreshInfo
    ).filter(
        models.RefreshInfo.refresh_status != 0
    ).all()
