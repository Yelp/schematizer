# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from schematizer.models.connections.default_connection import DefaultConnection


def get_connection_obj():
    return DefaultConnection()
