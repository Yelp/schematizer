# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

def datetime_to_local_ISO_8601(datetime):
    return datetime.isoformat() + 'Z'
