# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class PageInfo(object):
    """This class holds the information of the pagination.

    Args:
        count (Optional[int]): maximum number of entries to return.
        min_id (Optional[int]): return entities of which the id is equal to or
            greater than this id.
    """

    def __init__(self, count=0, min_id=0):
        self.count = count
        self.min_id = min_id
