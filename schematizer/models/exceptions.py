# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class EntityNotFoundError(Exception):

    def __init__(self, entity_cls=None, entity_desc=None, **extra_info):
        entity_desc = entity_desc or entity_cls.__name__
        err_message = '{} not found.'.format(entity_desc)
        if extra_info:
            err_message = '{} {}'.format(
                err_message,
                ', '.join('{}={}'.format(k, v) for k, v in extra_info.items())
            )
        super(EntityNotFoundError, self).__init__(err_message)


class InvalidTopicClusterTypeError(Exception):

    def __init__(self, cluster_type):
        err_message = '`{}` kafka cluster type does not exist.'.format(
            cluster_type
        )
        super(InvalidTopicClusterTypeError, self).__init__(err_message)
