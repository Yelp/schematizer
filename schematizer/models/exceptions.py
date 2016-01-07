# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals


class EntityNotFoundError(Exception):

    def __init__(self, entity_cls, message=None, **extra_info):
        self._entity_cls = entity_cls

        err_message = '{} not found.'.format(entity_cls.__name__)
        if message:
            err_message = message
        if extra_info:
            err_message = '{} {}'.format(
                err_message,
                ', '.join('{}={}'.format(k, v) for k, v in extra_info.items())
            )
        super(EntityNotFoundError, self).__init__(err_message)

    @property
    def entity_type(self):
        return self._entity_cls
