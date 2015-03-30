# -*- coding: utf-8 -*-
import abc

from schematizer import models


class BaseConverter(object):

    source_type = models.SchemaKindEnum.Unknown
    target_type = models.SchemaKindEnum.Unknown

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def convert(self, src_schema):
        """Convert the given source schema to the target schema type."""
        raise NotImplementedError()
