# -*- coding: utf-8 -*-
import logging


log = logging.getLogger('schematizer.config')


def routes(config):
    """Add routes to the configuration."""
    config.add_route('api.hours', '/hours/{biz_id}')
