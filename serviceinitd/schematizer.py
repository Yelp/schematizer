# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import unicode_literals

from wsgiref.simple_server import make_server

from schematizer.webapp import create_application


app = create_application()
server = make_server('0.0.0.0', 8888, app)
server.serve_forever()
