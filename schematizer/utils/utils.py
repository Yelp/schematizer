# -*- coding: utf-8 -*-
# Copyright 2016 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
from __future__ import absolute_import
from __future__ import unicode_literals

import inspect


def get_current_func_arg_name_values():
    """Return a dict of {parameter name: value} of current function.
    """
    caller_frame = inspect.currentframe().f_back
    arg_info = inspect.getargvalues(caller_frame)

    params = {
        key: arg_info.locals[key] for key in arg_info.args if key != 'self'
    }
    if arg_info.varargs:
        params[arg_info.varargs] = arg_info.locals[arg_info.varargs]
    if arg_info.keywords:
        params.update(arg_info.locals[arg_info.keywords])
    return params
