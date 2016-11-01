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

from schematizer.utils import utils


class TestCurrentFuncParamNameAndValue(object):

    def test_func_with_no_param(self):

        def func():
            return utils.get_current_func_arg_name_values()

        actual = func()
        assert actual == {}

    def test_func_with_required_param_only(self):

        def func(arg1):
            return utils.get_current_func_arg_name_values()

        actual = func(10)
        assert actual == self._stringfy_keys({'arg1': 10})

    def test_func_with_required_and_optional_params(self):

        def func(arg1, arg2=None):
            return utils.get_current_func_arg_name_values()

        actual = func(10, 20)
        assert actual == self._stringfy_keys({'arg1': 10, 'arg2': 20})

        actual = func(10, arg2=20)
        assert actual == self._stringfy_keys({'arg1': 10, 'arg2': 20})

    def test_func_with_unnamed_params(self):

        def func(arg1, *nums):
            return utils.get_current_func_arg_name_values()

        actual = func(10)
        assert actual == self._stringfy_keys({'arg1': 10, 'nums': ()})

        actual = func(10, 'a', 'b')
        assert actual == self._stringfy_keys({'arg1': 10, 'nums': ('a', 'b')})

    def test_func_with_keyword_params(self):

        def func(arg1, **kv):
            return utils.get_current_func_arg_name_values()

        actual = func(10)
        assert actual == self._stringfy_keys({'arg1': 10})

        actual = func(10, a=1, b=2)
        assert actual == self._stringfy_keys({'arg1': 10, 'a': 1, 'b': 2})

    def _stringfy_keys(self, arg_dict):
        return {str(key): value for key, value in arg_dict.items()}
