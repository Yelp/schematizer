# -*- coding: utf-8 -*-
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
