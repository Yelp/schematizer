
# This file is not pylint'd, because we often don't have pyx_treap and/or pyx_treap_node during testing

try:
	from pyx_duptreap import *
except ImportError:
	from py_duptreap import *

