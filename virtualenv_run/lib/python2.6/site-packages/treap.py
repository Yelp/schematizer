
# This file is not pylint'd, because we often don't have pyx_treap and/or pyx_treap_node during testing

'''Import pyx_treap if available, otherwise import py_treap'''

try:
	from pyx_treap import *
except ImportError:
	from py_treap import *

