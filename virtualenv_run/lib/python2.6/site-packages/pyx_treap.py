#!/usr/bin/env python

'''
Provides a treap module - specifically, the class that describes the overall treap, not the nodes.

A treap is a datastructure that's kind of half way between a heap and a binary search tree
'''

## Editor width - you must be at least this tall to enjoy this ride ################################################################

# Appears to be an excellent reference on treaps in java:
# http://users.cis.fiu.edu/~weiss/dsaajava/Code/DataStructures
# However, I'm not certain the remove method is correct
#
# Another nice reference on treaps in java:
# https://blog.eldslott.org/2009/04/25/treap-implementation-in-java/
# It doesn't seem to mention removals, but it shows rotations nicely
#
# A pretty complete-looking java implementation of treaps:
# http://www.nada.kth.se/~snilsson/public/code/treap/source/Treap.java
# It has what looks like a nice remove.  However, I wound up using something based on Weiss's code with two additional if's instead.



import sys
#mport copy
import math
#mport time
#mport functools
import pyx_treap_node as treap_node

# Each element is stored in a node in a tree, and each node contains an integer and two references, one to the left
# subtree and one to the right subtree.

# We need to increase the recurision limit to fend off the randomization trolls from causing an over-deep recursion
# In practice, this'll be very unlikely, extremely unlikely, unless the client of this package chooses a
# small priority_size and saves a large number of values
MIN_HEAP_SIZE = 100000
if sys.getrecursionlimit() < MIN_HEAP_SIZE:
	sys.setrecursionlimit(MIN_HEAP_SIZE)


def pad_to(string, length):
	'''Pad a string to a specified length'''
	return string + '_' * (length - len(string) - 1) + ' '


def center(string, field_use_width, field_avail_width):
	'''Center a string within a given field width'''
	field_use = (string + '_' * (field_use_width - 1))[:field_use_width - 1]
	pad_width = (field_avail_width - len(field_use)) / 2.0
	result = ' ' * int(pad_width) + field_use + ' ' * int(math.ceil(pad_width))
	return result


# this is the public portion
class treap:
	'''The treap class - or rather, the non-node, treap-proper'''
	def __init__(self):
		self.root = None
		self.length = 0

	# __bool__ is the python 3 name of the special method, while __nonzero__ is the python 2 name
	def __bool__(self):
		if self.root is None:
			return False
		else:
			return True

	__nonzero__ = __bool__

	def __len__(self):
		return self.length

	def insert(self, key, value, priority=None):
		'''Insert a node in the treap'''
		if self.root is None:
			self.root = treap_node.treap_node()
			self.root.key = key
			self.root.value = value
			if priority:
				self.root.priority = priority

			self.length = 1
		else:
			(length_delta, self.root) = self.root.insert(self.root, key, value, priority)
			assert length_delta in [ 0, 1 ]
			self.length += length_delta

	__setitem__ = insert

	def remove(self, key):
		'''Remove a node from the treap'''
		if self.root != None:
			(found, self.root) = self.root.remove(self.root, key)
			if found:
				self.length -= 1
			else:
				raise KeyError
		else:
			raise KeyError

	__delitem__ = remove

	def remove_min(self):
		'''Remove the lowest node in the treap'''
		if self.root != None:
			(self.root, result) = self.root.remove_min(self.root)
			if not (result is None):
				self.length -= 1
			return result
		else:
			raise KeyError

	def remove_max(self):
		'''Remove the largest node in the treap'''
		if self.root != None:
			(self.root, result) = self.root.remove_max(self.root)
			if not (result is None):
				self.length -= 1
			return result
		else:
			raise KeyError

	def find(self, key):
		'''Look up a node in the treap'''
		current = self.root

		while True:
			if current is None:
				raise KeyError
			elif key < current.key:
				current = current.left
			elif key > current.key:
				current = current.right
			else:
				# equal
				return current.value

	__getitem__ = find

	def find_min(self):
		'''Find the lowest node in the treap'''
		current = self.root

		if current is None:
			raise KeyError

		while current.left != None:
			current = current.left

		return current.key

	def find_max(self):
		'''Find the highest node in the treap'''
		current = self.root

		if current is None:
			raise KeyError

		while current.right != None:
			current = current.right

		return current.key

	def inorder_traversal(self, visit):
		'''Perform an inorder traversal of the treap'''
		if self.root != None:
			self.root.inorder_traversal(visit)

	def detailed_inorder_traversal(self, visit):
		'''Perform an inorder traversal of the treap, passing a little more detail to the visit function at each step'''
		if self.root != None:
			self.root.detailed_inorder_traversal(visit)

	def check_tree_invariant(self):
		'''Check the tree invariant'''
		if self.root is None:
			return True
		else:
			return self.root.check_tree_invariant()

	def check_heap_invariant(self):
		'''Check the heap invariant'''
		if self.root is None:
			return True
		else:
			return self.root.check_heap_invariant()

	def depth(self):
		'''Return the depth of the treap (tree)'''
		class maxer:
			'''Class facilitates computing the maximum depth of all the treap (tree) branches'''
			def __init__(self, maximum=-1):
				self.max = maximum

			def feed(self, node, key, value, depth, from_left):
				# pylint: disable=R0913
				# R0913: We need a bunch of arguments
				'''Check our maximum so far against the current node - updating as needed'''
				dummy = node
				dummy = key
				dummy = value
				dummy = from_left
				if depth > self.max:
					self.max = depth

			def result(self):
				'''Return the maximum we've found - plus one for human readability'''
				return self.max + 1

		max_obj = maxer()
		self.detailed_inorder_traversal(max_obj.feed)
		return max_obj.result()

	def _depth_and_field_width(self):
		'''Compute the depth of the tree and the maximum width within the nodes - for pretty printing'''
		class maxer:
			'''Class facilitates computing the max depth of the treap (tree) and max width of the nodes'''
			def __init__(self, maximum=-1):
				self.depth_max = maximum
				self.field_width_max = -1

			def feed(self, node, key, value, depth, from_left):
				'''Check our maximums so far against the current node - updating as needed'''
				# pylint: disable=R0913
				# R0913: We need a bunch of arguments
				dummy = key
				dummy = value
				dummy = from_left
				if depth > self.depth_max:
					self.depth_max = depth
				str_node = str(node)
				len_key = len(str_node)
				if len_key > self.field_width_max:
					self.field_width_max = len_key

			def result(self):
				'''Return the maximums we've computed'''
				return (self.depth_max + 1, self.field_width_max)

		max_obj = maxer()
		self.detailed_inorder_traversal(max_obj.feed)
		return max_obj.result()

	def __str__(self):
		'''Format a treap as a string'''
		class Desc:
			# pylint: disable=R0903
			# R0903: We don't need a lot of public methods
			'''Build a pretty-print string during a recursive traversal'''
			def __init__(self, pretree):
				self.tree = pretree

			def update(self, node, key, value, depth, from_left):
				'''Add a node to the tree'''
				# pylint: disable=R0913
				# R0913: We need a bunch of arguments
				dummy = key
				dummy = value
				self.tree[depth][from_left] = str(node)

		if self.root is None:
			# empty output for an empty tree
			return ''
		else:
			pretree = []
			depth, field_use_width = self._depth_and_field_width()
			field_use_width += 1
			for level in xrange(depth):
				string = '_' * (field_use_width - 1)
				pretree.append([ string ] * 2 ** level)
			desc = Desc(pretree)
			self.root.detailed_inorder_traversal(desc.update)
			result = []
			widest = 2 ** (depth - 1) * field_use_width
			for level in xrange(depth):
				two_level = 2 ** level
				field_avail_width = widest / two_level
				string = ''.join([ center(x, field_use_width, field_avail_width) for x in desc.tree[level] ])
				# this really isn't useful for more than dozen values or so, and that without priorities printed
				result.append(('%2d ' % level) + string)
			return '\n'.join(result)

	class state_class:
		# pylint: disable=R0903
		# R0903: We don't need a lot of public methods
		'''A state class, used for iterating over the nodes in a treap'''
		def __init__(self, todo, node):
			self.todo = todo
			self.node = node

		def __repr__(self):
			return '%s %s' % (self.todo, self.node)



	# These three things: keys, values, items; are a bit of a cheat.  In Python 2, they're really supposed to return lists,
	# but we return iterators like python 3.  A better implementation would check what version of python we're targetting,
	# give this behavior for python 3, and coerce the value returned to a list for python 2.
	def iterkeys(self):
		'''A macro for iterators - produces keys, values and items from almost the same code'''
		stack = [ self.state_class('L', self.root) ]

		while stack:
			state = stack.pop()
			if state.node != None:
				if state.todo == 'V':
					# yield state.node.key
					yield state.node.key
				else:
					if state.node.right != None:
						stack.append(self.state_class('R', state.node.right))
					stack.append(self.state_class('V', state.node))
					if state.node.left != None:
						stack.append(self.state_class('L', state.node.left))

	keys = iterator = __iter__ = iterkeys

	def itervalues(self):
		'''A macro for iterators - produces keys, values and items from almost the same code'''
		stack = [ self.state_class('L', self.root) ]

		while stack:
			state = stack.pop()
			if state.node != None:
				if state.todo == 'V':
					# yield state.node.key
					yield state.node.value
				else:
					if state.node.right != None:
						stack.append(self.state_class('R', state.node.right))
					stack.append(self.state_class('V', state.node))
					if state.node.left != None:
						stack.append(self.state_class('L', state.node.left))

	values = itervalues

	def iteritems(self):
		'''A macro for iterators - produces keys, values and items from almost the same code'''
		stack = [ self.state_class('L', self.root) ]

		while stack:
			state = stack.pop()
			if state.node != None:
				if state.todo == 'V':
					# yield state.node.key
					yield state.node.key, state.node.value
				else:
					if state.node.right != None:
						stack.append(self.state_class('R', state.node.right))
					stack.append(self.state_class('V', state.node))
					if state.node.left != None:
						stack.append(self.state_class('L', state.node.left))

	items = iteritems

	def reverse_iterator(self):
		'''Iterate over the nodes of the treap in reverse order'''
		stack = [ self.state_class('L', self.root) ]

		while stack:
			state = stack.pop()
			if state.node != None:
				if state.todo == 'V':
					yield state.node.key
				else:
					if state.node.left != None:
						stack.append(self.state_class('L', state.node.left))
					stack.append(self.state_class('V', state.node))
					if state.node.right != None:
						stack.append(self.state_class('R', state.node.right))

