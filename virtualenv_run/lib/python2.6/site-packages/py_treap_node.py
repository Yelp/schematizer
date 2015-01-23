
'''A class is provided to contain a single node of a treap - multiple instances for multiple nodes of course'''

# actually, it's a little faster with the standard random module than the lcgrng module; although random is a more time
# consuming algorithm, it's coded in C.
import random as random_module
#import lcgrng as random_module

# FIXME: nice for debugging, bad for real life
#random_module.seed(0)

PRIORITY_SIZE = 0x7fffffff


# this is all "hands off" to a client of the module


class treap_node:
	'''Hold a single node of a treap'''






	def __init__(self):
		self.priority = int(random_module.random() * PRIORITY_SIZE)
		self.key = None
		self.value = None
		self.left = None
		self.right = None

	def check_tree_invariant(self):
		'''Check the tree invariant'''
		if self.left != None:
			assert self.key > self.left.key
			assert self.left.check_tree_invariant()
		if self.right != None:
			assert self.key < self.right.key
			assert self.right.check_tree_invariant()
		return True

	def check_heap_invariant(self):
		'''Check the heap invariant'''
		# I kinda thought it was supposed to be <, but clearly that won't work with random priorities
		if self.left != None:
			assert self.priority <= self.left.priority
			assert self.left.check_heap_invariant()
		if self.right != None:
			assert self.priority <= self.right.priority
			assert self.right.check_heap_invariant()
		return True

	def check_invariants(self):
		'''Check the tree and heap invariants'''
		assert self.check_tree_invariant()
		assert self.check_heap_invariant()
		return True

	def insert(self, node, key, value, priority):
		'''Insert a node - just call the fast version'''
		return self.pyx_insert(node, key, value, priority)

	def pyx_insert(self, node, key, value, priority):
		'''Insert a node - this is the fast version'''
		# We arbitrarily ditch duplicate values, but I believe we could just save them in a list.
		# We probably should have a series of classes via a class factory that sets class variables to
		# distinguish the priority max and whether we store duplicates.
		if node is None:
			# adding a node, increasing the treap length by 1
			node = treap_node()
			if priority:
				node.priority = priority
			node.key = key
			node.value = value
			return (1, node)
		elif key < node.key:
			(length_delta, node.left) = self.pyx_insert(node.left, key, value, priority)
			if node.left.priority < node.priority:
				node = self.rotate_with_left_child(node)
			return (length_delta, node)
		elif key > node.key:
			(length_delta, node.right) = self.pyx_insert(node.right, key, value, priority)
			if node.right.priority < node.priority:
				node = self.rotate_with_right_child(node)
			return (length_delta, node)
		else:
			# must be equal - replacing a node - does not change the treap length
			node.key = key
			node.value = value
			return (0, node)

	def remove(self, node, key):
		'''Remove a node - just call the fast version'''
		return self.pyx_remove(node, key)

	def pyx_remove(self, node, key):
		'''Remove a node - this is the fast version'''
		found = False
		if node != None:
			if key < node.key:
				(found, node.left) = self.pyx_remove(node.left, key)
			elif key > node.key:
				(found, node.right) = self.pyx_remove(node.right, key)
			else:
				# Match found
				# these two tests for emptiness don't appear to be in http://users.cis.fiu.edu/~weiss/dsaajava/Code/DataStructures
				if node.left is None:
					return (True, node.right)
				if node.right is None:
					return (True, node.left)
				if node.left.priority < node.right.priority:
					node = self.rotate_with_left_child(node)
				else:
					node = self.rotate_with_right_child(node)

				# Continue on down
				if node != None:
					(found, node) = self.pyx_remove(node, key)
				else:
					# At a leaf
					node.left = None
		return (found, node)

	def remove_min(self, node):
		'''Remove the lowest node below us'''
		if not (node is None):
			if not (node.left is None):
				(node.left, result) = self.remove_min(node.left)
			else:
				# Minimum found
				return (node.right, (node.key, node.value))
		return (node, result)

	def remove_max(self, node):
		'''Remove the highest node below us'''
		if not (node is None):
			if not (node.right is None):
				(node.right, result) = self.remove_max(node.right)
			else:
				# maximum found
				return (node.left, (node.key, node.value))
		return (node, result)

	def rotate_with_left_child(self, node):
		# pylint: disable=R0201
		# R0201: Cython (Mar 28, 2011) doesn't like decorators on cdef's , so we disable the "method could be a function" warning
		'''This is a treap thing - rotate to rebalance'''
		temp = node.left
		node.left = temp.right
		temp.right = node
		node = temp
		return node

	def rotate_with_right_child(self, node):
		# pylint: disable=R0201
		# R0201: Cython (Mar 28, 2011) doesn't like decorators on cdef's , so we disable the "method could be a function" warning
		'''This is a treap thing - rotate to rebalance'''
		temp = node.right
		node.right = temp.left
		temp.left = node
		node = temp
		return node

	def detailed_inorder_traversal(self, visit, depth=0, from_left=0):
		'''Do an inorder traversal - with lots of parameters'''
		if self.left != None:
			self.left.detailed_inorder_traversal(visit, depth + 1, from_left * 2)
		visit(self, self.key, self.value, depth, from_left)
		if self.right != None:
			self.right.detailed_inorder_traversal(visit, depth + 1, from_left * 2 + 1)

	def inorder_traversal(self, visit):
		'''Do an inorder traversal - without lots of parameters'''
		if self.left != None:
			self.left.inorder_traversal(visit)
		visit(self.key, self.value)
		if self.right != None:
			self.right.inorder_traversal(visit)

	def __str__(self):
		return '%s/%s/%s' % (self.key, self.priority, self.value)
		#return '%s/%s' % (self.key, self.value)

