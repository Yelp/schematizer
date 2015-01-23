#!/usr/bin/env python

try:
	import psyco
except:
	pass
else:
	psyco.full()

class nlowest:
	def __init__(self, n, dups=False):
		self.n = n
		if dups:
			import duptreap
			self.treap = duptreap.duptreap()
		else:
			import treap
			self.treap = treap.treap()
			
		self.__iter__ = self.treap.iterator
		self.have_evicted = False

	def add(self, value):
		if self.have_evicted and value < self.evicted or not self.have_evicted:
			self.treap[value] = 0
			if len(self.treap) > self.n:
				(self.evicted, zero) = self.treap.remove_max()
				self.have_evicted = True

class nhighest:
	def __init__(self, n, dups=False):
		self.n = n
		if dups:
			import duptreap
			self.treap = duptreap.duptreap()
		else:
			import treap
			self.treap = treap.treap()
		self.__iter__ = self.treap.reverse_iterator
		self.have_evicted = False

	def add(self, value):
		if self.have_evicted and value > self.evicted or not self.have_evicted:
			self.treap[value] = 0
			if len(self.treap) > self.n:
				(self.evicted, zero) = self.treap.remove_min()
				self.have_evicted = True

