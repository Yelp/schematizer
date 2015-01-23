# -*- coding: utf-8 -*-

import math
import random
import time

import treap

from uwsgi_metrics.snapshot import Snapshot


# By default, store 1028 elements in the reservoir.  This offers a 99.9%
# confidence level with a 5% margin of error assuming a normal distribution,
# and an alpha factor of 0.015, which heavily biases the reservoir to the past
# 5 minutes of measurements.
DEFAULT_SIZE = 1028
DEFAULT_ALPHA = 0.015

# Rescale the priorities (keys) every hour.
RESCALE_THRESHOLD_S = 60 * 60


class Reservoir(object):
    """An exponentially-decaying random reservoir of integers. Uses Cormode et
    al's forward-decaying priority reservoir sampling method to produce a
    statistically representative sampling reservoir, exponentially biased
    towards newer entries.

    See http://dimacs.rutgers.edu/~graham/pubs/papers/fwddecay.pdf
    "Cormode et al. Forward Decay: A Practical Time Decay Model for Streaming
    Systems. ICDE '09: Proceedings of the 2009 IEEE International Conference on
    Data Engineering (2009)"

    Translated from ExponentiallyDecayingReservoir.java
    """

    def __init__(self, unit=None, size=DEFAULT_SIZE,
                 alpha=DEFAULT_ALPHA):
        """Create a new exponentially-decaying random reservoir.

        :param size: the number of samples to keep in the sampling reservoir
        :param alpha: the exponential decay factor; the higher this is, the
            more biased the reservoir will be towards newer values.
        """

        self.unit = unit
        self.size = size
        self.alpha = alpha
        self.start_time = self.current_time_in_fractional_seconds()
        self.next_scale_time = self.start_time + RESCALE_THRESHOLD_S
        self.values = treap.treap()

    def current_time_in_fractional_seconds(self):
        return time.time()

    def weight(self, t):
        return math.exp(self.alpha * t)

    def update(self, value, timestamp=None):
        """Add a value to the reservoir.

        :param value: the value to be added
        :param timestamp: the epoch timestamp of the value in seconds, defaults
            to the current timestamp if not specified.
        """

        if timestamp is None:
            timestamp = self.current_time_in_fractional_seconds()
        self.rescale_if_needed()
        priority = self.weight(timestamp - self.start_time) / random.random()
        self.values[priority] = value
        if len(self.values) > self.size:
            self.values.remove_min()

    def rescale_if_needed(self):
        now = self.current_time_in_fractional_seconds()
        if now >= self.next_scale_time:
            self.rescale(now)

    def rescale(self, now):
        """ "A common feature of the above techniques—indeed, the key technique
        that allows us to track the decayed weights efficiently—is that they
        maintain counts and other quantities based on g(ti − L), and only scale
        by g(t − L) at query time. But while g(ti −L)/g(t−L) is guaranteed to
        lie between zero and one, the intermediate values of g(ti − L) could
        become very large. For polynomial functions, these values should not
        grow too large, and should be effectively represented in practice by
        floating point values without loss of precision. For exponential
        functions, these values could grow quite large as new values of
        (ti − L) become large, and potentially exceed the capacity of common
        floating point types. However, since the values stored by the
        algorithms are linear combinations of g values (scaled sums), they
        can be rescaled relative to a new landmark. That is, by the analysis
        of exponential decay in Section VI-A, the choice of L does not affect
        the final result. We can therefore multiply each value based on L by a
        factor of exp(−α(L′ − L)), and obtain the correct value as if we had
        instead computed relative to a new landmark L′ (and then use this new
        L′ at query time). This can be done with a linear pass over whatever
        data structure is being used."
        """

        old_start_time = self.start_time
        self.start_time = self.current_time_in_fractional_seconds()
        self.next_scale_time = now + RESCALE_THRESHOLD_S
        new_values = treap.treap()
        for key in self.values.keys():
            new_key = key * math.exp(
                -self.alpha * (self.start_time - old_start_time))
            new_values[new_key] = self.values[key]
        self.values = new_values

    def get_snapshot(self):
        return Snapshot(self.values.values())
