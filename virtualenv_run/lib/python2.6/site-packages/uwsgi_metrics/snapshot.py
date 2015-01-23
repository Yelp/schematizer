import math


class Snapshot(object):
    """A snapshot of a reservoir state.

    Translated from Snapshot.java
    """

    def __init__(self, values=None):
        if values is None:
            self.values = []
        else:
            self.values = sorted(values)

    def get_value(self, quantile):
        if quantile < 0.0 or quantile > 1.0:
            raise ValueError(quantile + " is not in [0..1]")

        if len(self.values) == 0:
            return 0.0

        pos = quantile * (len(self.values) + 1)

        if pos < 1:
            return self.values[0]

        if pos >= len(self.values):
            return self.values[-1]

        lower = self.values[int(pos) - 1]
        upper = self.values[int(pos)]
        return lower + (pos - math.floor(pos)) * (upper - lower)

    def get_median(self):
        return self.get_value(0.5)

    def get_75th_percentile(self):
        return self.get_value(0.75)

    def get_95th_percentile(self):
        return self.get_value(0.95)

    def get_98th_percentile(self):
        return self.get_value(0.98)

    def get_99th_percentile(self):
        return self.get_value(0.99)

    def get_999th_percentile(self):
        return self.get_value(0.999)

    def get_mean(self):
        if len(self.values) == 0:
            return 0.0
        return sum(self.values) / float(len(self.values))

    def get_std_dev(self):
        # Two-pass algorithm for variance, avoids numeric overflow

        if len(self.values) <= 1:
            return 0.0

        mean = self.get_mean()
        sum_ = 0

        for value in self.values:
            diff = value - mean
            sum_ += diff * diff

        variance = sum_ / (len(self.values) - 1.0)
        return math.sqrt(variance)

    def get_min(self):
        if len(self.values) == 0:
            return 0.0
        return self.values[0]

    def get_max(self):
        if len(self.values) == 0:
            return 0.0
        return self.values[-1]

    def size(self):
        return len(self.values)

    def view(self):
        result = {
            'max': self.get_max(),
            'mean': self.get_mean(),
            'min': self.get_min(),
            'p50': self.get_median(),
            'p75': self.get_75th_percentile(),
            'p95': self.get_95th_percentile(),
            'p98': self.get_98th_percentile(),
            'p99': self.get_99th_percentile(),
            'p999': self.get_999th_percentile(),
            'stddev': self.get_std_dev(),
        }
        return result
