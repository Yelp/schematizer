from uwsgi_metrics.reservoir import Reservoir


class Histogram(object):
    """A metric which calculates the distribution of a value.

    Translated from Histogram.java
    """

    def __init__(self):
        self.count = 0
        self.reservoir = Reservoir()

    def update(self, value):
        self.count += 1
        self.reservoir.update(value)

    def get_count(self):
        return self.count

    def get_snapshot(self):
        return self.reservoir.get_snapshot()

    def view(self):
        result = self.get_snapshot().view()
        result['count'] = self.get_count()
        return result
