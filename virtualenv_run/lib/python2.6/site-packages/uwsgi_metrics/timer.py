from uwsgi_metrics.meter import Meter
from uwsgi_metrics.histogram import Histogram


class Timer(object):
    """A timer metric which aggregates timing durations and provides duration
    statistics, plus throughput statistics via a Meter.

    Translated from Timer.java
    """

    def __init__(self, duration_units):
        self.meter = Meter()
        self.histogram = Histogram()
        self.duration_units = duration_units

    def view(self):
        snapshot = self.get_snapshot()
        result = {
            'count': self.meter.get_count(),
            'max': snapshot.get_max(),
            'mean': snapshot.get_mean(),
            'min': snapshot.get_min(),
            'p50': snapshot.get_median(),
            'p75': snapshot.get_75th_percentile(),
            'p95': snapshot.get_95th_percentile(),
            'p98': snapshot.get_98th_percentile(),
            'p99': snapshot.get_99th_percentile(),
            'p999': snapshot.get_999th_percentile(),
            'stddev': snapshot.get_std_dev(),
            'm15_rate': self.meter.get_fifteen_minute_rate(),
            'm1_rate': self.meter.get_one_minute_rate(),
            'm5_rate': self.meter.get_five_minute_rate(),
            'mean_rate': self.meter.get_mean_rate(),
            'duration_units': self.duration_units,
            'rate_units': 'calls/second'
            }
        return result

    def update(self, duration):
        """Add a recorded duration."""
        if duration >= 0:
            self.histogram.update(duration)
            self.meter.mark()

    def get_count(self):
        return self.histogram.get_count()

    def get_fifteen_minute_rate(self):
        return self.meter.get_fifteen_minute_rate()

    def get_five_minute_rate(self):
        return self.meter.get_five_minute_rate()

    def get_one_minute_rate(self):
        return self.meter.get_one_minute_rate()

    def get_mean_rate(self):
        return self.meter.get_mean_rate()

    def get_snapshot(self):
        return self.histogram.get_snapshot()
