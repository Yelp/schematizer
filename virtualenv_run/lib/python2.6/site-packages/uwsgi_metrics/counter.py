class Counter(object):
    """An incrementing and decrementing counter metric.

    Translated from Counter.java
    """

    def __init__(self):
        self.count = 0

    def inc(self, n=1):
        """Increment the counter."""
        self.count += n

    def dec(self, n=1):
        """Decrement the counter."""
        self.count -= n

    def get_count(self):
        """Get the counter's current value."""
        return self.count

    def view(self):
        return {
            'count': self.get_count(),
        }
