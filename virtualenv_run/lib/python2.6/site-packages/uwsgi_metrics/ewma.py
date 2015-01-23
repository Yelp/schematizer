from math import exp


class EWMA(object):
    """An exponentially-weighted moving average.

    See:
    * UNIX Load Average Part 1: How It Works
      http://www.teamquest.com/pdfs/whitepaper/ldavg1.pdf
    * UNIX Load Average Part 2: Not Your Average Average
      http://www.teamquest.com/pdfs/whitepaper/ldavg2.pdf
    * http://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average

    Translated from EWMA.java
    """

    TICK_INTERVAL_S = 5
    SECONDS_PER_MINUTE = 60.0
    ONE_MINUTE = 1
    FIVE_MINUTES = 5
    FIFTEEN_MINUTES = 15
    M1_ALPHA = 1 - exp(-TICK_INTERVAL_S / SECONDS_PER_MINUTE / ONE_MINUTE)
    M5_ALPHA = 1 - exp(-TICK_INTERVAL_S / SECONDS_PER_MINUTE / FIVE_MINUTES)
    M15_ALPHA = 1 - exp(
        -TICK_INTERVAL_S / SECONDS_PER_MINUTE / FIFTEEN_MINUTES)

    @classmethod
    def one_minute_EWMA(cls):
        """Creates a new EWMA which is equivalent to the UNIX one minute load
        average and which expects to be ticked every 5 seconds.
        """

        return cls(cls.M1_ALPHA, cls.TICK_INTERVAL_S)

    @classmethod
    def five_minute_EWMA(cls):
        """Creates a new EWMA which is equivalent to the UNIX five minute load
        average and which expects to be ticked every 5 seconds.
        """

        return cls(cls.M5_ALPHA, cls.TICK_INTERVAL_S)

    @classmethod
    def fifteen_minute_EWMA(cls):
        """Creates a new EWMA which is equivalent to the UNIX fifteen minute
        load average and which expects to be ticked every 5 seconds.
        """

        return cls(cls.M15_ALPHA, cls.TICK_INTERVAL_S)

    def __init__(self, alpha, tick_interval_s):
        self.alpha = alpha
        self.tick_interval_s = tick_interval_s
        self.initialized = False
        self.rate = 0.0
        self.count = 0

    def update(self, n):
        """Update the moving average with a new value."""
        self.count += n

    def tick(self):
        """Mark the passage of time and decay the current rate accordingly."""
        instant_rate = self.count / float(self.tick_interval_s)
        self.count = 0
        if self.initialized:
            self.rate += (self.alpha * (instant_rate - self.rate))
        else:
            self.rate = instant_rate
            self.initialized = True

    def get_rate(self):
        """Get the per-second rate."""
        return self.rate
