"""
This module provides utility functions for performance measurement and logging.

Functions
---------
timeit(func)
"""

import time

from loguru import logger


def timeit(func):
    """
    A decorator that measures the execution time of a function and logs the duration.

    Parameters
    ----------
    func : callable
        The function to be wrapped and timed.

    Returns
    -------
    callable
        The wrapped function with added timing functionality.

    Notes
    -----
    The execution time is logged using the `logger.debug` method.
    """

    def wrapped(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        end = time.time()
        duration = end - start
        logger.debug(f"Function '{func.__name__}' executed in {duration:f} s")
        return result

    return wrapped
