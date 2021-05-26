"""Module consists of multiprocessing decorator

The function provided in this module is a decorator which can be used easily in connection with
pandas apply function or similar parallelizable functions.

Example:
    @dec_multiprocess
    def your_function(x):
        return x+2

    # create a new data frame column by using multiprocess starmap function as a decorator
    df['y'] = your_function(df['x'])

Note:
    This module does not use the python internal multiprocessing package. Instead it uses a fork called multiprocess
    due to the fact that this fork provides a handling to pickle functions which then get called inside the
    decorator. This handling will lead to errors in the original package.
"""

from functools import wraps, partial
import multiprocess as mp
import logging


def dec_multiprocess(func):
    """Decorator to apply multiprocess

    With this decorator multiprocessing is applicable via a simple decoration.
    The only thing that has to be provided is an iterable as function input defined in a
    dictionary.
    """

    @wraps(func)
    def wrapper(*iterable, **kwargs):
        logging.getLogger('b2b_logger')
        logging.info(f'Start using muliprocess for {func.__name__}')

        # use partial from functools to bind kwargs to function. The resulting
        # function is uset in pool.starmap
        p_func = partial(func, **kwargs)

        # start multiprocessing
        pool = mp.Pool(mp.cpu_count())
        erg = pool.starmap(p_func, zip(*iterable))
        pool.close()
        pool.join()

        return erg

    return wrapper

