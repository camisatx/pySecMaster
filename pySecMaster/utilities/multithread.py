from multiprocessing import Pool


def multithread(function, items, threads=4):
    """ Takes the main function to run in parallel, inputs the variable(s)
    and returns the results.

    :param function: The main function to process in parallel.
    :param items: A list of strings that are passed into the function for
    each thread.
    :param threads: The number of threads to use. The default is 4, but
    the threads are not CPU core bound.
    :return: The results of the function passed into this function.
    """

    """The async variant, which submits all processes at once and
    retrieve the results as soon as they are done."""
    pool = Pool(threads)
    output = [pool.apply_async(function, args=(item,)) for item in items]
    results = [p.get() for p in output]
    pool.close()
    pool.join()

    return results
