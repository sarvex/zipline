from toolz import partition_all


def compute_date_range_chunks(sessions, start_date, end_date, chunksize):
    """Compute the start and end dates to run a pipeline for.

    Parameters
    ----------
    sessions : DatetimeIndex
        The available dates.
    start_date : pd.Timestamp
        The first date in the pipeline.
    end_date : pd.Timestamp
        The last date in the pipeline.
    chunksize : int or None
        The size of the chunks to run. Setting this to None returns one chunk.

    Returns
    -------
    ranges : iterable[(np.datetime64, np.datetime64)]
        A sequence of start and end dates to run the pipeline for.
    """
    if start_date not in sessions:
        raise KeyError(
            f'Start date {start_date.strftime("%Y-%m-%d")} is not found in calendar.'
        )
    if end_date not in sessions:
        raise KeyError(
            f'End date {end_date.strftime("%Y-%m-%d")} is not found in calendar.'
        )
    if end_date < start_date:
        raise ValueError(
            f'End date {end_date.strftime("%Y-%m-%d")} cannot precede start date {start_date.strftime("%Y-%m-%d")}.'
        )

    if chunksize is None:
        return [(start_date, end_date)]

    start_ix, end_ix = sessions.slice_locs(start_date, end_date)
    return (
        (r[0], r[-1]) for r in partition_all(
            chunksize, sessions[start_ix:end_ix]
        )
    )
