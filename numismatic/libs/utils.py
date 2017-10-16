"""Utility functions"""

import math
from datetime import datetime, timedelta

def date_range(start_date, end_date, **freq):
    """Calculates date range by substracting a delta
    value from end_date until we reach start_date"""

    delta = timedelta(**freq)
    dates = []

    while end_date > start_date:
        dates.insert(0, end_date)
        end_date -= delta   

    dates.insert(0, start_date)
    return dates

def to_datetime(datelike, origin=None):
    """Converts a datelike object to python date
    type, else raises TypeError"""

    if datelike is None:
        return datetime.now()
    elif isinstance(datelike, str):
        return parse(datelike)
    else:
        raise TypeError(f'{datelike}')

def make_list_str(items):
    """Converts a string to a list"""
    if isinstance(items, str):
        items = items.split(',')
    return ','.join(items)

def dates_and_frequencies(start_date, end_date, freq):
    """Converts dates to python dates (if necessary)
    Returns dates as well as frequency string and interval"""
    freqmap = dict(d='days', h='hours', m='minutes', s='seconds',
                    ms='milliseconds', us='microseconds')
    freqstr = freqmap[freq]
    end_date = to_datetime(end_date)
    if isinstance(start_date, int):
        start_date = end_date + timedelta(**{freqstr:start_date})
    else:
        start_date = to_datetime(start_date)
    interval_time = timedelta(**{freqstr:1})
    intervals = math.ceil((end_date-start_date)/interval_time)
    return start_date, end_date, freqstr, intervals
