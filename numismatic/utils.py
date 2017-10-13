"""Utility functions"""

def date_range(start_date, end_date, delta):
    """Calculates date range by substracting a delta
    value from end_date until we reach start_date"""

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
