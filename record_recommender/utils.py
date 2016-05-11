
"""Helper functions."""

from __future__ import absolute_import, print_function

import time
from datetime import date, datetime, timedelta


def get_week_dates(year, week, as_timestamp=False):
    """
    Get the dates or timestamp of a week in a year.

    param year: The year.
    param week: The week.
    param as_timestamp: Return values as timestamps.
    returns: The begin and end of the week as datetime.date or as timestamp.
    """
    year = int(year)
    week = int(week)
    start_date = date(year, 1, 1)
    if start_date.weekday() > 3:
        start_date = start_date + timedelta(7 - start_date.weekday())
    else:
        start_date = start_date - timedelta(start_date.weekday())
    dlt = timedelta(days=(week-1)*7)
    start = start_date + dlt
    end = start_date + dlt + timedelta(days=6)
    if as_timestamp:
        # Add the complete day
        one_day = timedelta(days=1).total_seconds() - 0.000001
        end_timestamp = time.mktime(end.timetuple()) + one_day
        return time.mktime(start.timetuple()), end_timestamp
    return start, end


def get_year_week(timestamp):
    """Get the year and week for a given timestamp."""
    time_ = datetime.fromtimestamp(timestamp)
    year = time_.isocalendar()[0]
    week = time_.isocalendar()[1]
    return year, week


def get_last_weeks(number_of_weeks):
    """Get the last weeks."""
    time_now = datetime.now()
    year = time_now.isocalendar()[0]
    week = time_now.isocalendar()[1]
    weeks = []
    for i in range(0, number_of_weeks):
        start = get_week_dates(year, week - i, as_timestamp=True)[0]
        n_year, n_week = get_year_week(start)
        weeks.append((n_year, n_week))

    return weeks
