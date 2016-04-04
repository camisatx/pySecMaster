from datetime import datetime


def dt_from_iso(row, column):
    """
    Changes the ISO 8601 date string to a datetime object.
    """

    iso = row[column]
    try:
        return datetime.strptime(iso, '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError:
        return datetime.strptime(iso, '%Y-%m-%dT%H:%M:%S')
    except TypeError:
        return 'NaN'


def date_to_iso(row, column):
    """
    Change the default date format of "YYYY-MM-DD" to an ISO 8601 format.
    """

    raw_date = row[column]
    try:
        raw_date_obj = datetime.strptime(raw_date, '%Y-%m-%d')
    except TypeError:   # Occurs if there is no date provided ("nan")
        raw_date_obj = datetime.today()
    return raw_date_obj.isoformat()
