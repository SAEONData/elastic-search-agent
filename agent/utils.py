import cherrypy
import json
from datetime import datetime


def gen_unique_id():
    return datetime.now().strftime("%Y%m%d%H%M%S%f")


def iso_8601(adate):
    try:
        return adate.strftime("%Y-%m-%dT%H:%M:%SZ")
    except Exception as e:
        raise RuntimeError('Error {} formating iso_8601 date {}'.format(
            e, adate))


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return iso_8601(obj)
        return super().default(obj)
    def iterencode(self, value):
        # Adapted from cherrypy/_cpcompat.py
        for chunk in super().iterencode(value):
            yield chunk.encode("utf-8")

json_encoder = _JSONEncoder()


def json_handler(*args, **kwargs):
    # Adapted from cherrypy/lib/jsontools.py
    value = cherrypy.serving.request._json_inner_handler(*args, **kwargs)
    return json_encoder.iterencode(value)


def get_request_host(request):
    host = request.headers.get('hostd')
    if not host:
        host = request.headers.get('host')
    return host


def format_json_date(date_in):
    # Convert a date
    date_dict = dict(date_in)
    if date_dict.get('date'):
        if date_dict['date']['gte'] == date_dict['date']['lte']:
            date_dict['date'] = date_dict['date']['gte']
        else:
            date_dict['date'] = '{}/{}'.format(
                date_dict['date']['gte'],
                date_dict['date']['lte'])
    return date_dict


def format_json_dates(hits):
    # Convert dates
    results = []
    for result in hits:
        result = result.to_dict()
        new_dates = []
        for dates in result['record'].get('dates', []):
            new_dates.append(format_json_date(dates))
        result['record']['dates'] = new_dates
        results.append(result)

    return results


def get_dc_date(jsonData):
    dates = jsonData.get('dates', [])
    dc_date = None
    for adate in dates:
        if adate.get('dateType') and \
           adate.get('dateType') == 'Submitted' and \
           adate.get('date'):
            dates_dict = format_json_date(adate)
            dc_date = dates_dict.get('date')
            break

    if dc_date is None:
        year = jsonData.get('publicationYear')
        if year:
            dc_date = year

    return dc_date
