import cherrypy
import json
import logging
from datetime import datetime
from elasticsearch_dsl import Index


logger = logging.getLogger(__name__)


def format_geo_point(point):
    results = point.split(' ')
    results = [i for i in filter(('').__ne__, results)]
    results = ','.join(results)
    return results


def format_geo_box(box):
    results = box.split(' ')
    results = [float(i) for i in filter(('').__ne__, results)]
    coords = "(({} {}, {} {}, {} {}, {} {}, {} {}))".format(
        results[2], results[1],
        results[0], results[1],
        results[0], results[3],
        results[2], results[3],
        results[2], results[1])
    results = 'POLYGON {}'.format(coords)
    # print('format_geo_box: {}'.format(results))
    return results


def validate_metadata_record(record):
    output = {'success': False}
    try:
        identifier = record['identifier']['identifier']
    except Exception as e:
        msg = "identifier is required"
        logger.debug('Exception: {}: {}'.format(msg, e))
        output['msg'] = msg
        return output
    if identifier == '':
        msg = "identifier is required"
        output['msg'] = msg
        return output

    # Hack to fix rights
    rights = record.get('rights')
    if rights == '':
        record['rights'] = []

    # Hack to fix dates
    dates = record.get('dates')
    lst = []
    for date_dict in dates:
        if date_dict.get('date', '') != '':
            new = dict()
            if date_dict.get('dateType'):
                new['dateType'] = date_dict.get('dateType')
            the_date = date_dict['date']
            if '/' in the_date:
                the_dates = the_date.split('/')
                new['date'] = {'gte': the_dates[0], 'lte': the_dates[1]}
            else:
                new['date'] = {'gte': the_date, 'lte': the_date}
            lst.append(new)
    logger.debug(lst)
    record['dates'] = lst

    output['success'] = True
    return output


def index_exists(index):
    idx = Index(index)
    return idx.exists()


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


def format_json_dates(result):
    # Convert dates
    new_dates = []
    for dates in result['metadata_json'].get('dates', []):
        new_dates.append(format_json_date(dates))
    result['metadata_json']['dates'] = new_dates


def format_metadata(hits):
    # Convert certain fields
    results = []
    for result in hits:
        result = result.to_dict()
        if result.get('metadata_json'):
            format_json_dates(result)
        infrastructures = result.get('infrastructures', [])
        if isinstance(infrastructures, str):
            infrastructures = [infrastructures]
        result['infrastructures'] = infrastructures
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
