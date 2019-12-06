import cherrypy
import json
import logging
from datetime import datetime
from elasticsearch_dsl import Index


logger = logging.getLogger(__name__)


def format_geo_point(point):
    if isinstance(point, dict):
        if not point.get('pointLatitude') or \
           not point.get('pointLongitude'):
            return None
        return ','.join(
            [point.get('pointLatitude'), point.get('pointLongitude')])
    if isinstance(point, dict):
        if len(point.split(' ')) != 2:
            return None
        point = [float(i) for i in point.split(' ')]
        return '{} {}'.format(point[0], point[1])


def format_geo_box(box):
    if isinstance(box, dict):
        west = box.get('westBoundLongitude')
        east = box.get('eastBoundLongitude')
        south = box.get('southBoundLatitude')
        north = box.get('northBoundLatitude')
        if not west or not east or not south or not north:
            return None
    elif isinstance(box, str):
        box = box.split()
        if len(box) != 4:
            return None
        south = box[0]
        west = box[1]
        north = box[2]
        east = box[3]
    else:
        return None

    geo_box = "POLYGON (({} {}, {} {}, {} {}, {} {}, {} {}))".format(
        west, south,
        east, south,
        east, north,
        west, north,
        west, south)
    return geo_box


def format_geo_polygons(polygons):
    polygon_strings = []
    for polygon in polygons:
        points = polygon.get('polygonPoints', [])
        if len(points) < 4 or points[0] != points[-1]:
            # a valid polygon must have at least 4 points, with the first and last being equal
            continue
        point_strings = ['{} {}'.format(point['pointLongitude'], point['pointLatitude']) for point in points]
        polygon_strings += ['(({}))'.format(', '.join(point_strings))]

    multipolygon_string = 'MULTIPOLYGON ({})'.format(', '.join(polygon_strings))
    return multipolygon_string


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
    output['success'] = True
    return output


def transpose_metadata_record(record):
    output = {'success': False}
    # Transpose rights from datacite to ES
    rights = record.get('rights')
    if rights == '':
        record['rights'] = []

    # Transpose dates from datacite to ES
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

    # Transpose geoLocations from datacite to ES
    geoLocations = record.get('geoLocations')
    if geoLocations:
        for geoLocation in geoLocations:
            if geoLocation.get('geoLocationPoint'):
                geoLocation['geoLocationPoint'] = \
                    format_geo_point(geoLocation['geoLocationPoint'])
            if geoLocation.get('geoLocationBox'):
                geoLocation['geoLocationBox'] = \
                    format_geo_box(geoLocation['geoLocationBox'])
            if geoLocation.get('geoLocationPolygons'):
                geoLocation['geoLocationPolygons'] = \
                    format_geo_polygons(geoLocation['geoLocationPolygons'])

    # # Hack to clean up description
    # if record.get('description'):
    #     record['descriptions'] = record['description']
    #     del record['description']

    # # Hack to clean up string resourceType
    # if record.get('resourceType'):
    #     resourceType = record['resourceType']
    #     if isinstance(resourceType, str):
    #         record['resourceType'] = {
    #             "resourceTypeGeneral": "Software",
    #             "resourceType": resourceType
    #         }

    # # Hack to clean up publication year
    # if len(record.get('publicationYear')) > 4:
    #     year = record['publicationYear']
    #     if '-' in year:
    #         adate = datetime.strptime(year, '%Y-%m-%d')
    #         record['publicationYear'] = adate.year
    #     elif '/' in year:
    #         adate = datetime.strptime(year, '%Y/%m/%d')
    #         record['publicationYear'] = adate.year

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
