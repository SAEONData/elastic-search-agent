import logging
from datetime import datetime
from agent.persist import Metadata
from elasticsearch.exceptions import TransportError
from elasticsearch_dsl import DateHistogramFacet
from elasticsearch_dsl import FacetedSearch
from elasticsearch_dsl import Mapping
from elasticsearch_dsl import Q
from elasticsearch_dsl import TermsFacet

logger = logging.getLogger(__name__)


def search_all(index):
    srch = Metadata.search(index=index)
    logger.info('search_all in index {}'.format(index))
    return {'success': True, 'result': srch.scan()}


def search(index, **kwargs):
    output = {'success': False}
    mapping = Mapping.from_es(index, 'doc')
    srch = Metadata.search(index=index)
    logger.info('search in index {}'.format(index))
    source_fields = ''
    sort_field = None
    from_date = None
    to_date = None
    date_type = None
    filters = []
    afilter = None
    relation = None
    coords = None

    start = 1
    size = 100
    q_list = []
    for key in kwargs:
        logger.debug('------------------------' + key)
        if key == 'fields':
            source_fields = kwargs[key]
            continue
        elif key == 'start':
            start = kwargs[key]
            continue
        elif key == 'size':
            size = kwargs[key]
            continue
        elif key == 'sort':
            sort_field = kwargs[key]
            continue
        elif key == 'from':
            from_date = kwargs[key]
            continue
        elif key == 'to':
            to_date = kwargs[key]
            continue
        elif key == 'dates.dateType':
            date_type = kwargs[key].lower()
            continue
        elif key in 'encloses':
            relation = 'within'
            coords = kwargs[key]
            continue
        elif key in 'includes':
            relation = 'contains'
            coords = kwargs[key]
            continue
        elif key in 'overlaps':
            relation = 'intersects'
            coords = kwargs[key]
            continue
        # Otherwise add to query
        field_type = mapping.resolve_field(key)
        field_name = '.'.join(key.split('.')[1:])
        if field_type is None:
            field_name = '.'.join(key.split('.')[1:])
            msg = 'Unknown search field: {} {}'.format(key, field_name)
            output['error'] = msg
            return output
        if type(field_type).name in ['object', ]:
            msg = 'Cannot search on field: {}'.format(field_name)
            output['error'] = msg
            return output
        q_list.append(Q({"match": {key: kwargs[key]}}))

    if sort_field:
        logger.debug('Sort on {}'.format(sort_field))
        if sort_field.startswith('-'):
            sort_field = sort_field[1:]
            sort_field_name = sort_field
            sort_field = {sort_field_name: {'order': 'desc'}}
        else:
            sort_field_name = sort_field
            sort_field = {sort_field_name: {'order': 'asc'}}

        field_type = mapping.resolve_field(sort_field_name)
        if field_type is None:
            msg = 'Unknown sort field: {}'.format(sort_field_name)
            output['error'] = msg
            return output
        if type(field_type).name in ['object', ]:
            msg = 'Cannot sort on field: {}'.format(sort_field_name)
            output['error'] = msg
            return output
        srch = srch.sort(sort_field)

    if source_fields:
        new_fields = []
        if isinstance(source_fields, str):
            source_fields = source_fields.split(',')
        for field in source_fields:
            field = field.strip()
            logger.debug('limit output field: {}'.format(field))
            field_name = field
            if mapping.resolve_field(field_name) is None:
                msg = 'Unknown source field: {}'.format(field)
                output['error'] = msg
                return output
            new_fields.append(field_name)
        srch = srch.source(new_fields)

    if from_date:
        try:
            from_date = datetime.strptime(from_date, '%Y-%m-%d').date()
        except Exception as e:
            msg = 'from date {} format should be YYYY-MM-DD'.format(
                from_date)
            output['error'] = msg
            return output

    if to_date:
        try:
            to_date = datetime.strptime(to_date, '%Y-%m-%d').date()
        except Exception as e:
            msg = 'to date {} format should be YYYY-MM-DD'.format(
                to_date)
            output['error'] = msg
            return output

    if from_date or to_date:
        dates = []
        if from_date:
            dates.append(
                {'range': {'metadata_json.dates.date.lte': {'gte': from_date}}})
        if to_date:
            dates.append(
                {'range': {'metadata_json.dates.date.gte': {'lte': to_date}}})
        if date_type:
            dates.append(
                {'term': {'metadata_json.dates.dateType': date_type}})
        filters.append({'bool': {'must': dates}})
    elif date_type:
        q_list.append(Q({"match": {'metadata_json.dates.dateType': date_type}}))

    if relation:
        try:
            coords = coords.split(',')
            coords = [float(i) for i in coords]
            afilter = {
                "geo_shape": {
                    "metadata_json.geoLocations.geoLocationBox": {
                        "shape": {
                            "type": "envelope",
                            "coordinates": [[coords[0], coords[1]], [coords[2], coords[3]]]
                        },
                        "relation": relation
                    }
                }
            }
            filters.append(afilter)
            if relation in ['within', 'intersects']:
                afilter = {
                    "geo_bounding_box": {
                        "metadata_json.geoLocations.geoLocationPoint": {
                            "top_left": {
                                "lat": coords[0],
                                "lon": coords[1]
                            },
                            "bottom_right": {
                                "lat": coords[2],
                                "lon": coords[3]
                            }
                        }
                    }
                }
                filters.append(afilter)
        except Exception as e:
            msg = 'Coordinate values {} are malformed'.format(coords)
            output['error'] = msg
            return output
        logger.info('Geo Search: {}'.format(afilter))

    # Add Query
    if len(q_list) == 0:
        q_list = {'match_all': {}}

    qry = {'must': q_list}

    # Add Filter
    if filters:
        if len(filters) == 1:
            qry['filter'] = filters[0]
        else:
            qry['filter'] = {'bool': {'should': filters}}

    qry = {'bool': qry}
    logger.info('Search Query {}'.format(qry))
    srch.query = qry

    try:
        start = int(start)
    except Exception as e:
        msg = 'start {} must be an integer'.format(start)
        output['error'] = msg
        return output

    if start <= 0:
        msg = 'start must be greater that zero'
        output['error'] = msg
        return output

    start -= 1

    try:
        size = int(size)
    except Exception as e:
        msg = 'size {} must be an integer'.format(size)
        output['error'] = msg
        return output
    if size < 0:
        msg = 'size must be greater that zero'
        output['error'] = msg
        return output
    size = size + start

    logger.debug('page {} - {}'.format(start, size))
    srch = srch[start: size]

    try:
        output['result'] = srch.execute()
        output['count'] = srch.count()
        logger.debug('count {}'.format(srch.count()))
        output['success'] = True
    except TransportError as e:
        if e.error == 'search_phase_execution_exception':
            msg = 'Search phrase error {}'.format(e)
        else:
            msg = 'Search Engine Transport error: {}'.format(e)
        output['error'] = msg
    except Exception as e:
        msg = 'Search Engine unknown error: {}'.format(e)
        output['error'] = msg
    return output


all_facets = {
    'subjects': TermsFacet(field='metadata_json.subjects.subject.raw'),
    'creators': TermsFacet(field='metadata_json.creators.creatorName.raw'),
    'publicationYear': TermsFacet(field='metadata_json.publicationYear'),
    'publisher': TermsFacet(field='metadata_json.publisher.raw'),
    'collectedStartDate': DateHistogramFacet(
        field='metadata_json.dates.date.gte',
        interval="month"),
    'collectedEndDate': DateHistogramFacet(
        field='metadata_json.dates.date.lte',
        interval="month"),
}


class MetadataSearch(FacetedSearch):

    doc_types = [Metadata, ]
    date_query = {
        'simple_query_string':
            {'fields': ['metadata_json.dates.dateType'], 'query': 'Collected'}
    }
    date_filter = [
        {'term': {'metadata_json.dates.dateType': 'Collected'}}
    ]

    fields = [
    ]

    def __init__(self, **kwargs):
        facet = None
        self.facets = {}
        if kwargs.get('index'):
            self.index = kwargs.pop('index')
        if kwargs.get('facet'):
            facet = kwargs.pop('facet')
        if facet is None:
            self.facets = all_facets
        else:
            self.facets[facet] = all_facets[facet]

        super(MetadataSearch, self).__init__(**kwargs)

    def search(self):
        # override methods to add custom pieces
        s = super(MetadataSearch, self).search()
        print(str(self.facets))
        if 'collectedStartDate' in self.facets or \
           'collectedEndDate' in self.facets:
            date_type = {'fields': ['metadata_json.dates.dateType'],
                         'query': 'Collected'}
            s.query = {'simple_query_string': date_type}
        return s.filter()
