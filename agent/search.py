from datetime import datetime
from agent.config import metadata_index_name
from agent.persist import Metadata
from elasticsearch.exceptions import TransportError
from elasticsearch_dsl import DateHistogramFacet
from elasticsearch_dsl import FacetedSearch
from elasticsearch_dsl import Mapping
from elasticsearch_dsl import Q
# from elasticsearch_dsl import RangeFacet
from elasticsearch_dsl import TermsFacet


def search_all():
    srch = Metadata.search()
    return {'success': True, 'result': srch.scan()}


def search(**kwargs):
    mapping = Mapping.from_es(metadata_index_name, 'doc')
    output = {'success': False}
    srch = Metadata.search()
    source_fields = ''
    sort_field = None
    from_date = None
    to_date = None
    start = 1
    size = 100
    q_list = []
    for key in kwargs:
        # print('------------------------' + key)
        if key == 'record.fields':
            source_fields = kwargs[key]
            continue
        elif key == 'record.start':
            start = kwargs[key]
            continue
        elif key == 'record.size':
            size = kwargs[key]
            continue
        elif key == 'record.sort':
            sort_field = kwargs[key]
            continue
        elif key == 'record.from':
            from_date = kwargs[key]
            continue
        elif key == 'record.to':
            to_date = kwargs[key]
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
        # print('Sort on {}'.format(sort_field))
        if sort_field.startswith('-'):
            sort_field = sort_field[1:]
            sort_field_name = 'record.{}'.format(sort_field)
            sort_field = {sort_field_name: {'order': 'desc'}}
        else:
            sort_field_name = 'record.{}'.format(sort_field)
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
        for field in source_fields.split(','):
            field = field.strip()
            # print('limit output field: record.{}'.format(field))
            field_name = 'record.{}'.format(field)
            if mapping.resolve_field(field_name) is None:
                msg = 'Unknown source field: {}'.format(field)
                output['error'] = msg
                return output
            new_fields.append(field_name)
        srch = srch.source(new_fields)

    if from_date:
        try:
            from_date = datetime.strptime(from_date, '%Y-%m-%d')
        except Exception as e:
            msg = 'from date {} format should be YYYY-MM-DD'.format(
                from_date)
            output['error'] = msg
            return output

    if to_date:
        try:
            to_date = datetime.strptime(to_date, '%Y-%m-%d')
        except Exception as e:
            msg = 'to date {} format should be YYYY-MM-DD'.format(
                to_date)
            output['error'] = msg
            return output

    if from_date and to_date:
        dates = {'record.dates.date': {
            'gte': from_date,
            'lte': to_date,
            'relation': 'intersects'}}
        print('dates: {}'.format(dates))
        q_list.append(Q({"range": dates}))
    elif from_date:
        dates = {'record.dates.date': {
            'gte': from_date}}
        q_list.append(Q({"range": dates}))
        print('dates: {}'.format(dates))
    elif to_date:
        dates = {'record.dates.date': {
            'lte': to_date}}
        q_list.append(Q({"range": dates}))
        print('dates: {}'.format(dates))

    srch.query = {'bool': {'must': q_list}}

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

    print('page {} - {}'.format(start, size))
    srch = srch[start: size]

    try:
        output['result'] = srch.execute()
        output['count'] = srch.count()
        print('count {}'.format(srch.count()))
        output['success'] = True
    except TransportError as e:
        if e.error == 'search_phase_execution_exception':
            msg = 'Sort field {} is not allowed'.format(sort_field_name)
        else:
            msg = 'Search Engine Transport error: {}'.format(e)
        output['error'] = msg
    except Exception as e:
        msg = 'Search Engine unknown error: {}'.format(e)
        output['error'] = msg
    return output


class MetadataSearch(FacetedSearch):
    doc_types = [Metadata, ]
    index = metadata_index_name

    fields = [
        # 'record.publicationYear',
        # 'record.subjects.subject.raw',
        # 'record.creators.creatorName.raw',
        # 'record.publisher.raw',
        # 'record.dates.date',
    ]

    facets = {
        'subjects': TermsFacet(field='record.subjects.subject.raw'),
        'creators': TermsFacet(field='record.creators.creatorName.raw'),
        # 'dates': DateHistogramFacet(field='record.dates.date.gte', interval="month"),
        # 'dates': RangeFacet(field='record.dates.date.gte', format="MM-yy", ranges=[
        #     ("to", "now-10M/M"), ("from", "now-10M/M")]),
        'publicationYear': TermsFacet(field='record.publicationYear'),
        'publisher': TermsFacet(field='record.publisher.raw'),
    }

    # def search(self):
    #     # override methods to add custom pieces
    #     s = super(MetadataSearch, self).search()
    #     return s.filter()
