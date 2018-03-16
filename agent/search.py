from elasticsearch_dsl import FacetedSearch
from elasticsearch_dsl import TermsFacet
from elasticsearch_dsl import Q
from agent.persist import Metadata


def search_all():
    srch = Metadata.search()
    return srch.scan()


def search(**kwargs):
    srch = Metadata.search()
    fields = ''
    sort_field = None
    size = 100
    for k in kwargs:
        # print('------------------------' + k)
        if k == 'record.fields':
            fields = kwargs[k]
            continue
        elif k == 'record.size':
            size = kwargs[k]
            continue
        elif k == 'record.sort':
            field = kwargs[k]
            if field.startswith('-'):
                field = field[1:]
                sort_field = {'record.{}'.format(field): {'order': 'desc'}}
            else:
                sort_field = {'record.{}'.format(field): {'order': 'asc'}}
            continue
        srch.update_from_dict({"match": {k: kwargs[k]}})
    srch.update_from_dict({'size': size})

    if sort_field:
        # print('Sort on {}'.format(sort_field))
        srch = srch.sort(sort_field)

    if fields:
        new_fields = []
        for field in fields.split(','):
            # print('limit output field: record.{}'.format(field))
            new_fields.append('record.{}'.format(field))
        srch = srch.source(new_fields)
    return srch.execute()


class MetadataSearch(FacetedSearch):
    doc_types = [Metadata, ]

    fields = ['title', 'content.subjects.subject']

    facets = {
        # 'creator': TermsFacet(field='content.creators.creatorName'),
        'subject': TermsFacet(field='content.subjects.subject'),
    }

    def search(self):
        # override methods to add custom pieces
        s = super().search()
        return s.filter()
