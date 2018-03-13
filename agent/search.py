from elasticsearch_dsl import FacetedSearch
from elasticsearch_dsl import TermsFacet
from elasticsearch_dsl import Q
from agent.persist import Metadata


def search_all():
    s = Metadata.search()
    return s.scan()


def search(**kwargs):
    s = Metadata.search()
    q_list = [Q({"match": {'content_type': 'Metadata'}}), ]
    fields = ''
    for k in kwargs:
        print('------------------------' + k)
        if k == 'record.fields':
            fields = kwargs[k]
            continue
        q_item = Q({"match": {k: kwargs[k]}})
        q_list.append(q_item)
    qry = {'bool': {'must': q_list}}
    # print('Search Query: {}'.format(qry))
    s.query = qry

    if fields:
        new_fields = []
        for field in fields.split(' '):
            new_fields.append('record.{}'.format(field))
        s = s.source(new_fields)
    return s.scan()


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
