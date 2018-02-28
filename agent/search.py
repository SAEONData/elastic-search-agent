from elasticsearch_dsl import FacetedSearch
from elasticsearch_dsl import TermsFacet
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
from agent.config import index_name
from agent.persist import Metadata


def search_all():
    s = Search(index=index_name)
    return s.scan()


def search(**kwargs):
    s = Search(index=index_name)
    q_list = []
    fields = ''
    for k in kwargs:
        print('------------------------' + k)
        if k == 'record.fields':
            fields = kwargs[k]
            continue
        q_item = Q({"match": {k: kwargs[k]}})
        q_list.append(q_item)
    qry = {'bool': {'must': q_list}}
    print('Search Query: {}'.format(qry))
    s.query = qry

    # fields = [k for k in kwargs.keys()]
    # fields = ['record.creators', ]
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
