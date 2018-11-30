from agent.config import es_domain
from agent.config import es_port
from agent.config import token_index_name
from datetime import datetime
from elasticsearch_dsl import analyzer
from elasticsearch_dsl import connections
from elasticsearch_dsl import Date
from elasticsearch_dsl import DateRange
from elasticsearch_dsl import DocType
from elasticsearch_dsl import GeoPoint
from elasticsearch_dsl import GeoShape
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Integer
from elasticsearch_dsl import MetaField
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text

es_connection = connections.create_connection(
    hosts=['{}:{}'.format(es_domain, es_port)],
    timeout=20)

html_strip = analyzer(
    'html_strip',
    tokenizer="standard",
    filter=["standard", "lowercase", "stop", "snowball"],
    char_filter=["html_strip"]
)


class Metadata(DocType):
    created_at = Date()
    collection = Text()
    metadata_json = Object()
    organization = Text()
    record_id = Keyword()
    anytext = Text()

    class Meta:
        using = es_connection
        dynamic_templates = MetaField([
            # Can't get this to work
            # https://stackoverflow.com/questions/49740033/elasticsearch-6-copy-to-with-dynamic-index-mappings
            # {
            #     "strings":
            #         {
            #             "match_mapping_type": "string",
            #             "mapping": {
            #                 "type": "text",
            #                 "copy_all": "anytext"
            #             }
            #         }
            # },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.identifier.identifier",
                        "match_mapping_type": "string",
                        "mapping": Keyword("not_analyzed")
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.titles.title",
                        "match_mapping_type": "string",
                        "mapping": Text(fields={'raw': Keyword()}),
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.subjects.subject",
                        "match_mapping_type": "string",
                        "mapping": Text(fields={'raw': Keyword()}),
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.creators.creatorName",
                        "match_mapping_type": "string",
                        "mapping": Text(fields={'raw': Keyword()}),
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.publicationYear",
                        "match_mapping_type": "string",
                        "mapping": Integer()
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.publisher",
                        "match_mapping_type": "string",
                        "mapping": Text(fields={'raw': Keyword()}),
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.dates.date",
                        "match_mapping_type": "string",
                        "mapping": DateRange()
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.geoLocations.geoLocationBox",
                        "match_mapping_type": "string",
                        "mapping": GeoShape()
                    }
            },
            {
                "metadata_json":
                    {
                        "path_match": "metadata_json.geoLocations.geoLocationPoint",
                        "match_mapping_type": "string",
                        "mapping": GeoPoint()
                    }
            }
        ])

    def _get_anytext(self):
        result = []
        anyfields = [
            'titles.title',
            'subjects.subject',
            'descriptions.description',
        ]
        for field in anyfields:
            top = self.metadata_json
            parts = field.split('.')
            for idx, val in enumerate(parts):
                top = getattr(top, val)
                if not isinstance(top, str):
                    for item in top:
                        sub = getattr(item, parts[idx + 1])
                        result.append(sub)
                    break
        return ' '.join(result)

    def save(self, **kwargs):
        self.created_at = datetime.now()
        self.anytext = self._get_anytext()
        return super().save(**kwargs)


class ResumptionToken(DocType):
    md_token = Text()
    md_size = Integer()
    md_cursor = Integer()

    class Meta:
        index = token_index_name
        using = es_connection

    def save(self, **kwargs):
        self.md_token = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return super().save(**kwargs)
