from agent.config import es_connection
from agent.config import metadata_index_name
from agent.config import token_index_name
from datetime import datetime
from elasticsearch_dsl import analyzer
from elasticsearch_dsl import Date
from elasticsearch_dsl import DateRange
from elasticsearch_dsl import DocType
from elasticsearch_dsl import Keyword
from elasticsearch_dsl import Integer
from elasticsearch_dsl import MetaField
from elasticsearch_dsl import Object
from elasticsearch_dsl import Text


html_strip = analyzer(
    'html_strip',
    tokenizer="standard",
    filter=["standard", "lowercase", "stop", "snowball"],
    char_filter=["html_strip"]
)


class Metadata(DocType):
    created_at = Date()
    set_spec = Text()
    record_id = Keyword()
    record = Object()

    class Meta:
        index = metadata_index_name
        using = es_connection
        dynamic_templates = MetaField([
            {
                "record":
                    {
                        "path_match": "record.identifier.identifier",
                        "match_mapping_type": "string",
                        "mapping": Keyword("not_analyzed")
                    }
            },
            {
                "record":
                    {
                        "path_match": "record.subjects.subject",
                        "match_mapping_type": "string",
                        "mapping": Text(fields={'raw': Keyword()}),
                    }
            },
            {
                "record":
                    {
                        "path_match": "record.creators.creatorName",
                        "match_mapping_type": "string",
                        "mapping": Text(fields={'raw': Keyword()}),
                    }
            },
            {
                "record":
                    {
                        "path_match": "record.publicationYear",
                        "match_mapping_type": "string",
                        "mapping": Integer()
                    }
            },
            {
                "record":
                    {
                        "path_match": "record.publisher",
                        "match_mapping_type": "string",
                        "mapping": Text(fields={'raw': Keyword()}),
                    }
            },
            {
                "record":
                    {
                        "path_match": "record.start_date",
                        "match_mapping_type": "string",
                        "mapping": Date()
                    }
            },
            {
                "record":
                    {
                        "path_match": "record.dates.date",
                        "match_mapping_type": "string",
                        "mapping": DateRange()
                    }
            }
        ])

    def save(self, **kwargs):
        self.created_at = datetime.now()
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
