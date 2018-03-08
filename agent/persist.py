from agent.config import es_connection
from agent.config import index_name
from datetime import datetime
from elasticsearch_dsl import analyzer
from elasticsearch_dsl import Date
from elasticsearch_dsl import DocType
from elasticsearch_dsl import Keyword
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
    spec_set = Text()
    record = Object()

    class Meta:
        index = index_name
        using = es_connection
        dynamic_templates = MetaField([
            {
                "record":
                {
                    "path_match": "record.identifier.identifier",
                    "match_mapping_type": "string",
                    "mapping": Keyword("not_analyzed")
                }
            }
        ])

    def save(self, **kwargs):
        self.created_at = datetime.now()
        return super().save(**kwargs)
