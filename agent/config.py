from elasticsearch_dsl import connections

# CherryPy testng details
server_url = 'http://localhost:8080'

# ES details
es_port = 9200
metadata_index_name = 'md_index_4'
token_index_name = 'rt_index_4'
es_connection = connections.create_connection(
    hosts=['localhost:{}'.format(es_port)],
    timeout=20)

# Importer details
import_url = 'http://qa.dirisa.org'
import_user = 'admin'
import_password = ''

# Repo Identity details
repositoryName = 'SAEON'
baseURL = 'http://oai.saeon.ac.za'
protocolVersion = '2'
adminEmail = 'info@saeon.ac.za'
earliestDatestamp = '2011-01-01T00:00:00Z'
deletedRecord = 'persistent'
granularity = 'YYYY-MM-DDThh:mm:ssZ'
compressions = ['gzip', 'deflate']
scheme = 'oai'
repositoryIdentifier = 'oai.saeon.ac.za'
delimiter = ':'
sampleIdentifier = 'oai:oai.saeon.ac.za:12425'
