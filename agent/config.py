
# CherryPy testng details
server_port = 9210
server_url = 'http://localhost:{}'.format(server_port)

# ES details
es_port = 9200
metadata_index_name = 'md_index_999'
token_index_name = 'rt_index_999'

# Importer details
import_url = 'http://oa.dirisa.org'
import_user = 'admin'
import_password = ''

# OAI PMH Repo Identity details
repositoryName = 'SAEON'
baseURL = 'http://{}'
protocolVersion = '2'
adminEmail = 'info@saeon.ac.za'
earliestDatestamp = '{}-01-01T00:00:00Z'
deletedRecord = 'persistent'
granularity = 'YYYY-MM-DDThh:mm:ssZ'
compressions = ['gzip', 'deflate']
scheme = 'oai'
repositoryIdentifier = '{}/oaipmh'
delimiter = ':'
sampleIdentifier = 'oai:{}/oaipmh:12425'
