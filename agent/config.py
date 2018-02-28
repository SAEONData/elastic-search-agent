from elasticsearch_dsl import connections


server_url = 'http://localhost:8080'
es_port = 9200
index_name = 'metadata13'
es_connection = connections.create_connection(
    hosts=['localhost:{}'.format(es_port)],
    timeout=20)
import_url = 'http://oa.dirisa.org'
import_user = 'admin'
import_password = 'editbew123'
