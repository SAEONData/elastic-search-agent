from agent.config import adminEmail
from agent.config import baseURL
from agent.config import compressions
from agent.config import deletedRecord
from agent.config import delimiter
from agent.config import earliestDatestamp
from agent.config import granularity
from agent.config import metadata_index_name
from agent.config import protocolVersion
from agent.config import repositoryIdentifier
from agent.config import repositoryName
from agent.config import sampleIdentifier
from agent.config import scheme
from agent.datacite_export import generateXMLDataCite
from agent.oai_dc_export import generateXMLDC
from agent.persist import Metadata
from agent.persist import ResumptionToken
from agent.search import search
from agent.utils import get_dc_date
from agent.utils import get_request_host
from datetime import datetime
from elasticsearch_dsl import Q
import xml.etree.ElementTree as ET

SIZE = 10


def add_resumption_token(size, cursor):
    rs = ResumptionToken(md_size=size, md_cursor=cursor)
    rs.save()
    # print('add_resumption_token: {} {} {}'.format(
    #     size, cursor, rs.md_token))
    return rs.md_token


def find_resumption_token(md_token):
    srch = ResumptionToken.search()
    srch = srch.query('match', md_token=md_token)
    rs = srch.execute()
    if len(rs.hits):
        return rs.hits[0].md_cursor


def format_identifier(root, record):
    set_spec = ''
    el_header = ET.SubElement(root, "header")
    try:
        set_spec = record.set_spec
        record = record.to_dict().get('record').get('metadata_json')
        if record.get('identifier', '') != '':
            child = ET.SubElement(el_header, 'identifier')
            child.text = record['identifier']['identifier']

        dc_date = get_dc_date(record)
        if dc_date:
            dc_date = dc_date.split('/')[0]
            child = ET.SubElement(el_header, 'datestamp')
            child.text = dc_date

        if set_spec:
            child = ET.SubElement(el_header, 'setSpec')
            child.text = set_spec
    except AttributeError as e:
        raise RuntimeError('AttributeError: {}'.format(e))


def format_records(root, records, prefix, md_token=None):
    for record in records:
        if prefix == 'oai_datacite':
            el_record = ET.SubElement(root, "resource")
            # , {
            #     'xsi:schemaLocation': "http://datacite.org/schema/kernel-2.1 http://schema.datacite.org/meta/kernel-2.1/metadata.xsd"})
        else:
            el_record = ET.SubElement(root, "record")
            format_identifier(el_record, record)
        try:
            if prefix == 'datacite':
                generateXMLDataCite(
                    el_record,
                    record.to_dict().get('record').get('metadata_json'))
            elif prefix == 'oai_datacite':
                generateXMLDataCite(
                    el_record,
                    record.to_dict().get('record').get('metadata_json'))
            elif prefix == 'oai_dc':
                generateXMLDC(
                    el_record,
                    record.to_dict().get('record').get('metadata_json'))
        except AttributeError as e:
            print('AttributeError: {}'.format(e))
            raise

    if md_token:
        child = ET.SubElement(root, 'resumptionToken')
        child.text = md_token
    return root


def get_record(root, request_element, **kwargs):
    prefix = 'datacite'
    qry = None
    for k in kwargs:
        if k == 'verb':
            # ignore
            continue
        request_element.set(k, kwargs[k])
        if k == 'metadataPrefix':
            prefix = kwargs[k]
        elif k == 'identifier':
            key = 'record.metadata_json.identifier.identifier'
            qry = Q({"match": {key: kwargs[k]}})
        else:
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'Unknown argument "{}"'.format(k)
            return root

    # Ensure metadataPrefix is provided
    if prefix is None or len(prefix) == 0:
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'argument "metadataPrefix" is required'
        return root

    # Ensure metadataPrefix can be handled
    if prefix not in ['datacite', 'oai_dc', 'oai_datacite']:
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'metadataPrefix "{}" cannot be processed'.format(prefix)
        return root

    if qry is None:
        # Ensure identifier is provided
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'argument "identifier" not found'
        return root

    # print('Search for query {}'.format(qry))
    srch = Metadata.search()
    srch.query = qry
    records = srch.execute()
    records = [r for r in records]
    if len(records) == 0:
        child = ET.SubElement(root, 'error', {'code': 'idDoesNotExist'})
        child.text = 'Not matching identifier'
        return root

    # print('Found {} records'.format(len(records)))
    if prefix == 'oai_datacite':
        # Override root
        root = ET.Element("oai_datacite")
        # , {
        #     "xsi:schemaLocation":
        #     "http://schema.datacite.org/oai/oai-1.0/ http://schema.datacite.org/oai/oai-1.0/oai.xsd"
        # })
        child = ET.SubElement(root, 'isReferenceQuality')
        child.text = 'true'
        child = ET.SubElement(root, 'schemaVersion')
        child.text = '2.1'
        child = ET.SubElement(root, 'datacentreSymbol')
        child.text = 'CISTI.JOE'
        el_getrecord = ET.SubElement(root, 'payload')
    else:
        el_getrecord = ET.SubElement(root, 'GetRecord')

    format_records(el_getrecord, records, prefix)
    return root


def format_identifiers(root, records, prefix, md_token):
    if records:
        el_identifiers = ET.SubElement(root, 'ListIdentifiers')

        for record in records:
            format_identifier(el_identifiers, record)

    if md_token:
        child = ET.SubElement(root, 'resumptionToken')
        child.text = md_token
    return


def list_results(root, request_element, form, **kwargs):
    md_token = None
    from_date = None
    until_date = None
    prefix = 'datacite'
    query = dict()
    query['index'] = metadata_index_name
    for k in kwargs:
        if k == 'verb':
            # ignore
            continue
        request_element.set(k, kwargs[k])
        if k == 'metadataPrefix':
            prefix = kwargs[k]
        elif k == 'set':
            query['set_spec'] = kwargs[k]
        elif k == 'from':
            from_date = kwargs[k]
        elif k == 'until':
            until_date = kwargs[k]
        elif k == 'resumptionToken':
            md_token = kwargs[k]
        else:
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'Unknown argument "{}"'.format(k)
            return root

    # Ensure metadataPrefix is provided
    if prefix is None or len(prefix) == 0:
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'argument "metadataPrefix" is required'
        return root

    # Ensure metadataPrefix can be handled
    if prefix not in ['datacite', 'oai_dc', 'oai_datacite']:
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'metadataPrefix "{}" cannot be processed'.format(
            prefix)
        return root

    # Get resumptionToken
    md_cursor = 0
    if md_token:
        md_cursor = find_resumption_token(md_token)
        if md_cursor is None:
            ET.SubElement(root, 'error', {'code': 'badResumptionToken'})
            return root

    if from_date:
        try:
            datetime.strptime(from_date, '%Y-%m-%d')
        except Exception as e:
            print('from date format error {}'.format(e))
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'from date {} format should be YYYY-MM-DD'.format(
                from_date)
            return root

    if until_date:
        try:
            datetime.strptime(until_date, '%Y-%m-%d')
        except Exception as e:
            print('Until date format error {}'.format(e))
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'until date {} format should be YYYY-MM-DD'.format(
                until_date)
            return root

    if from_date:
        query['from'] = from_date
    if until_date:
        query['to'] = until_date
    query['sort'] = 'record.metadata_json.identifier.identifier'
    query['start'] = md_cursor + 1
    query['size'] = SIZE

    response = search(**query)
    if not response.get('success', False):
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = response['error']
        return root
    records = [r for r in response['result']]
    if len(records) == 0:
        child = ET.SubElement(root, 'error', {'code': 'noRecordsMatch'})
        return root
    # print(', '.join([r.doc_type for r in records]))
    new_token = None
    end_cursor = md_cursor + SIZE
    if len(records) == SIZE and end_cursor != response.get('count', 0):
        new_token = add_resumption_token(size=SIZE, cursor=end_cursor)
    if form == 'records':
        if prefix == 'oai_datacite':
            # Override root
            root = ET.Element("oai_datacite")
            # , {
            #     "xsi:schemaLocation":
            #     "http://schema.datacite.org/oai/oai-1.0/ http://schema.datacite.org/oai/oai-1.0/oai.xsd"
            # })
            child = ET.SubElement(root, 'isReferenceQuality')
            child.text = 'true'
            child = ET.SubElement(root, 'schemaVersion')
            child.text = '2.1'
            child = ET.SubElement(root, 'datacentreSymbol')
            child.text = 'CISTI.JOE'
            el_listrecords = ET.SubElement(root, 'payload')
        else:
            el_listrecords = ET.SubElement(root, 'ListRecords')
        format_records(el_listrecords, records, prefix, new_token)
        return root
    elif form == 'identifiers':
        return format_identifiers(root, records, prefix, new_token)
    else:
        raise RuntimeError('list_results: unknown param {}'.format(k))


def identity(root, host, repositoryName, baseURL, protocolVersion, adminEmail,
             earliestDatestamp, deletedRecord, granularity, compressions,
             scheme, repositoryIdentifier, delimiter, sampleIdentifier):
    child = ET.SubElement(root, 'Identity')
    child = ET.SubElement(root, 'repositoryName')
    child.text = repositoryName
    child = ET.SubElement(root, 'baseURL')
    child.text = baseURL.format(host)
    child = ET.SubElement(root, 'protocolVersion')
    child.text = protocolVersion
    child = ET.SubElement(root, 'adminEmail')
    child.text = adminEmail
    years = search(**{
        'index': metadata_index_name,
        'size': 1,
        'fields': 'record.metadata_json.publicationYear',
        'sort': 'record.metadata_json.publicationYear'})
    if years.get('success'):
        year = [y for y in years['result']][0]['record']['metadata_json']['publicationYear']
        child = ET.SubElement(root, 'earliestDatestamp')
        child.text = earliestDatestamp.format(year)
    child = ET.SubElement(root, 'deletedRecord')
    child.text = deletedRecord
    child = ET.SubElement(root, 'granularity')
    child.text = granularity
    for compression in compressions:
        child = ET.SubElement(root, 'compression')
        child.text = compression
    desc = ET.SubElement(root, 'description')
    oai_id = ET.SubElement(desc, 'oai-identifier', {
        'xmlns': "http://www.openarchives.org/OAI/2.0/oai-identifier",
        'xmlns:xsi': "http://www.w3.org/2001/XMLSchema-instance",
        'xsi:schemaLocation': "http://www.openarchives.org/OAI/2.0/oai-identifier http://www.openarchives.org/OAI/2.0/oai-identifier.xsd"})
    child = ET.SubElement(desc, 'scheme')
    child.text = scheme
    child = ET.SubElement(oai_id, 'repositoryIdentifier')
    child.text = repositoryIdentifier.format(host)
    child = ET.SubElement(oai_id, 'delimiter')
    child.text = delimiter
    child = ET.SubElement(oai_id, 'sampleIdentifier')
    child.text = sampleIdentifier.format(host)


def list_metadata_formats(root):
    el_list = ET.SubElement(root, 'ListMetadataFormats')

    el_format = ET.SubElement(el_list, 'metadataFormat')
    child = ET.SubElement(el_format, 'metadataPrefix')
    child.text = 'oai_dc'
    child = ET.SubElement(el_format, 'schema')
    child.text = 'http://www.openarchives.org/OAI/2.0/oai_dc.xsd'
    child = ET.SubElement(el_format, 'metadataNamespace')
    child.text = 'http://www.openarchives.org/OAI/2.0/oai_dc/'

    el_format = ET.SubElement(el_list, 'metadataFormat')
    child = ET.SubElement(el_format, 'metadataPrefix')
    child.text = 'datacite'
    child = ET.SubElement(el_format, 'schema')
    child.text = 'http://datacite.org/schema/nonexistant'
    child = ET.SubElement(el_format, 'metadataNamespace')
    child.text = 'http://schema.datacite.org/meta/nonexistant/nonexistant.xsd'

    el_format = ET.SubElement(el_list, 'metadataFormat')
    child = ET.SubElement(el_format, 'metadataPrefix')
    child.text = 'oai_datacite'
    child = ET.SubElement(el_format, 'schema')
    child.text = 'http://schema.datacite.org/oai/oai-1.1/'
    child = ET.SubElement(el_format, 'metadataNamespace')
    child.text = 'http://schema.datacite.org/oai/oai-1.1/oai.xsd'


def process_request(request, query_string, **kwargs):
    host = get_request_host(request)

    root = ET.Element("OAI-PMH", {
        "xmlns": "http://www.openarchives.org/OAI/2.0/",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
    })

    child = ET.SubElement(root, 'responseDate')
    child.text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    request_element = ET.SubElement(root, 'request')
    request_element.text = '{}://{}'.format(request.scheme, host)

    # Ensure verb is provided
    verb = kwargs.get('verb', '')
    if verb is None or len(verb) == 0:
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'argument "verb" not found'
        return ET.tostring(root)

    # Ensure verb can be handled
    allowed_verbs = [
        'GetRecord',
        'Identity',
        'ListMetadataFormats',
        'ListIdentifiers',
        'ListRecords',
    ]
    if verb not in allowed_verbs:
        child = ET.SubElement(root, 'error', {'code': 'badVerb'})
        child.text = 'verb "{}" cannot be processed'.format(verb)
        return ET.tostring(root)

    request_element.set('verb', verb)
    if verb == 'GetRecord':
        root = get_record(root, request_element, **kwargs)

    elif verb == 'ListRecords':
        root = list_results(root, request_element, 'records', **kwargs)

    elif verb == 'ListIdentifiers':
        list_results(root, request_element, 'identifiers', **kwargs)

    elif verb == 'Identity':

        identity(root, host, repositoryName, baseURL, protocolVersion,
                 adminEmail, earliestDatestamp, deletedRecord,
                 granularity, compressions, scheme, repositoryIdentifier,
                 delimiter, sampleIdentifier)

    elif verb == 'ListMetadataFormats':

        list_metadata_formats(root)

    return ET.tostring(root)
