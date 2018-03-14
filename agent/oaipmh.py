from agent.config import repositoryName
from agent.config import baseURL
from agent.config import protocolVersion
from agent.config import adminEmail
from agent.config import earliestDatestamp
from agent.config import deletedRecord
from agent.config import granularity
from agent.config import compressions
from agent.config import scheme
from agent.config import repositoryIdentifier
from agent.config import delimiter
from agent.config import sampleIdentifier
from agent.persist import Metadata
from agent.persist import ResumptionToken
from agent.datacite_export import generateXMLDataCite
from agent.dc_export import generateXMLDC
from datetime import datetime
from elasticsearch_dsl import Q
import xml.etree.ElementTree as ET


def add_resumption_token(size, cursor):
    rs = ResumptionToken(md_size=size, md_cursor=cursor)
    rs.save()
    print('add_resumption_token: {} {} {}'.format(size, cursor, rs.md_token))
    return rs.md_token


def find_resumption_token(md_token):
    srch = ResumptionToken.search()
    srch = srch.query('match', md_token=md_token)
    rs = srch.execute()
    if len(rs.hits):
        return rs.hits[0].md_cursor

    return 0


def format_records(root, records, prefix):
    if records:
        e_getrecord = ET.SubElement(root, 'GetRecord')

        for record in records:
            e_record = ET.SubElement(e_getrecord, "record")
            try:
                if prefix == 'datacite':
                    generateXMLDataCite(
                        e_record, record.to_dict().get('record'))
                elif prefix == 'oai_dc':
                    generateXMLDC(
                        e_record, record.to_dict().get('record'))
            except AttributeError as e:
                print('AttributeError: {}'.format(e))
    return


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
            continue
        if k == 'identifier':
            key = 'record.identifier.identifier'
            qry = Q({"match": {key: kwargs[k]}})
            break

    if qry is None:
        # Ensure identifier is provided
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'argument "identifier" not found'
        return ET.tostring(root)

    # print('Search for query {}'.format(qry))
    srch = Metadata.search()
    srch.query = qry
    records = srch.execute()
    records = [r for r in records]
    if len(records) == 0:
        child = ET.SubElement(root, 'error', {'code': 'idDoesNotExist'})
        child.text = 'Not matching identifier'
        return ET.tostring(root)
    # print('Found {} records'.format(len(records)))
    return format_records(root, records, prefix)


def format_identifiers(root, records, prefix, md_token):
    if records:
        e_identiers = ET.SubElement(root, 'ListIdentifiers')

        for record in records:
            set_spec = ''
            e_header = ET.SubElement(e_identiers, "header")
            try:
                set_spec = record.set_spec
                record = record.to_dict().get('record')
                if record.get('identifier', '') != '':
                    child = ET.SubElement(e_header, 'identifier')
                    child.text = record['identifier']['identifier']

                date = None
                lstDates = record.get('dates', [])
                if len(lstDates) > 0:
                    for dte in lstDates:
                        date = dte['date']
                        if date:
                            # Use the first date
                            child = ET.SubElement(e_header, 'datestamp')
                            child.text = date
                            break

                if set_spec:
                    child = ET.SubElement(e_header, 'setSpec')
                    child.text = set_spec
            except AttributeError as e:
                print('AttributeError: {}'.format(e))

    if md_token:
        child = ET.SubElement(root, 'resumptionToken')
        child.text = md_token
    return


def list_identifiers(root, request_element, **kwargs):
    query = {}
    md_token = None
    prefix = 'datacite'
    for k in kwargs:
        if k == 'verb':
            # ignore
            continue
        request_element.set(k, kwargs[k])
        if k == 'metadataPrefix':
            prefix = kwargs[k]
            continue
        if k == 'set':
            query['set_spec'] = kwargs[k]
        if k == 'resumptionToken':
            md_token = kwargs[k]
        else:
            # TODO
            raise RuntimeError(
                'list_identifiers: unknown param {}'.format(k))

    # print('list_identifiers query: {}'.format(query))
    md_cursor = 0
    if md_token:
        md_cursor = find_resumption_token(md_token)
    end = md_cursor + 10
    print('Cursor: {} - {}'.format(md_cursor, end))
    srch = Metadata.search()
    srch = srch.sort('record.identifier.identifier')
    srch = srch[md_cursor:end]
    records = srch.execute()
    records = [r for r in records]
    # print(', '.join([r.doc_type for r in records]))
    new_token = None
    if len(records) == 10:
        new_token = add_resumption_token(size=10, cursor=end)
    return format_identifiers(root, records, prefix, new_token)


def identity(root, repositoryName, baseURL, protocolVersion, adminEmail,
             earliestDatestamp, deletedRecord, granularity, compressions,
             scheme, repositoryIdentifier, delimiter, sampleIdentifier):
    child = ET.SubElement(root, 'Identity')
    child = ET.SubElement(root, 'repositoryName')
    child.text = repositoryName
    child = ET.SubElement(root, 'baseURL')
    child.text = baseURL
    child = ET.SubElement(root, 'protocolVersion')
    child.text = protocolVersion
    child = ET.SubElement(root, 'adminEmail')
    child.text = adminEmail
    child = ET.SubElement(root, 'earliestDatestamp')
    child.text = earliestDatestamp
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
    child.text = repositoryIdentifier
    child = ET.SubElement(oai_id, 'delimiter')
    child.text = delimiter
    child = ET.SubElement(oai_id, 'sampleIdentifier')
    child.text = sampleIdentifier


def process_request(request_base, query_string, **kwargs):
    root = ET.Element("OAI-PMH", {
        "xmlns": "http://www.openarchives.org/OAI/2.0/",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
    })

    child = ET.SubElement(root, 'responseDate')
    child.text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    request_element = ET.SubElement(root, 'request')
    request_element.text = request_base

    # Ensure verb is provided
    verb = kwargs.get('verb', '')
    if verb is None or len(verb) == 0:
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'argument "verb" not found'
        return ET.tostring(root)

    # Ensure verb can be handled
    if verb not in ['GetRecord', 'Identity', 'ListIdentifiers']:
        child = ET.SubElement(root, 'error', {'code': 'badVerb'})
        child.text = 'verb "{}" cannot be processed'.format(verb)
        return ET.tostring(root)

    request_element.set('verb', verb)
    if verb == 'GetRecord':
        # Ensure metadataPrefix is provided
        metadataPrefix = kwargs.get('metadataPrefix', '')
        if metadataPrefix is None or len(metadataPrefix) == 0:
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'argument "metadataPrefix" not found'
            return ET.tostring(root)

        # Ensure metadataPrefix can be handled
        if metadataPrefix not in ['datacite', 'oai_dc']:
            child = ET.SubElement(
                root, 'error', {'code': 'cannotDisseminateFormat'})
            return ET.tostring(root)

        get_record(root, request_element, **kwargs)

    elif verb == 'ListIdentifiers':
        # Ensure metadataPrefix is provided
        metadataPrefix = kwargs.get('metadataPrefix', '')
        if metadataPrefix is None or len(metadataPrefix) == 0:
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'argument "metadataPrefix" not found'
            return ET.tostring(root)

        # Ensure metadataPrefix can be handled
        if metadataPrefix not in ['datacite', 'oai_dc', 'resumptionToken']:
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'metadataPrefix "{}" cannot be processed'.format(
                metadataPrefix)
            return ET.tostring(root)

        list_identifiers(root, request_element, **kwargs)

    elif verb == 'Identity':

        identity(root, repositoryName, baseURL, protocolVersion,
                 adminEmail, earliestDatestamp, deletedRecord,
                 granularity, compressions, scheme, repositoryIdentifier,
                 delimiter, sampleIdentifier)

    return ET.tostring(root)
