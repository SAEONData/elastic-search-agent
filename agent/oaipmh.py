from agent.config import index_name
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
from agent.datacite_export import generateXML
from datetime import datetime
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import xml.etree.ElementTree as ET


def format_response(root, records):
    if records:
        e_getrecord = ET.SubElement(root, 'GetRecord')

        for record in records:
            e_record = ET.SubElement(e_getrecord, "record")
            try:
                generateXML(e_record, record.to_dict().get('record'))
            except AttributeError as e:
                print('AttributeError: {}'.format(e))
    return


def get_record(root, request_element, **kwargs):
    s = Search(index=index_name)
    q_list = []
    for k in kwargs:
        if k == 'verb':
            # ignore
            continue
        request_element.set(k, kwargs[k])
        if k == 'metadataPrefix':
            # ignore
            continue
        if k == 'identifier':
            key = 'record.identifier.identifier'
        else:
            # TODO
            raise RuntimeError('get_record: unknown param {}'.format(k))
        q_item = Q({"match": {key: kwargs[k]}})
        q_list.append(q_item)

    if q_list:
        qry = {'bool': {'must': q_list}}
        print('GetRecord Query: {}'.format(qry))
        s.query = qry

    records = [r for r in s.scan()]
    print('Found {} records'.format(len(records)))
    return format_response(root, records)


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
    if verb not in ['GetRecord', 'Identity']:
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
        if metadataPrefix not in ['datacite', ]:
            child = ET.SubElement(root, 'error', {'code': 'badArgument'})
            child.text = 'metadataPrefix "{}" cannot be processed'.format(
                metadataPrefix)
            return ET.tostring(root)

        get_record(root, request_element, **kwargs)

    elif verb == 'Identity':

        identity(root, repositoryName, baseURL, protocolVersion,
                 adminEmail, earliestDatestamp, deletedRecord,
                 granularity, compressions, scheme, repositoryIdentifier,
                 delimiter, sampleIdentifier)

    return ET.tostring(root)
