from agent.utils import get_request_host
from datetime import datetime
import xml.etree.ElementTree as ET


def get_record(root, request_element, **kwargs):
    # prefix = 'datacite'
    # qry = None
    return root


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
