from agent.config import index_name
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
        if k == 'identifier':
            key = 'record.identifier.identifier'
        elif k == 'metadataPrefix':
            # TODO
            pass
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


def process_request(request_base, query_string, **kwargs):
    root = ET.Element("OAI-PMH", {
        "xmlns": "http://www.openarchives.org/OAI/2.0/",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
    })

    child = ET.SubElement(root, 'responseDate')
    child.text = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    verb = kwargs.get('verb', '')
    request_element = ET.SubElement(root, 'request')
    request_element.text = request_base

    # Ensure verb is provided
    if verb is None or len(verb) == 0:
        child = ET.SubElement(root, 'error', {'code': 'badArgument'})
        child.text = 'argument "verb" not found'
        return ET.tostring(root)

    # Ensure verb can be handled
    if verb not in ['GetRecord', ]:
        child = ET.SubElement(root, 'error', {'code': 'badVerb'})
        child.text = 'verb "{}" cannot be processed'.format(verb)
        return ET.tostring(root)

    request_element.set('verb', verb)
    if verb == 'GetRecord':
        get_record(root, request_element, **kwargs)

    return ET.tostring(root)
