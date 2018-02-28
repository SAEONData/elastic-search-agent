from agent.config import index_name
from datetime import datetime
from elasticsearch_dsl import Search
from elasticsearch_dsl import Q
import xml.etree.ElementTree as ET


def format_record(record):
    identifier = 'NONE'
    if record.get('identifier'):
        identifier = record.get('identifier').get('identifier')

    response = """
        <identifier>{}</identifier>
    """.format(identifier)
    return response


def format_response(request, records):
    response = """
<?xml version="1.0" encoding="UTF-8" ?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/
         http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd">
 <responseDate>{date}</responseDate>
    """.format(date=datetime.now())

    response += request

    if records:
        response += "<GetRecord>"

        for record in records:
            response += "<record>"
            try:
                response += format_record(record.to_dict().get('record'))
            except AttributeError as e:
                print('AttributeError: {}'.format(e))
            response += "</record>"

        response += "</GetRecord>"
    response += "</OAI-PMH>"
    return response


def get_record(**kwargs):
    s = Search(index=index_name)
    q_list = []
    for k in kwargs:
        if k == 'verb':
            # ignore
            continue
        key = 'record.{}'.format(k)
        q_item = Q({"match": {key: kwargs[k]}})
        q_list.append(q_item)
    if q_list:
        qry = {'bool': {'must': q_list}}
        print('GetRecord Query: {}'.format(qry))
        s.query = qry

    response = s.scan()
    return response


def process_request(**kwargs):
    root = ET.Element("OAI-PMH", {
        "xmlns": "http://www.openarchives.org/OAI/2.0/",
        "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
        "xsi:schemaLocation": "http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd"
    })

    # child = ET.SubElement(root, 'responseDate', {'text': str(datetime.now())})
    child = ET.SubElement(root, 'responseDate', {
        'xml': datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
    request = ''
    response = ''
    verb = kwargs.get('verb', '')
    if verb is None or len(verb) == 0:
        ET.SubElement(root, 'error', {'code': 'badArgument', 'text': 'argument "verb" not found'})
        return ET.tostring(root)
    if verb not in ['GetRecord', ]:
        request += \
            '<error code="badVerb">unknown verb {}</error>'.format(verb)

    if verb == 'GetRecord':
        response = get_record(**kwargs)

    return format_response(request, response)
