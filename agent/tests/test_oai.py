import requests
import time
import xml.etree.ElementTree as ET
from agent.config import server_url


def list_identifiers_bad_verb():
    response = requests.get(
        url="{}/oaipmh?verb=Identifiers&metadataPrefix=oai_dc".format(server_url)
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    if 'badVerb' not in response.text:
        raise RuntimeError('badVerb not found on response text')
    print('list_identifiers_bad_verb passed')


def list_identifiers_bad_arg():
    response = requests.get(
        url="{}/oaipmh?verb=ListIdentifiers&Prefix=oai_dc".format(server_url)
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    if 'badArgument' not in response.text:
        raise RuntimeError('badArgument not found on response text')
    print('list_identifiers_bad_arg passed')


def find_tag_contents(tag, text):
    pass


def request_list_identifiers(token=None):
    url = "{}/oaipmh?verb=ListIdentifiers&metadataPrefix=oai_dc".format(
        server_url)
    if token:
        url += '&resumptionToken={}'.format(token)
    response = requests.get(url=url)
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    return response.text


def request_list_records(token=None):
    url = "{}/oaipmh?verb=ListRecords&metadataPrefix=oai_dc".format(
        server_url)
    if token:
        url += '&resumptionToken={}'.format(token)
    response = requests.get(url=url)
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    return response.text


def request_get_record(identifier):
    url = "{}/oaipmh?verb=GetRecord&identifier={}&metadataPrefix=oai_dc".format(
        server_url, identifier)
    response = requests.get(url=url)
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    return response.text


def list_identifiers():
    text = request_list_identifiers()

    try:
        root = ET.fromstring(text)
    except Exception:
        raise RuntimeError('Response text is not valid XML')

    # Collect identifiers to look for duplicates
    identifiers = []
    cnt = 0
    for i in root.iter('{http://www.openarchives.org/OAI/2.0/}identifier'):
        cnt += 1
        print('{}. {}'.format(cnt, i.text))
        identifiers.append(i.text)

    if len(identifiers) != 10:
        print('{} headers returned'.format(
            len(identifiers)))
        return

    print('list_identifiers: Page 1 complete')

    done = False
    loop = 1
    while not done:
        time.sleep(2)
        loop += 1

        token = root.find(
            '{http://www.openarchives.org/OAI/2.0/}resumptionToken')
        try:
            token = token.text
        except Exception:
            raise RuntimeError('resumptionToken not found')

        # All good, now get next 10
        text = request_list_identifiers(token=token)
        try:
            root = ET.fromstring(text)
        except Exception:
            raise RuntimeError('Response text is not valid XML')

        cnt = 0
        for i in root.iter('{http://www.openarchives.org/OAI/2.0/}identifier'):
            cnt += 1
            print('{}. {}'.format(cnt, i.text))
            if i.text in identifiers:
                raise RuntimeError('Identifier {} found previously'.format(
                    i.text))
            identifiers.append(i.text)
        if cnt != 10:
            print('{} headers returned'.format(cnt))
            done = True
        print('list_identifiers: Page {} complete'.format(loop))


def list_records():
    text = request_list_records()

    try:
        root = ET.fromstring(text)
    except Exception:
        raise RuntimeError('Response text is not valid XML')

    # Collect identifiers to look for duplicates
    identifiers = []
    cnt = 0
    for i in root.iter('{http://www.openarchives.org/OAI/2.0/}identifier'):
        cnt += 1
        print('{}. {}'.format(cnt, i.text))
        identifiers.append(i.text)

    if len(identifiers) != 10:
        print('{} headers returned'.format(
            len(identifiers)))
        return

    print('list_records: Page 1 complete')

    done = False
    loop = 1
    while not done:
        time.sleep(2)
        loop += 1

        tokens = list(root.iter(
            '{http://www.openarchives.org/OAI/2.0/}resumptionToken'))
        try:
            token = tokens[0].text
        except Exception:
            raise RuntimeError('resumptionToken not found')

        # All good, now get next 10
        text = request_list_records(token=token)
        try:
            root = ET.fromstring(text)
        except Exception:
            raise RuntimeError('Response text is not valid XML')

        cnt = 0
        for i in root.iter('{http://www.openarchives.org/OAI/2.0/}identifier'):
            cnt += 1
            print('{}. {}'.format(cnt, i.text))
            if i.text in identifiers:
                raise RuntimeError('Identifier {} found previously'.format(
                    i.text))
            identifiers.append(i.text)
        if cnt != 10:
            print('{} headers returned'.format(cnt))
            done = True
        print('list_records: Page {} complete'.format(loop))


def get_record(identifier):
    text = request_get_record(identifier)

    try:
        root = ET.fromstring(text)
    except Exception:
        raise RuntimeError('Response text is not valid XML')

    record_el = root.find(
        '{http://www.openarchives.org/OAI/2.0/}GetRecord')
    records = record_el.getchildren()
    if len(records) == 0:
        print('not record found with identifier {}'.format(identifier))

    print('get_record found {} record'.format(len(records)))


if __name__ == "__main__":
    list_identifiers_bad_verb()
    list_identifiers_bad_arg()
    list_identifiers()
    list_records()
    get_record('12345/ABC')
