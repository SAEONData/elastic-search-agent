import requests
import time
import xml.etree.ElementTree as ET

serviceUrl = "http://localhost:8080"


def list_identeifiers_bad_verb():
    response = requests.get(
        url="{}/oaipmh?verb=Identifiers&metadataPrefix=oai_dc".format(serviceUrl)
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    if 'badVerb' not in response.text:
        raise RuntimeError('badVerb not found on response text')
    print('list_identeifiers_bad_verb passed')


def list_identeifiers_bad_arg():
    response = requests.get(
        url="{}/oaipmh?verb=ListIdentifiers&Prefix=oai_dc".format(serviceUrl)
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    if 'badArgument' not in response.text:
        raise RuntimeError('badArgument not found on response text')
    print('list_identeifiers_bad_arg passed')


def find_tag_contents(tag, text):
    pass


def request_list_identifiers(token=None):
    url = "{}/oaipmh?verb=ListIdentifiers&metadataPrefix=oai_dc".format(
        serviceUrl)
    if token:
        url += '&resumptionToken={}'.format(token)
    response = requests.get(url=url)
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    return response.text


def list_identifiers():
    text = request_list_identifiers()

    try:
        root = ET.fromstring(text)
    except:
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

    print('Page 1 complete')

    done = False
    loop = 1
    while not done:
        time.sleep(2)
        loop += 1

        token = root.find(
            '{http://www.openarchives.org/OAI/2.0/}resumptionToken')
        try:
            token = token.text
        except:
            raise RuntimeError('resumptionToken not found')

        # All good, now get next 10
        text = request_list_identifiers(token=token)
        try:
            root = ET.fromstring(text)
        except:
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
        print('Page {} complete'.format(loop))


if __name__ == "__main__":
    # list_identeifiers_bad_verb()
    # list_identeifiers_bad_arg()
    list_identifiers()
