import json
import requests
from agent.config import metadata_index_name
from agent.config import server_url


def create_new_index():

    data = {
        'record': json.dumps(JSON_DICT),
        'index': metadata_index_name,
    }
    response = requests.post(
        url="{}/create_index".format(server_url),
        params=data
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    print(response.text)
    return response.text


JSON_DICT = {
    'name': '1000-100-1',
    'owner_org': 'WebTide',
    'metadata_collection_id': '1000',
    'infrustructures': ['SASDI', 'SANSA'],
    'metadata_json': {
        'additionalFields': {
            'coverageBegin': '',
            'coverageEnd': '',
            'onlineResources': [
                {'desc': 'Original Metadata Record',
                 'func': 'metadata',
                 'href': 'http://qa.dirisa.org/Portals/test-mike/testcustmike/metadata/metadata.2018-02-28.9981326861/getOriginalXml',
                 'name': 'Original Metadata Record'}],
            'source_uri': '',
            'status': 'complete'
        },
        'alternateIdentifiers': [{
            'alternateIdentifier': 'http://schema.datacite.org/schema/meta/kernel-3.1/example/datacite-example-full-v3.1.xml',
            'alternateIdentifierType': 'URL'}],
        'bounds': [-68.302, 30.233, -66.302, 32.233000000000004],
        'contributors': [{
            'affiliation': 'California Digital Library',
            'contributorName': 'Starr, Joan',
            'contributorType': 'ProjectLeader',
            'nameIdentifier': '0000-0002-7285-027X',
            'nameIdentifierScheme': 'ORCID',
            'schemeURI': 'http://orcid.org/'}],
        'creators': [{
            'affiliation': 'DataCite',
            'creatorName': 'Miller, Elizabeth',
            'nameIdentifier': '0000-0001-5000-0007',
            'nameIdentifierScheme': 'ORCID',
            'schemeURI': 'http://orcid.org/'}],
        'dates': [{'date': '2018-01-01/2018-12-31',
                   'dateType': 'Collected'}],
        'description': [{
            'description': 'XML example of all DataCite '
                           'Metadata Schema v3.1 properties.',
            'descriptionType': 'Abstract'}],
        'errors': [],
        'formats': ['application/xml'],
        'geoLocations': [{
            "geoLocationBox": "-30.83 18.45 -22.125 32.94",
            'geoLocationPlace': 'Atlantic Ocean',
            'geoLocationPoint': '-30.233 31.302'
        }],
        'identifier': {
            'identifier': 'example-record', 'identifierType': 'DOI'},
        'language': 'en-us',
        'publicationYear': '2014',
        'publisher': 'DataCite',
        'relatedIdentifiers': [{
            'relatedIdentifier': 'http://data.datacite.org/application/citeproc+json/10.5072/example-full',
            'relatedIdentifierType': 'URL',
            'relatedMetadataScheme': 'citeproc+json',
            'relatedType': 'IsMetadataFor',
            'relationType': 'HasMetadata',
            'schemeType': '',
            'schemeURI': 'https://github.com/citation-style-language/schema/raw/master/csl-data.json'},
            {
            'relatedIdentifier': 'arXiv:0706.0001',
            'relatedIdentifierType': 'arXiv',
            'relatedMetadataScheme': '',
            'relatedType': 'IsMetadataFor',
            'relationType': 'IsReviewedBy',
            'schemeType': '',
            'schemeURI': ''}],
        'resourceType': 'XML',
        'resourceTypeGeneral': 'Software',
        'rights': [{
            'rights': 'CC0 1.0 Universal',
            'rightsURI': 'http://creativecommons.org/publicdomain/zero/1.0/'}],
        'schemaSpecific': {},
        'sizes': ['3KB', '25ml'],
        'subjects': [{
            'schemeURI': 'http://dewey.info/',
            'subject': 'computer science',
            'subjectScheme': 'dewey'}],
        'subtitle': '',
        'title': 'Full DataCite XML Example',
        'titles': [
            {'title': 'Full DataCite XML Example', 'titleType': ''},
            {'title': 'Demonstration of DataCite Properties.',
             'titleType': 'Subtitle'}],
        'version': '3.1',
        'xsiSchema': 'http://datacite.org/schema/kernel-3 '
                     'http://schema.datacite.org/meta/kernel-3/metadata.xsd'
    }
}

if __name__ == "__main__":
    create_new_index()
