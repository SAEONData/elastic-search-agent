import json
import requests
from agent.config import server_url


def add_a_metadata_record(data):

    data = {
        'record': json.dumps(data)
    }
    response = requests.post(
        url="{}/add".format(server_url),
        params=data
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    print(response.text)
    return response.text


def add_metadata_records():
    for data in JSON_DICTS:
        add_a_metadata_record(data)


JSON_DICTS = [
    {
        "subtitle": "",
        "contributors": [],
        "userId": "",
        "xsiSchema": "http://datacite.org/schema/kernel-3",
        "owner": "",
        "subjects": [
            {
                "subjectScheme": "",
                "schemeURI": "",
                "subject": "SOC"
            }
        ],
        "additionalFields": {
            "onlineResources": [
            ],
            "coverageBegin": "",
            "coverageEnd": ""
        },
        "geoLocations": [
            {
                "geoLocationBox": "-34.83416999999997 16.451910000000055 -22.12502999999998 32.944980000000044"
            }
        ],
        "userVersion": "",
        "description": [
            {
                "descriptionType": "",
                "description": "Soil Organic Carbon (SOC) represents all the organic carbon in the soil to a depth of 1m. SOC is derived from the data provided by the Africa Soil Information System (AfSIS). In this case the organic carbon of the top 300mm of the natural soil was NOT reduced by land cover factors.\n\nUnits: average gC/m2 within 1km x 1km pixel"
            }
        ],
        "publicationYear": "2017",
        "relatedIdentifiers": [],
        "creators": [
            {
                "creatorName": "Prof J Soap",
                "affiliation": "UCT"
            },
            {
                "creatorName": "Prof MJ Mets",
                "affiliation": "UWC"
            }
        ],
        "publisher": "UCT",
        "dates": [
            {
                "date": "",
                "dateType": "Accepted"
            },
            {
                "date": "",
                "dateType": "Issued"
            }
        ],
        "language": "eng",
        "rights": [
            {
                "rights": "",
                "rightsURI": ""
            }
        ],
        "resourceType": "on",
        "sizes": [],
        "resourceTypeGeneral": "",
        "bounds": [],
        "alternateIdentifiers": [],
        "titles": [
            {
                "titleType": "",
                "title": "Soil Organic Carbon Unperturbed (gC/sq.m)"
            }
        ],
        "identifier": {
            "identifier": "12345/ABC",
            "identifierType": "DOI"
        }
    },
    {
        "subtitle": "",
        "contributors": [],
        "userId": "",
        "xsiSchema": "http://datacite.org/schema/kernel-3",
        "owner": "",
        "subjects": [
            {
                "subjectScheme": "",
                "schemeURI": "",
                "subject": "SOC"
            },
            {
                "subjectScheme": "",
                "schemeURI": "",
                "subject": "COS"
            }
        ],
        "additionalFields": {
            "onlineResources": [
            ],
            "coverageBegin": "",
            "coverageEnd": ""
        },
        "geoLocations": [
            {
                "geoLocationBox": "-34.83416999999997 16.451910000000055 -22.12502999999998 32.944980000000044"
            }
        ],
        "userVersion": "",
        "description": [
            {
                "descriptionType": "",
                "description": "Soil Organic Carbon (SOC) represents all the organic carbon in the soil to a depth of 1m. SOC is derived from the data provided by the Africa Soil Information System (AfSIS). In this case the organic carbon of the top 300mm of the natural soil was NOT reduced by land cover factors.\n\nUnits: average gC/m2 within 1km x 1km pixel"
            }
        ],
        "publicationYear": "2017",
        "relatedIdentifiers": [],
        "creators": [
            {
                "creatorName": "Prof J Soap",
                "affiliation": "UCT"
            }
        ],
        "publisher": "UCT",
        "dates": [
            {
                "date": "",
                "dateType": "Accepted"
            },
            {
                "date": "",
                "dateType": "Issued"
            }
        ],
        "language": "eng",
        "rights": [
            {
                "rights": "",
                "rightsURI": ""
            }
        ],
        "resourceType": "on",
        "sizes": [],
        "resourceTypeGeneral": "",
        "bounds": [],
        "alternateIdentifiers": [],
        "titles": [
            {
                "titleType": "",
                "title": "Soil Organic Carbon Unperturbed (gC/sq.m)"
            }
        ],
        "identifier": {
            "identifier": "12345/XYZ",
            "identifierType": "DOI"
        }
    }
]

if __name__ == "__main__":
    add_metadata_records()
