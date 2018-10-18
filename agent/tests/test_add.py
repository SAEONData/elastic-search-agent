import json
import requests
from agent.config import metadata_index_name
from agent.config import server_url


def add_a_metadata_record(collection, metadata_json, organization, record_id, infrastructures):

    data = {
        'metadata_json': json.dumps(metadata_json),
        'index': metadata_index_name,
        'collection': collection,
        'infrastructures': infrastructures,
        'organization': organization,
        'record_id': record_id,
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


def add_metadata_records(repeats=1):
    cnt = 0
    for rep in range(repeats):
        for data in JSON_DICTS:
            cnt += 1
            # print('Add record {}'.format(cnt))
            add_a_metadata_record(
                collection=data['collection'],
                infrastructures=data['infrastructures'],
                metadata_json=data['metadata_json'],
                organization=data['organization'],
                record_id=data['record_id'],
            )


JSON_DICTS = [{
    'record_id': '1001',
    'organization': 'WebTide',
    'collection': '1000',
    'infrastructures': ['SASDI', 'SANSA'],
    'metadata_json': {
        "identifier": {
            "identifier": "10.5072/example-full",
            "identifierType": "DOI"
        },
        "creators": [
            {
                "creatorName": "Miller, Elizabeth",
                "nameType": "Personal",
                "givenName": "Elizabeth",
                "familyName": "Miller",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "0000-0001-5000-0007",
                        "nameIdentifierScheme": "ORCID",
                        "schemeURI": "http://orcid.org/"
                    }
                ],
                "affiliations": [
                    {
                        "affiliation": "DataCite"
                    }
                ]
            }
        ],
        "titles": [
            {
                "title": "Full DataCite XML Example"
            },
            {
                "title": "Demonstration of DataCite Properties.",
                "titleType": "Subtitle"
            }
        ],
        "publisher": "DataCite",
        "publicationYear": "2014",
        "subjects": [
            {
                "subject": "000 computer science",
                "subjectScheme": "dewey",
                "schemeURI": "http://dewey.info/"
            }
        ],
        "contributors": [
            {
                "contributorType": "ProjectLeader",
                "contributorName": "Starr, Joan",
                "givenName": "Joan",
                "familyName": "Starr",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "0000-0002-7285-027X",
                        "nameIdentifierScheme": "ORCID",
                        "schemeURI": "http://orcid.org/"
                    }
                ],
                "affiliations": [
                    {
                        "affiliation": "California Digital Library"
                    }
                ]
            }
        ],
        "dates": [
            {
                "date": "2017-09-13",
                "dateType": "Updated",
                "dateInformation": "Updated with 4.1 properties"
            },
            {
                "date": "2018-09-21",
                "dateType": "Collected"
            }
        ],
        "language": "en-US",
        "resourceType": {
            "resourceTypeGeneral": "Software",
            "resourceType": "XML"
        },
        "alternateIdentifiers": [
            {
                "alternateIdentifier": "https://schema.datacite.org/meta/kernel-4.1/example/datacite-example-full-v4.1.xml",
                "alternateIdentifierType": "URL"
            }
        ],
        "relatedIdentifiers": [
            {
                "relatedIdentifier": "https://data.datacite.org/application/citeproc+json/10.5072/example-full",
                "relatedIdentifierType": "URL",
                "relationType": "HasMetadata",
                "relatedMetadataScheme": "citeproc+json",
                "schemeURI": "https://github.com/citation-style-language/schema/raw/master/csl-data.json"
            },
            {
                "relatedIdentifier": "arXiv:0706.0001",
                "relatedIdentifierType": "arXiv",
                "relationType": "IsReviewedBy",
                "resourceTypeGeneral": "Text"
            }
        ],
        "sizes": [
            {
                "size": "4 kB"
            }
        ],
        "formats": [
            {
                "format": "application/xml"
            }
        ],
        "version": "4.1",
        "rightsList": [
            {
                "rights": "CC0 1.0 Universal",
                "rightsURI": "http://creativecommons.org/publicdomain/zero/1.0/"
            }
        ],
        "descriptions": [
            {
                "description": "XML example of all DataCite Metadata Schema v4.1 properties.",
                "descriptionType": "Abstract"
            }
        ],
        "geoLocations": [
            {
                "geoLocationPlace": "Atlantic Ocean",
                "geoLocationPoint": {
                    "pointLongitude": "-67.302",
                    "pointLatitude": "31.233"
                },
                "geoLocationBox": {
                    "westBoundLongitude": "-71.032",
                    "eastBoundLongitude": "-68.211",
                    "southBoundLatitude": "41.090",
                    "northBoundLatitude": "42.893"
                },
                "geoLocationPolygons": [
                    {
                        "polygonPoints": [
                            {
                                "pointLatitude": "41.991",
                                "pointLongitude": "-71.032"
                            },
                            {
                                "pointLatitude": "42.893",
                                "pointLongitude": "-69.622"
                            },
                            {
                                "pointLatitude": "41.991",
                                "pointLongitude": "-68.211"
                            },
                            {
                                "pointLatitude": "41.090",
                                "pointLongitude": "-69.622"
                            },
                            {
                                "pointLatitude": "41.991",
                                "pointLongitude": "-71.032"
                            }
                        ]
                    }
                ]
            }
        ],
        "fundingReferences": [
            {
                "funderName": "National Science Foundation",
                "funderIdentifier": "https://doi.org/10.13039/100000001",
                "funderIdentifierType": "Crossref Funder ID",
                "awardNumber": "CBET-106",
                "awardTitle": "Full DataCite XML Example"
            }
        ],
        "immutableResource": {
            "resourceURL": "https://schema.datacite.org/meta/kernel-4.1/example/datacite-example-full-v4.1.xml",
            "resourceChecksum": "1f4d92f643bf831131f7bd26bdb6d3e3",
            "checksumAlgorithm": "md5",
            "resourceName": "Full DataCite XML Example",
            "resourceDescription": "A complete example of a DataCite 4.1-compliant XML metadata record"
        },
        "linkedResources": [
            {
                "linkedResourceType": "Information",
                "resourceURL": "https://schema.datacite.org/meta/kernel-4.1/doc/DataCite-MetadataKernel_v4.1.pdf",
                "resourceName": "DataCite 4.1 Specification",
                "resourceDescription": "DataCite metadata schema documentation for the publication and citation of research data"
            }
        ],
        "originalMetadata": "<?xml version=\"1.0\"?><resource>...the original metadata...</resource>"
    }
},{
    'record_id': '1002',
    'organization': 'WebTide',
    'collection': '1000',
    'infrastructures': ['SASDI', 'SANSA'],
    'metadata_json': {
        "identifier": {
            "identifier": "10.5072/example-full",
            "identifierType": "DOI"
        },
        "creators": [
            {
                "creatorName": "Miller, Elizabeth",
                "nameType": "Personal",
                "givenName": "Elizabeth",
                "familyName": "Miller",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "0000-0001-5000-0007",
                        "nameIdentifierScheme": "ORCID",
                        "schemeURI": "http://orcid.org/"
                    }
                ],
                "affiliations": [
                    {
                        "affiliation": "DataCite"
                    }
                ]
            }
        ],
        "titles": [
            {
                "title": "Full DataCite XML Example"
            },
            {
                "title": "Demonstration of DataCite Properties.",
                "titleType": "Subtitle"
            }
        ],
        "publisher": "DataCite",
        "publicationYear": "2014",
        "subjects": [
            {
                "subject": "000 computer science",
                "subjectScheme": "dewey",
                "schemeURI": "http://dewey.info/"
            }
        ],
        "contributors": [
            {
                "contributorType": "ProjectLeader",
                "contributorName": "Starr, Joan",
                "givenName": "Joan",
                "familyName": "Starr",
                "nameIdentifiers": [
                    {
                        "nameIdentifier": "0000-0002-7285-027X",
                        "nameIdentifierScheme": "ORCID",
                        "schemeURI": "http://orcid.org/"
                    }
                ],
                "affiliations": [
                    {
                        "affiliation": "California Digital Library"
                    }
                ]
            }
        ],
        "dates": [
            {
                "date": "2017-09-13",
                "dateType": "Updated",
                "dateInformation": "Updated with 4.1 properties"
            },
            {
                "date": "2018-09-21",
                "dateType": "Collected"
            }
        ],
        "language": "en-US",
        "resourceType": {
            "resourceTypeGeneral": "Software",
            "resourceType": "XML"
        },
        "alternateIdentifiers": [
            {
                "alternateIdentifier": "https://schema.datacite.org/meta/kernel-4.1/example/datacite-example-full-v4.1.xml",
                "alternateIdentifierType": "URL"
            }
        ],
        "relatedIdentifiers": [
            {
                "relatedIdentifier": "https://data.datacite.org/application/citeproc+json/10.5072/example-full",
                "relatedIdentifierType": "URL",
                "relationType": "HasMetadata",
                "relatedMetadataScheme": "citeproc+json",
                "schemeURI": "https://github.com/citation-style-language/schema/raw/master/csl-data.json"
            },
            {
                "relatedIdentifier": "arXiv:0706.0001",
                "relatedIdentifierType": "arXiv",
                "relationType": "IsReviewedBy",
                "resourceTypeGeneral": "Text"
            }
        ],
        "sizes": [
            {
                "size": "4 kB"
            }
        ],
        "formats": [
            {
                "format": "application/xml"
            }
        ],
        "version": "4.1",
        "rightsList": [
            {
                "rights": "CC0 1.0 Universal",
                "rightsURI": "http://creativecommons.org/publicdomain/zero/1.0/"
            }
        ],
        "descriptions": [
            {
                "description": "XML example of all DataCite Metadata Schema v4.1 properties.",
                "descriptionType": "Abstract"
            }
        ],
        "geoLocations": [
            {
                "geoLocationPlace": "Atlantic Ocean",
                "geoLocationPoint": {
                    "pointLongitude": "-67.302",
                    "pointLatitude": "31.233"
                },
                "geoLocationBox": {
                    "westBoundLongitude": "-71.032",
                    "eastBoundLongitude": "-68.211",
                    "southBoundLatitude": "41.090",
                    "northBoundLatitude": "42.893"
                },
                "geoLocationPolygons": [
                    {
                        "polygonPoints": [
                            {
                                "pointLatitude": "41.991",
                                "pointLongitude": "-71.032"
                            },
                            {
                                "pointLatitude": "42.893",
                                "pointLongitude": "-69.622"
                            },
                            {
                                "pointLatitude": "41.991",
                                "pointLongitude": "-68.211"
                            },
                            {
                                "pointLatitude": "41.090",
                                "pointLongitude": "-69.622"
                            },
                            {
                                "pointLatitude": "41.991",
                                "pointLongitude": "-71.032"
                            }
                        ]
                    }
                ]
            }
        ],
        "fundingReferences": [
            {
                "funderName": "National Science Foundation",
                "funderIdentifier": "https://doi.org/10.13039/100000001",
                "funderIdentifierType": "Crossref Funder ID",
                "awardNumber": "CBET-106",
                "awardTitle": "Full DataCite XML Example"
            }
        ],
        "immutableResource": {
            "resourceURL": "https://schema.datacite.org/meta/kernel-4.1/example/datacite-example-full-v4.1.xml",
            "resourceChecksum": "1f4d92f643bf831131f7bd26bdb6d3e3",
            "checksumAlgorithm": "md5",
            "resourceName": "Full DataCite XML Example",
            "resourceDescription": "A complete example of a DataCite 4.1-compliant XML metadata record"
        },
        "linkedResources": [
            {
                "linkedResourceType": "Information",
                "resourceURL": "https://schema.datacite.org/meta/kernel-4.1/doc/DataCite-MetadataKernel_v4.1.pdf",
                "resourceName": "DataCite 4.1 Specification",
                "resourceDescription": "DataCite metadata schema documentation for the publication and citation of research data"
            }
        ],
        "originalMetadata": "<?xml version=\"1.0\"?><resource>...the original metadata...</resource>"
    }
},
]

if __name__ == "__main__":
    add_metadata_records()
