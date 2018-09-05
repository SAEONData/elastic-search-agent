import json
import requests
import time
from agent.config import import_url
from agent.config import import_user
from agent.config import import_password
from agent.utils import gen_unique_id
from test_add import add_a_metadata_record


def import_metadata_record(path):

    url = "{}/{}/jsonGetRecords".format(import_url, path)
    print(url)
    response = requests.get(
        url=url,
        auth=requests.auth.HTTPBasicAuth(
            import_user, import_password)
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    return response.text


def import_metadata_records():
    paths = []
    # paths.append("Institutions/coj/coj/metadata")
    # paths.append("Portals/test-mike/testcustmike/metadata")
    # paths.append('Institutions/coj/coj/metadata')
    paths.append("Institutions/sa-risk-and-vulnerability-atlas/sarva/metadata")
    # paths.append("Institutions/geoss/geoss/metadata")
    # paths.append("Institutions/south-african-national-biodiversity-institute/south-african-national-biodiversity-institute")
    for path in paths:
        records = import_metadata_record(path)
        if records.startswith('There is nothing here'):
            print('\n### Unauthorised to access {}\n'.format(path))
            continue
        records = json.loads(records)
        for record in records['content']:
            time.sleep(0.5)
            record_id = record['jsonData']['identifier']['identifier']
            if not record_id:
                record_id = gen_unique_id()
                record['jsonData']['identifier']['identifier'] = record_id
                record['jsonData']['identifier']['identifierType'] = 'internal'
            add_a_metadata_record(
                record_id=record_id,
                organization='WebTide',
                infrustructures=['SASDI', 'SANSA'],
                metadata_json=record['jsonData'],
                collection='TestImport'
            )


if __name__ == "__main__":
    import_metadata_records()
