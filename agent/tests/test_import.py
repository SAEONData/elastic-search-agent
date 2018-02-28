import json
import requests
from agent.config import import_url
from agent.config import import_user
from agent.config import import_password
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
    paths = ["Institutions/coj/coj/metadata", ]
    paths = ["Portals/test-mike/testcustmike/metadata", ]
    for path in paths:
        records = import_metadata_record(path)
        records = json.loads(records)
        for record in records['content']:
            add_a_metadata_record(record['jsonData'])
            import pdb; pdb.set_trace()


if __name__ == "__main__":
    import_metadata_records()
