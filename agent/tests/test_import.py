import json
import requests
import time
from datetime import datetime
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


def gen_unique_id():
    return datetime.now().strftime("%Y%m%d%H%M%S%f")


def import_metadata_records():
    # paths = ["Institutions/coj/coj/metadata", ]
    # "Portals/test-mike/testcustmike/metadata", ]
    paths = ['Institutions/coj/coj/metadata', ]
    for path in paths:
        records = import_metadata_record(path)
        records = json.loads(records)
        for record in records['content']:
            time.sleep(1)
            record['jsonData']['identifier']['identifier'] = gen_unique_id()
            add_a_metadata_record(record['jsonData'], set_spec='Import')


if __name__ == "__main__":
    import_metadata_records()
