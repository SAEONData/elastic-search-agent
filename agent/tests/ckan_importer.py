import argparse
from datetime import datetime
import json
import requests
import time

import_to_ckan = True
import_to_agent = False

# Constants
# Source config
src_base_url = 'http://qa.dirisa.org'
# src_base_url = 'http://oa.dirisa.org'
# src_base_url = 'http://localhost:8080/SAEON'

# ES destination config
agent_base_url = 'http://localhost:9210'
agent_user = 'admin'
metadata_index_name = 'md_index_1'

# CKAN destination config
ckan_base_url = 'http://ckan.dirisa.org:9090'
ckan_user = 'mike@webtide.co.za'


def gen_unique_id():
    return datetime.now().strftime("%Y%m%d%H%M%S%f")


def get_physical_path(url):
    idx = 3
    if 'localhost' in url:
        idx = 4
    return '/'.join(url.split('/')[idx:])


def add_a_record_to_ckan(collection, metadata_json, organization, record_id, infrastructures, state):

    data = {
        'jsonData': json.dumps(metadata_json),
        'metadataType': 'datacite-4-1',
        'workflowState': state,
    }
    url = "{}/Institutions/{}/{}-repository/metadata/jsonCreateMetadataAsJson".format(
        ckan_base_url, organization['id'], organization['id'])
    response = requests.post(
        url=url,
        params=data,
        auth=requests.auth.HTTPBasicAuth(
            creds['dest_user'], creds['dest_pwd'])
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))
    result = json.loads(response.text)
    if result['status'] == 'success':
        if check_ckan_added(organization, result):
            print('Added Successfully')
        else:
            print('Record not found')
        if result['workflow_status'] == 'failed':
            print('But workflow failed: {}'.format(result['workflow_msg']))
    else:
        print(result)
    return result


def add_a_record_to_elastic(collection, metadata_json, organization, record_id, infrastructures):

    data = {
        'metadata_json': json.dumps(metadata_json),
        'index': metadata_index_name,
        'collection': collection,
        'infrastructures': infrastructures,
        'organization': organization['title'],
        'record_id': record_id,
    }
    url = "{}/add".format(agent_base_url)
    response = requests.post(
        url=url,
        params=data,
        auth=requests.auth.HTTPBasicAuth(
            creds['dest_user'], creds['dest_pwd'])
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))
    result = json.loads(response.text)
    if result['success']:
        if import_to_agent:
            if check_agent_added(organization, record_id):
                print('Added Successfully')
            else:
                print('Record not found')
    else:
        print(result)
    return result


def check_ckan_added(organization, result):

    # Find the record via jsonContent
    record_id = result['uid']
    data = {
        'types': 'Metadata',
    }
    url = "{}/Institutions/{}/{}-repository/metadata/jsonContent".format(
        ckan_base_url, organization['id'], organization['id'])
    response = requests.get(
        url=url,
        params=data,
        auth=requests.auth.HTTPBasicAuth(
            creds['dest_user'], creds['dest_pwd'])
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))
    # print(response.text)
    found = False
    result = json.loads(response.text)
    print(len(result))
    for record in result:
        if record['id'] == record_id:
            found = True
    return found


def check_agent_added(organization, record_id):

    time.sleep(1)
    data = {
        'record_id': record_id,
        'index': metadata_index_name,
    }
    url = "{}/search".format(agent_base_url)
    response = requests.get(
        url=url,
        params=data,
        auth=requests.auth.HTTPBasicAuth(
            creds['dest_user'], creds['dest_pwd'])
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))
    # print(response.text)
    result = json.loads(response.text)
    if not result['success']:
        raise RuntimeError('Request failed with response: %s' % (
            result.get('msg', 'Dunno')))
    if result['result_length'] == 0:
        print(record_id)
        return False

    return True


def get_metadata_records(path, creds):

    url = "{}/{}/jsonGetRecords".format(src_base_url, path)
    # print(url)
    response = requests.get(
        url=url,
        auth=requests.auth.HTTPBasicAuth(
            creds['src_user'], creds['src_pwd'])
    )
    if response.status_code != 200:
        return 'Request failed with return code: %s' % (
            response.status_code)

    return response.text


def get_institutions(creds):

    url = "{}/Institutions/jsonContent?types=Institution".format(src_base_url)
    # print(url)
    response = requests.get(
        url=url,
        auth=requests.auth.HTTPBasicAuth(
            creds['src_user'], creds['src_pwd'])
    )
    if response.status_code != 200:
        return 'Request failed with return code: %s' % (
            response.status_code)

    results = json.loads(response.text)
    institutions = []
    for result in results:
        inst = {
            'id': result['id'],
            'title': result['title'],
            'path': get_physical_path(result['context_path'])
        }
        institutions.append(inst)
    return institutions


def get_metadata_collections(inst, creds, log_data):

    url = "{}/{}/jsonContent?types=MetadataCollection&depth=-1".format(
        src_base_url, inst['path'])
    # print(url)
    response = requests.get(
        url=url,
        auth=requests.auth.HTTPBasicAuth(
            creds['src_user'], creds['src_pwd'])
    )
    if response.status_code != 200:
        msg = 'Find collections in {} failed -> {}'.format(
            inst['path'], response.status_code)
        log_info(log_data, 'repo', msg)
        return []

    results = json.loads(response.text)
    paths = []
    for result in results:
        if result['id'] == 'templates':
            continue
        paths.append(get_physical_path(result['context_path']))
    return paths


def create_institution(inst):
    url = "{}/Institutions/jsonCreateInstitution?title={}".format(
        ckan_base_url, inst['title'])
    print(url)
    response = requests.post(
        url=url,
        auth=requests.auth.HTTPBasicAuth(
            creds['dest_user'], creds['dest_pwd'])
    )
    if response.status_code != 200:
        msg = 'Request failed with return code: %s' % (
            response.status_code)
        return {'status': 'failed', 'msg': [{'name': msg}]}

    results = json.loads(response.text)
    return results


def log_info(log_data, atype, msg):
    if not log_data.get(atype):
        log_data[atype] = []
    log_data[atype].append(msg)


def import_metadata_records(inst, creds, paths, log_data):
    for path in paths:
        records = get_metadata_records(path, creds)
        if records.startswith('Request failed'):
            msg = '\n### {}: {}\n'.format(path, records)
            log_info(log_data, 'import', msg)
            continue
        if records.startswith('There is nothing here'):
            msg = '\n### Unauthorised to access {}\n'.format(path)
            log_info(log_data, 'import', msg)
            continue
        records = json.loads(records)
        if len(records['content']) == 0:
            msg = '\n### No records found in {}\n'.format(path)
            log_info(log_data, 'import', msg)
            continue
        if import_to_ckan:
            response = create_institution(inst)
            if response['status'] == 'failed' and \
               not response['msg']['name'][0].startswith(
                    'Group name already exists'):
                log_info(log_data, 'institution', response['msg'])
                continue
        for record in records['content']:
            record_id = None
            if isinstance(record['jsonData']['identifier'], dict):
                record_id = record['jsonData']['identifier'].get('identifier')
            if not record_id:
                record_id = gen_unique_id()
                record['jsonData']['identifier'] = {
                    'identifier': record_id,
                    'identifierType': 'internal'
                }

            # Ignore problematic records
            if record.get('status') not in ['private', 'provisional']:
                log_info(log_data, 'add_record', {
                    'action': 'ignored',
                    'record_id': record_id,
                    'state': record.get('status')})
                continue

            log_info(log_data, 'add_record', {
                'action': 'add',
                'record_id': record_id,
                'state': record.get('status', 'dunno')})
            if import_to_agent:
                add_a_record_to_elastic(
                    record_id=record_id,
                    organization=inst,
                    infrastructures=['SASDI', 'SANSA'],
                    metadata_json=record['jsonData'],
                    collection='TestImport2',
                )
            if import_to_ckan:
                add_a_record_to_ckan(
                    record_id=record_id,
                    organization=inst,
                    infrastructures=[],
                    metadata_json=record['jsonData'],
                    collection='',
                    state='plone-{}'.format(record.get('status'))
                )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--src-user", required=False, help="user name for source")
    parser.add_argument("--src-pwd", required=True, help="admin password for source")
    parser.add_argument("--dest-user", required=False, help="user name for destination")
    parser.add_argument("--dest-pwd", required=True, help="admin password for destination")
    parser.add_argument("--import-to-ckan", required=False, help="import metadata to CKAN")
    parser.add_argument("--import-to-agent", required=False, help="import metadata to the SAEON Metadata Agent")
    parser.add_argument("--ckan-base-url", required=False, help="the URL of the CKAN instance")
    parser.add_argument("--agent-base-url", required=False, help="the URL of the SAEON Metadata Agent")

    args = parser.parse_args()
    creds = {
        'src_pwd': args.src_pwd,
        'dest_pwd': args.dest_pwd,
        'src_user': args.src_user or 'admin',
        'dest_user': args.dest_user or 'admin',
    }
    print(creds)
    if args.import_to_ckan:
        import_to_ckan = str(args.import_to_ckan).lower() == 'true'
    print(import_to_ckan)

    if args.import_to_agent:
        import_to_agent = str(args.import_to_agent).lower() == 'true'
    print(import_to_agent)

    institutions = get_institutions(creds)
    log_data = {}
    for inst in institutions:
        paths = get_metadata_collections(inst, creds, log_data)
        if paths:
            # added records to inst
            import_metadata_records(inst, creds, paths, log_data)

    # Get states from logged data
    states = {}
    ignored = 0
    added = 0
    for log in log_data.get('add_record', []):
        if log['action'] == 'ignored':
            ignored += 1
        else:
            added += 1

        # Prime dict
        if log['state'] not in states.keys():
            states[log['state']] = 0
        states[log['state']] += 1

    print('States: {}'.format(states))
    print('Ignored: {}'.format(ignored))
    print('Added: {}'.format(added))
