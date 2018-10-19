import json
import requests
from agent.config import metadata_index_name
from agent.config import server_url

TESTS = [
    {'data': {'index': metadata_index_name},
     'num_results': 2},
    {'data': {'index': metadata_index_name,
              'metadata_json.creators.creatorName': 'jane'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'from': '2019-01-01'},
     'num_results': 0},
    {'data': {'index': metadata_index_name,
              'from': '2018-06-01'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'includes': '31,31,31,31'},
     'num_results': 0},
    {'data': {'index': metadata_index_name,
              'includes': '41.5,-70,41.6,-69'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'encloses': '41.6,-70,41.4,-69'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'overlaps': '41.6,-70,41.4,-69'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'overlaps': '-41.4,70,-41.6,69'},
     'num_results': 1},
]


def search(test):
    data = test['data']

    response = requests.post(
        url="{}/search".format(server_url),
        params=data
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    result = json.loads(response.text)

    if not result['success']:
        print(result['error'])
        return False
    if test['num_results'] == result['result_length']:
        return True

    print('Expected {} != Found {}'.format(
        test['num_results'], result['result_length']))
    return False


if __name__ == "__main__":
    for test in TESTS:
        result = search(test)
        print('{}: {}'.format(result, test['data']))
