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
              'includes': '-34.12 18.46 -34.14 18.49'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'includes': '-34.08 18.46 -34.08 18.46'},
     'num_results': 2},
    {'data': {'index': metadata_index_name,
              'includes': '-32 18.46 -34 18.49'},
     'num_results': 0},
    {'data': {'index': metadata_index_name,
              'encloses': '-34 16 -35 19'},
     'num_results': 2},
    {'data': {'index': metadata_index_name,
              'overlaps': '-34 16 -35 19'},
     'num_results': 2},
    {'data': {'index': metadata_index_name,
              'overlaps': '-34.08 18.4 -34.09 18.41'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'overlaps': '-35.08 18.4 -35.09 18.41'},
     'num_results': 0},
    {'data': {'index': metadata_index_name,
              'excludes': '-34 16 -35 19'},
     'num_results': 0},
    {'data': {'index': metadata_index_name,
              'excludes': '-34.08 18.4 -34.09 18.41'},
     'num_results': 1},
    {'data': {'index': metadata_index_name,
              'excludes': '-35.08 18.4 -35.09 18.41'},
     'num_results': 2},
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
        if not result:
            import pdb; pdb.set_trace()
        print()
