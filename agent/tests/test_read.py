import json
import requests

serviceUrl = "http://localhost:8080"


def read_all():
    response = requests.get(
        url="{}/read".format(serviceUrl)
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    try:
        text = json.loads(response.text)
    except Exception as e:
        raise RuntimeError('response is not valid json: {}'.format(e))
    print(text)

    if not text.get('success', False):
        raise RuntimeError('response success is false')
    if not text.get('results', False):
        raise RuntimeError('response has no results')
    for result in text['results']:
        print('Title: {}'.format(result['title']))
        print(result.get('content', 'No Content'))
        print
    return text


if __name__ == "__main__":
    read_all()
