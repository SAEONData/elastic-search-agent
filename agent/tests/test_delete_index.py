import requests
from agent.config import metadata_index_name
from agent.config import server_url


def delete_new_index():

    data = {
        'index': metadata_index_name,
    }
    response = requests.post(
        url="{}/delete_index".format(server_url),
        params=data
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    print(response.text)
    return response.text


if __name__ == "__main__":
    delete_new_index()
