import requests
from agent.config import server_url


def delete_all():
    response = requests.get(
        url="{}/delete_all".format(server_url),
    )
    if response.status_code != 200:
        raise RuntimeError('Request failed with return code: %s' % (
            response.status_code))

    print(response.text)
    return response.text

if __name__ == "__main__":
    delete_all()
