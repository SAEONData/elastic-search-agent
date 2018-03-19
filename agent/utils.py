import cherrypy
import json
from datetime import datetime


def gen_unique_id():
    return datetime.now().strftime("%Y%m%d%H%M%S%f")


def iso_8601(adate):
    return adate.strftime("%Y-%m-%dT%H:%M:%SZ")


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return iso_8601(obj)
        return super().default(obj)
    def iterencode(self, value):
        # Adapted from cherrypy/_cpcompat.py
        for chunk in super().iterencode(value):
            yield chunk.encode("utf-8")

json_encoder = _JSONEncoder()


def json_handler(*args, **kwargs):
    # Adapted from cherrypy/lib/jsontools.py
    value = cherrypy.serving.request._json_inner_handler(*args, **kwargs)
    return json_encoder.iterencode(value)


def get_request_host(request):
    host = request.headers.get('hostd')
    if not host:
        host = request.headers.get('host')
    return host
