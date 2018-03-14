import cherrypy
import json
import datetime


def gen_unique_id():
    return datetime.now().strftime("%Y%m%d%H%M%S%f")


class _JSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.date):
            return obj.isoformat()
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
