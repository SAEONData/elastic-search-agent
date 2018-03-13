#!/usr/bin/env python

import cherrypy
import json
from elasticsearch_dsl import Search
from agent.config import metadata_index_name
from agent.config import token_index_name
from agent.oaipmh import process_request
from agent.persist import Metadata
from agent.search import FacetedSearch
from agent.search import search_all
from agent.search import search


class AgentAPI(object):

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def add(self, **kwargs):
        cherrypy.log(str(kwargs))
        output = {'success': False}
        record = kwargs.get('record')
        if record is None:
            msg = "Error: 'record' argument is required"
            output['msg'] = msg
            return output
        try:
            record = json.loads(record)
        except Exception as e:
            msg = "Error: json format {}".format(e)
            output['msg'] = msg
            return output

        set_spec = kwargs.get('set_spec', '')

        # Hacks to fix records
        identifier = record.get('identifier')
        if identifier == '':
            record['identifier'] = {}

        # Metadata.init()
        try:
            md = Metadata(record=record, set_spec=set_spec)
        except Exception as e:
            msg = "Error: {}: {}".format('Creation failed', e)
            output['msg'] = msg
            return output

        try:
            md.save()
        except Exception as e:
            msg = "Error: {}: {}".format('Save failed', e)
            output['msg'] = msg
            return output

        output['success'] = True
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_all(self):
        output = {'success': False}
        cherrypy.log('delete_all')
        s = Search(index=metadata_index_name)
        s.delete()
        s = Search(index=token_index_name)
        s.delete()
        output['success'] = True
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def read(self, **kwargs):
        output = {'success': False}
        if kwargs:
            new_kwargs = {}
            for k in kwargs:
                new_k = 'record.{}'.format(k)
                new_kwargs[new_k] = kwargs[k]
            response = search(**new_kwargs)
        else:
            response = search_all()
        lines = []
        for hit in response:
            # lines.append(json.dumps(hit.to_dict(), default=str))
            lines.append(hit.to_dict())

        output['success'] = True
        output['result_length'] = len(lines)
        output['results'] = lines

        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def faceted_search(self, **kwargs):
        output = {'success': False}
        subjects = kwargs.get('subjects', '')
        try:
            fs = FacetedSearch(subjects)
            response = fs.execute()
        except Exception as e:
            msg = 'Error: faceted_search failed with {}'.format(e)
            output['msg'] = msg
            return output
        lines = []
        for hit in response:
            lines.append(hit.to_dict())

        output['success'] = True
        output['result_length'] = len(lines)
        output['results'] = lines

        return output

    @cherrypy.expose
    def oaipmh(self, **kwargs):
        cherrypy.log('oai')
        request = cherrypy.request

        response = process_request(
            request.base, request.query_string, **kwargs)

        # prepare return on XML
        cherrypy.response.headers['Content-Type'] = \
            'application/xml;charset=UTF-8'
        cherrypy.response.headers['Content-Length'] = len(response)
        return response


if __name__ == '__main__':
    cherrypy.config.update({'server.socket_port': 8080})
    cherrypy.quickstart(AgentAPI(), '/')
