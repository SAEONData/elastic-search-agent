#!/usr/bin/env python

import cherrypy
import json
import logging
import time
import xml.etree.ElementTree as ET
from agent.config import server_port
from agent.config import token_index_name
from agent.oaipmh import process_request
from agent.persist import Metadata
from agent.search import MetadataSearch
from agent.search import search_all
from agent.search import search
from agent.utils import index_exists
from agent.utils import get_request_host
from agent.utils import json_handler
from agent.utils import format_metadata
from agent.utils import transpose_metadata_record
from agent.utils import validate_metadata_record
from elasticsearch_dsl import Index
from elasticsearch_dsl import Search


logger = logging.getLogger(__name__)
ALLOWED_FACETS = [
    'subjects',
    'creators',
    'publicationYear',
    'publisher',
    'collectedStartDate',
    'collectedEndDate'
]


def get_valid_facets(facets):
    if not facets:
        return None
    facets = facets.split(',')
    for facet in facets:
        if facet not in ALLOWED_FACETS:
            return None
    return facets


class AgentAPI(object):

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def create_index(self, **kwargs):
        cherrypy.log(str(kwargs))
        output = {'success': False}
        metadata_json = kwargs.get('metadata_json')
        if metadata_json is None:
            msg = "Error: 'metadata_json' argument is required"
            output['msg'] = msg
            return output
        index = kwargs.get('index')
        if index is None:
            msg = "Error: 'index' argument is required"
            output['msg'] = msg
            return output
        if index_exists(index):
            msg = "Error: index {} already exists".format(index)
            output['msg'] = msg
            return output

        try:
            metadata_json = json.loads(metadata_json)
        except Exception as e:
            msg = "Error: json format {}".format(e)
            output['msg'] = msg
            return output

        validation = validate_metadata_record(metadata_json)
        if not validation['success']:
            output['msg'] = validation['msg']
            return output

        transposed = transpose_metadata_record(metadata_json)
        if not transposed['success']:
            output['msg'] = validation['msg']
            return output

        record_id = metadata_json['identifier']['identifier']

        Metadata.init(index=index)
        try:
            md = Metadata(
                anytext='dummy',
                collection='dummy',
                organization='dummy',
                infrastructures=['d1'],
                record_id=record_id,
                metadata_json=metadata_json,
            )
            md.meta.index = index
        except Exception as e:
            msg = "Error: {}: {}".format('Creation failed', e)
            output['msg'] = msg
            return output

        try:
            md.save()
        except Exception as e:
            msg = "Error: {}: {} - {}".format(
                'Save failed', record_id, e.info)
            output['msg'] = msg
            return output

        # Wait until metadata_json has been created - Do No Remove!
        time.sleep(3)

        # Delete template metadata_json
        srch = Metadata.search(index=index)
        srch = srch.filter('match', record_id=record_id)
        srch.execute()
        if srch.count() == 0:
            msg = "Error: metadata_json {} not found".format(record_id)
            output['msg'] = msg
            return output
        if srch.count() > 1 and not force:
            msg = "Error: duplicate records found with id {}. ".format(
                record_id)
            msg.append('Use force=true argument to delete all duplicates')
            output['msg'] = msg
            return output
        srch.delete()

        output['success'] = True
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_index(self, **kwargs):
        cherrypy.log(str(kwargs))
        output = {'success': False}
        index = kwargs.get('index')
        if index is None:
            msg = "Error: 'index' argument is required"
            output['msg'] = msg
            return output
        idx = Index(index)
        if not idx.exists():
            msg = "Error: index {} does not exist".format(index)
            output['msg'] = msg
            return output

        try:
            idx.delete()
        except Exception as e:
            msg = "Error: delete index failed: resean {}".format(e)
            output['msg'] = msg
            return output

        output['success'] = True
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def add(self, **kwargs):
        cherrypy.log(str(kwargs))
        output = {'success': False}
        index = kwargs.get('index')
        if index is None:
            msg = "Error: 'index' argument is required"
            output['msg'] = msg
            return output
        collection = kwargs.get('collection')
        if collection is None:
            msg = "Error: 'collection' argument is required"
            output['msg'] = msg
            return output
        infrastructures = kwargs.get('infrastructures', [])
        # if infrastructures is None:
        #     msg = "Error: 'infrastructures' argument is required"
        #     output['msg'] = msg
        #     return output
        metadata_json = kwargs.get('metadata_json')
        if metadata_json is None:
            msg = "Error: 'metadata_json' argument is required"
            output['msg'] = msg
            return output
        organization = kwargs.get('organization')
        if organization is None:
            msg = "Error: 'organization' argument is required"
            output['msg'] = msg
            return output
        record_id = kwargs.get('record_id')
        if record_id is None:
            msg = "Error: 'record_id' argument is required"
            output['msg'] = msg
            return output
        if not index_exists(index):
            msg = "Error: index {} does not exist".format(index)
            output['msg'] = msg
            return output

        try:
            metadata_json = json.loads(metadata_json)
        except Exception as e:
            msg = "Error: json format {}".format(e)
            output['msg'] = msg
            return output

        validation = validate_metadata_record(metadata_json)
        if not validation['success']:
            output['msg'] = validation['msg']
            return output

        transposed = transpose_metadata_record(metadata_json)
        if not transposed['success']:
            output['msg'] = validation['msg']
            return output

        # print(geoLocations)
        # Replace record if it already exists ie. delete first
        srch = Metadata.search(index=index)
        srch = srch.filter('match', record_id=record_id)
        srch.execute()
        exists = False
        if srch.count() == 1:
            srch.delete()
            exists = True

        Metadata.init(index=index)
        try:
            md = Metadata(
                collection=collection,
                infrastructures=infrastructures,
                organization=organization,
                record_id=record_id,
                metadata_json=metadata_json,
            )
            md.meta.index = index
        except Exception as e:
            msg = "Error: {}: {}".format('Creation failed', e)
            output['msg'] = msg
            return output

        try:
            md.save()
        except AttributeError as e:
            msg = "{}: {} - {}".format(
                'Save failed', e.args, record_id)
            output['msg'] = msg
            return output
        except Exception as e:
            msg = "Error: {}: {} - {}".format(
                'Save failed', record_id, e.info)
            output['msg'] = msg
            return output

        output['success'] = True
        if exists:
            output['msg'] = 'Record replaced'
        else:
            output['msg'] = 'Record added'
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete_all(self, **kwargs):
        output = {'success': False}
        index = kwargs.get('index')
        if index is None:
            msg = "Error: 'index' argument is required"
            output['msg'] = msg
            return output
        if not index_exists(index):
            msg = "Error: index {} does not exist".format(index)
            output['msg'] = msg
            return output

        cherrypy.log('delete_all in index {}'.format(index))
        s = Search(index=index)
        s.delete()
        s = Search(index=token_index_name)
        s.delete()
        output['success'] = True
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def delete(self, **kwargs):
        output = {'success': False}
        cherrypy.log('delete_all')
        record_id = kwargs.get('record_id')
        if record_id is None:
            msg = "Error: 'record_id' argument is required"
            output['msg'] = msg
            return output
        index = kwargs.get('index')
        if index is None:
            msg = "Error: 'index' argument is required"
            output['msg'] = msg
            return output
        if not index_exists(index):
            msg = "Error: index {} does not exist".format(index)
            output['msg'] = msg
            return output

        force = kwargs.get('force', 'false').lower() == 'true'

        srch = Metadata.search(index=index)
        srch = srch.filter('match', record_id=record_id)
        srch.execute()
        if srch.count() == 0:
            output['msg'] = "Record not found"
            output['success'] = True
            return output
        if srch.count() > 1 and not force:
            msg = "Error: duplicate records found with id {}. ".format(
                record_id)
            msg.append('Use force=true argument to delete all duplicates')
            output['msg'] = msg
            return output
        srch.delete()
        output['success'] = True
        output['msg'] = 'Record deleted'
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out(handler=json_handler)
    def search(self, **kwargs):
        output = {'success': False}
        index = None
        new_kwargs = {}
        for k in kwargs:
            if k == 'index':
                index = kwargs[k]
                continue
            new_kwargs[k] = kwargs[k]
        if index is None:
            msg = "Error: 'index' argument is required"
            output['msg'] = msg
            return output
        if not index_exists(index):
            msg = "Error: index {} does not exist".format(index)
            output['msg'] = msg
            return output

        if new_kwargs:
            response = search(index=index, **new_kwargs)
        else:
            response = search_all(index=index)

        if not response['success']:
            output['error'] = response['error']
            return output

        items = format_metadata(response['result'])

        output['success'] = True
        output['result_length'] = len(items)
        output['results'] = items

        return output

    def get_one_facet(self, index, facet):
        output = {'success': False, 'lines': []}
        try:
            fs = MetadataSearch(**{'index': index, 'facet': facet})
            response = fs.execute()
        except Exception as e:
            msg = 'Error: faceted_search failed with {}'.format(e)
            output['error'] = msg
            return output
        logger.debug(response.hits.total, 'hits total')

        aggs = response.aggregations.to_dict()
        logger.debug(aggs)

        lines = []
        for facet_key in aggs:
            facet = aggs[facet_key]
            facet_name = [k for k in facet.keys() if k != 'doc_count'][0]
            facet_result = dict()
            for facet_dict in facet[facet_name]['buckets']:
                key = facet_dict['key']
                if facet_dict.get('key_as_string', False):
                    key = facet_dict['key_as_string']
                facet_result[key] = facet_dict['doc_count']
            lines.append({facet_name: facet_result})

        if lines:
            output['lines'] = lines

        output['success'] = True
        return output

    @cherrypy.expose
    @cherrypy.tools.json_out()
    def faceted_search(self, **kwargs):
        output = {'success': False}
        index = kwargs.get('index')
        if index is None:
            msg = "Error: 'index' argument is required"
            output['msg'] = msg
            return output
        if not index_exists(index):
            msg = "Error: index {} does not exist".format(index)
            output['msg'] = msg
            return output
        facets = kwargs.get('facets', 'ALL')
        if facets == 'ALL':
            facets = ALLOWED_FACETS
        else:
            facets = get_valid_facets(facets)
            if facets is None:
                output['error'] = \
                    'Allowed facets: {}'.format(', '.join(ALLOWED_FACETS))
                return output

        lines = []
        for facet in facets:
            result = self.get_one_facet(index, facet)
            if result['success']:
                lines.extend(result['lines'])
            else:
                output['error'] = result['error']
                return output

        output['success'] = True
        output['result_length'] = len(lines)
        output['results'] = lines

        return output

    @cherrypy.expose
    def oaipmh(self, **kwargs):
        request = cherrypy.request
        cherrypy.log('oaipmh')

        response = process_request(
            request, request.query_string, **kwargs)

        # prepare return on XML
        cherrypy.response.headers['Content-Type'] = \
            'application/xml;charset=UTF-8'
        cherrypy.response.headers['Content-Length'] = len(response)
        return response

    @cherrypy.expose
    def default(self, *args, **kwargs):
        request = cherrypy.request
        host = get_request_host(request)
        url = "http://{}".format(host)
        cherrypy.log('root')

        root = ET.Element("html")
        body = ET.SubElement(root, "body")
        child = ET.SubElement(body, "h2")
        child.text = 'Welcome to the SAEON Metadata Search Agent'
        api = ET.SubElement(body, "h3")
        api.text = 'JSON API'

        # Createa Index
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "br")
        add = ET.SubElement(api, "a", {
            'href': '{}/create_index'.format(url)
        })
        add.text = 'Create Index'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "Create an index"
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = 'Arguments:'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* index: where the new records will be stored'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* metadata_json: a template records used to define the metadata structure'

        # Delete Index
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "br")
        add = ET.SubElement(api, "a", {
            'href': '{}/delete_index'.format(url)
        })
        add.text = 'Delete Index'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "Delete an index"
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = 'Arguments:'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* index: name of index to be deleted'

        # Search
        ET.SubElement(api, "br")
        ET.SubElement(api, "br")
        search = ET.SubElement(api, "a", {
            'href': '{}/search'.format(url)
        })
        search.text = 'Search'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "Return selected records in a 'SAEON JSON DataCite' format"
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = 'Arguments:'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "index": the name of the index to be searched (default is md_index_1)'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* field/value pairs: provide any number of fields with the search value. Use * for wildcard queries'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "match": one of [must | must_not | should | filter]. Default is must'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "fields": limit output to only fields given in this comma separated list'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "from": from date'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "to": to date'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "sort": sort results by the given field in ascending order'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "sortorder": asc or desc'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "start": position of the first record returned, default is 1'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "size": number of records - default is 100'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "encloses": return objects that lay completely within given rectangle specified as postions top left and bottom right'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "overlaps": return objects that lay partially within given rectangle specified as postions top left and bottom right'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "includes": return objects that contain the given rectangle specified as postions top left and bottom right'

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* "excludes": return objects that are outside the given rectangle specified as postions top left and bottom right'

        # Add
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "br")
        add = ET.SubElement(api, "a", {
            'href': '{}/add'.format(url)
        })
        add.text = 'Add'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "Add a record to a collection"
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = 'Arguments:'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* index: where the new records will be added (default is md_index_1)'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "* record_id: unique ID of the record"

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "* metadata_json: json dict in 'SAEON JSON DataCite' format"
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "* organization: optional name of organization"

        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "* infrastructures: optional list of infrastructures"

        # Delete
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "br")
        add = ET.SubElement(api, "a", {
            'href': '{}/delete'.format(url)
        })
        add.text = 'Delete'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "Delete a given record"
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = 'Arguments:'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* index: the index from which records will be deleted (default is md_index_1)'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "* record_id: record identifier to be deleted"
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "* force: optional to force deletion of duplicated records"

        # Faceted Search
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "br")
        search = ET.SubElement(api, "a", {
            'href': '{}/faceted_search'.format(url)
        })
        search.text = 'Faceted Search'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = "Return the facet values for the given facets. Or all known facets if no argument provided"
        child.text = 'Arguments:'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* index: the name of the index to be searched (default is md_index_1)'
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "span", {
            'style': 'font-size: 12'})
        child.text = '* facets (optional): comma separated list of facets'

        # OAI-PMH
        child = ET.SubElement(api, "br")
        child = ET.SubElement(api, "br")
        oai = ET.SubElement(body, "h3")
        oai.text = 'OAI-Protocal for Metadata Harverting'
        child = ET.SubElement(oai, "br")
        verbs = ET.SubElement(oai, "span")
        verbs.text = 'Verbs'
        child = ET.SubElement(verbs, "br")

        child = ET.SubElement(verbs, "a", {
            'href': '{}/oaipmh?verb=Identity'.format(url)
        })
        child.text = 'Identity'
        child = ET.SubElement(verbs, "br")

        child = ET.SubElement(verbs, "a", {
            'href': '{}/oaipmh?verb=ListMetadataFormats'.format(url)
        })
        child.text = 'ListMetadataFormats'
        child = ET.SubElement(verbs, "br")

        child = ET.SubElement(verbs, "a", {
            'href': '{}/oaipmh?verb=ListIdentifiers'.format(url)
        })
        child.text = 'ListIdentifiers'
        child = ET.SubElement(verbs, "br")

        child = ET.SubElement(verbs, "a", {
            'href': '{}/oaipmh?verb=ListRecords'.format(url)
        })
        child.text = 'ListRecords'
        child = ET.SubElement(verbs, "br")

        child = ET.SubElement(verbs, "a", {
            'href': '{}/oaipmh?verb=GetRecord'.format(url)
        })
        child.text = 'GetRecord'
        child = ET.SubElement(verbs, "br")

        return ET.tostring(root)


if __name__ == '__main__':
    cherrypy.config.update({'server.socket_port': server_port})
    cherrypy.quickstart(AgentAPI(), '/')
