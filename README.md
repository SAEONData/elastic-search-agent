# Metadata Search Agent using Elastic Search

## Deployment

### System dependencies
* python3 (&ge; 3.6)
* elasticssearch
* elasticssearch_dsl

### Package dependencies
Elasticsearch DSL is a high-level library whose aim is to help with writing
and running queries against Elasticsearch.
It is built on top of the official low-level client (elasticsearch-py).

### Package installation
Assuming the Agent repository has been cloned to `$AGENTDIR`, install the Agent
and its remaining package dependencies with:

    pip3 install $AGENTDIR

### ElasticSearch setup
Run the following commands to create an elasticsearch instance for the Agent:

    sudo apt-get install apt-transport-https
    echo "deb https://artifacts.elastic.co/packages/6.x/apt stable main" | sudo tee -a /etc/apt/sources.list.d/elastic-6.x.list
    sudo apt-get update && sudo apt-get install elasticsearch
    sudo service elasticsearch start

Go to http://localhost:9200 to check that elasticsearch is running. Depending
on your system, you might need to set `START_DAEMON=true` in /etc/default/elasticsearch
(and restart the service after changing the config).

## Usage

### JSON API
#### create_index
Add an index where records will be stored
##### Arguments:
* index: where the new records will be stored
* metadata_json: a template records used to define the metadata structure

#### delete_index
Delete an exiting index
##### Arguments:
* index: name of index to be created

#### search
Return selected records in a 'SAEON JSON DataCite' format
##### Arguments:
* index: where the new records will be found
* field/value pairs: provide any number of fields with the search value
* fields: limit output to only fields given in this comma separated list
* from: from date
* to: to date
* sort: sort results by the given field in ascending order
* sortorder: asc or desc
* start: position of the first record returned, default is 1
* size: number of results, default is 100
* encloses: return objects that lay completely within given rectangle specified as postions top left and bottom right
* overlaps: return objects that lay partially within given rectangle specified as postions top left and bottom right
* includes: return objects that contain the given rectangle specified as postions top left and bottom right

#### add
Add a record to a collection
##### Arguments:
* index: where the new records will be stored
* record_id: unique ID of the record
* metadata_json: json dict in 'SAEON JSON DataCite' format
* collection: optional collection name
* organization: optional name of organization
* infrastructures: optional list of infrastructures

#### delete
Delete a given record
##### Arguments:
* index: where records are stored
* record_id: record identifier to be deleted
* force: optional to force deletion of duplicated records 

#### delete_all
Delete all records
* index: where records are stored

#### faceted_search
Return all known facets
* index: where records are stored
* facets (optional): comma separated list of facets


### OAI - Protocal for Metadata Harverting
See more details here (http://www.openarchives.org/pmh)

#### Available Verbs
* Identity
* ListMetadataFormats
* ListIdentifiers
* ListRecords
* GetRecord
