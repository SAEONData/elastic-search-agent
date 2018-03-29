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

    sudo apt-get install elasticsearch
    sudo service elasticsearch start


## Usage

### JSON API
#### search
Return selected records in a 'SAEON JSON DataCite' format
Arguments:
* field/value pairs: provide any number of fields with the search value
* "fields": limit output to only fields given in this comma separated list
* "from": from date
* "to": to date
* "sort": sort results by the given field in ascending order
* "start": position of the first record returned, default is 1
* "size": number of results, default is 100

#### add
Add a record to a collection
Arguments:
* record: json dict in 'SAEON JSON DataCite' format
* spec_set: optional name of collection

#### delete
Delete a given record
Arguments:
* record_id: record identifier to be deleted
* force: optional to force deletion of duplicated records 

#### delete_all
Delete all records


### OAI - Protocal for Metadata Harverting
See more details here (http://www.openarchives.org/pmh)

#### Available Verbs
* Identity
* ListMetadataFormats
* ListIdentifiers
* ListRecords
* GetRecord
