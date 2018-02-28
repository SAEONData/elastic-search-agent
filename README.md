# Elastic Search Agent for Handling Metadata

## Deployment

### System dependencies
* python3 (&ge; 3.6)
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


### Usage
add
