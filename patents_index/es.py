# -*- coding: utf-8 -*-

import subprocess
import json

def create_index(index_name, mapping=None):
    """Create an Elasticsearch index with optional custom mapping.

    Parameters
    ----------
    index_name : str
        Name of the Elasticsearch index to be created.
    mapping : dict, optional
        Custom mapping for the index properties. If None, use default mappings.
    """
    
    # Delete the index if it already exists to ensure a fresh start.
    st = 'curl -o /dev/null -s -X DELETE "localhost:9200/{}"'.format(index_name)
    subprocess.Popen(st, shell=True).communicate()

    # Base settings for all indexes
    settings = {"index": {"number_of_shards": 5}}

    # Define default mappings based on index name if no custom mapping is provided
    if mapping is None:
        if index_name == 'patent_tmp':
            mapping = {
                "doc": {
                    "_field_names": {"enabled": False},
                    "properties": {
                        "date": {"type": "date"},
                        "id": {"type": "keyword"},
                        "abstract": {"type": "text"}
                    }
                }
            }
        else:
            mapping = {
                "doc": {
                    "_field_names": {"enabled": False},
                    "properties": {
                        "id": {"type": "keyword"},
                        "text": {"type": "text"}
                    }
                }
            }
    
    # Create the index with settings and mappings
    payload = json.dumps({"settings": settings, "mappings": mapping})
    st = f'curl -o /dev/null -s -X PUT "localhost:9200/{index_name}" -H ' + \
         f'"Content-Type: application/json" -d \'{payload}\''
    subprocess.Popen(st, shell=True).communicate()

    # Update index settings: set replicas to 0 and disable automatic refreshing
    st = f'curl -o /dev/null -s -X PUT "localhost:9200/{index_name}/_settings" ' + \
         '-H \'Content-Type: application/json\' -d \'{"index": {"number_of_replicas": 0, "refresh_interval": -1}}\''
    subprocess.Popen(st, shell=True).communicate()

def delete_index(index_name):
    """Delete an Elasticsearch index."""
    st = f'curl -o /dev/null -s -X DELETE "localhost:9200/{index_name}"'
    subprocess.Popen(st, shell=True).communicate()

def refresh(index_name):
    """Refresh an Elasticsearch index."""
    st = f'curl -o /dev/null -s -X PUT "localhost:9200/{index_name}/_settings" ' + \
         '-H \'Content-Type: application/json\' -d \'{"index": {"refresh_interval": "1s"}}\''
    subprocess.Popen(st, shell=True).communicate()
    st = f'curl -o /dev/null -s -X POST "localhost:9200/{index_name}/_refresh"'
    subprocess.Popen(st, shell=True).communicate()

def bulk_insert(index_name, fp):
    """Use Elasticsearch Bulk API to load data into an index."""
    st = f'curl -o /dev/null -s -H "Content-Type: application/x-ndjson" ' + \
         f'-XPOST "localhost:9200/{index_name}/doc/_bulk?pretty" --data-binary @{fp}'
    subprocess.Popen(st, shell=True).communicate()