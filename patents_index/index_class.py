import os
import argparse
import json
import time
import re 
import pandas as pd
import elasticsearch
import elasticsearch.helpers

from es import create_index, refresh, bulk_insert

## Patent Classes Index
def index_classes(ipath):
    """
    Index CPC classification data into Elasticsearch.
    """
    print('Starting CPC classification indexing process...')
    index_name = 'cpc_classes_tmp'
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
    # Delete existing index if it exists
    print(f"Deleting existing index '{index_name}' if it exists...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define index mapping
    mapping = {
        "mappings": {
            "properties": {
                "patent_id": {"type": "keyword"},
                "cpc_section": {"type": "keyword"},
                "cpc_class": {"type": "keyword"},
                "cpc_subclass": {"type": "keyword"},
                "cpc_group": {"type": "keyword"},
                "cpc_type": {"type": "keyword"},
                "cpc_group_title": {"type": "text"},
                "cpc_class_title": {"type": "text"}
            }
        }
    }
    
    # Create the new index with the mapping
    print(f"Creating index '{index_name}' with mapping...")
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully")
    
    # Read and process data in chunks
    chunks = pd.read_csv(ipath, sep=',', dtype=str, chunksize=50000, on_bad_lines='skip')
    total_records = 0
    
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing chunk {chunk_idx+1}...")
        chunk.columns = chunk.columns.str.strip()
        records = []
        
        for _, row in chunk.iterrows():
            action = {
                "_index": index_name,
                "_source": {
                    "patent_id": row['patent_id'],
                    "cpc_section": row['cpc_section'],
                    "cpc_class": row['cpc_class'],
                    "cpc_subclass": row['cpc_subclass'],
                    "cpc_group": row['cpc_group'],
                    "cpc_type": row['cpc_type'],
                    "cpc_group_title": row['cpc_group_title'],
                    "cpc_class_title": row['cpc_class_title']
                }
            }
            records.append(action)
        
        if records:
            print(f"Bulk indexing {len(records)} records...")
            try:
                success, errors = elasticsearch.helpers.bulk(es, records, refresh=True)
                total_records += success
                print(f"Successfully indexed {success} records")
            except elasticsearch.helpers.BulkIndexError as e:
                success = len(records) - len(e.errors)
                total_records += success
                print(f"Successfully indexed {success} records")
                print(f"Failed to index {len(e.errors)} documents")
                for i, error in enumerate(e.errors):
                    error_doc_id = error.get('index', {}).get('_id', 'unknown')
                    error_reason = error.get('index', {}).get('error', {}).get('reason', 'unknown')
                    error_type = error.get('index', {}).get('error', {}).get('type', 'unknown')
                    print(f"Error {i+1}: Document ID: {error_doc_id}, Type: {error_type}, Reason: {error_reason}")
    
    print(f"Total records indexed: {total_records}")
    es.indices.refresh(index=index_name)
    return total_records