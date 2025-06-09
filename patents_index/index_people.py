import os
import argparse
import json
import time
import re 
import pandas as pd
import elasticsearch
import elasticsearch.helpers

def index_people(ipath):
    """
    Patent People Indexing Function
    
    Purpose:
    - Process patent-related personnel data from CSV
    - Index data into Elasticsearch for efficient searching
    
    Key Capabilities:
    - Large file handling via chunked processing
    - Robust error management
    - Detailed logging
    - Elasticsearch bulk indexing
    
    Args:
        ipath (str): Input CSV file path containing patent people data
    
    Returns:
        int: Total number of successfully indexed records
    """
    # Define a consistent, temporary index name for patent people data
    index_name = 'patent_people_tmp'
    
    # Establish Elasticsearch connection
    # Assumes Elasticsearch running on localhost:9200
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
    # Safety: Remove any existing index to prevent data conflicts
    print(f"Deleting existing index '{index_name}' if it exists...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define precise Elasticsearch mapping for patent people
    # Optimized for different types of search and aggregation
    mapping = {
        "mappings": {
            "properties": {
                # Identifier fields - optimized for exact matching
                "patent_id": {"type": "keyword"},  # Exact patent identifier
                "applicant_authority": {"type": "keyword"},  # Precise authority matching
                "assignee_id": {"type": "keyword"},  # Exact assignee identifier
                "inventor_id": {"type": "keyword"},  # Exact inventor identifier
                "gender_code": {"type": "keyword"},  # Quick gender filtering
                
                # Text fields with full-text search capabilities
                "applicant_organization": {"type": "text"},  # Searchable organization name
                "applicant_full_name": {"type": "text"},     # Searchable full name
                "assignee_organization": {"type": "text"},   # Searchable assignee org
                "assignee_full_name": {"type": "text"},      # Searchable assignee name
                "inventor_full_name": {"type": "text"}       # Searchable inventor name
            }
        }
    }
    
    # Create Elasticsearch index with defined mapping
    print(f"Creating index '{index_name}' with mapping...")
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully")
    
    # Debug: Peek into input file structure
    print("Reading sample lines from input file:")
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    # Initialize total records counter
    total_records = 0
    
    # Chunk-based CSV processing
    # Benefits: 
    # - Memory efficiency
    # - Handling large files
    # - Robust error handling
    chunks = pd.read_csv(
        ipath, 
        sep=',', 
        quoting=0, 
        lineterminator='\n', 
        dtype=str,  # Treat all columns as strings 
        chunksize=50000,  # Process in 50k record chunks
        on_bad_lines='skip'  # Skip problematic lines
    )
    
    # Process each chunk of data
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing chunk {chunk_idx+1}...")
        
        # Clean column names
        chunk.columns = chunk.columns.str.strip()
        
        # Diagnostic information
        print(f"Columns in chunk: {chunk.columns.tolist()}")
        print(f"First row in chunk: {chunk.iloc[0].to_dict()}")
        
        # Prepare records for bulk indexing
        records = []
        for _, row in chunk.iterrows():
            # Robust data transformation
            # Converts all fields to stripped strings
            # Prevents NaN and ensures clean data
            action = {
                "_index": index_name,
                "_source": {
                    "patent_id": str(row.get('patent_id', '')).strip(),
                    "applicant_authority": str(row.get('applicant_authority', '')).strip(),
                    "applicant_organization": str(row.get('applicant_organization', '')).strip(),
                    "applicant_full_name": str(row.get('applicant_full_name', '')).strip(),
                    "assignee_id": str(row.get('assignee_id', '')).strip(),
                    "assignee_organization": str(row.get('assignee_organization', '')).strip(),
                    "assignee_full_name": str(row.get('assignee_full_name', '')).strip(),
                    "inventor_id": str(row.get('inventor_id', '')).strip(),
                    "gender_code": str(row.get('gender_code', '')).strip(),
                    "inventor_full_name": str(row.get('inventor_full_name', '')).strip()
                }
            }
            records.append(action)
        
        # Bulk indexing with error handling
        if records:
            print(f"Bulk indexing {len(records)} records...")
            try:
                # Elasticsearch bulk indexing
                # refresh=True ensures immediate index refresh
                success, errors = elasticsearch.helpers.bulk(es, records, refresh=True)
                
                # Error reporting
                if errors:
                    print(f"Errors during bulk indexing (first 5): {errors[:5]}")
                
                print(f"Successfully indexed {success} records")
                total_records += success
            
            except elasticsearch.ElasticsearchException as e:
                print(f"Elasticsearch bulk index error: {e}")
    
    # Final index refresh
    print(f"Refreshing index '{index_name}'...")
    es.indices.refresh(index=index_name)
    print(f"Total records indexed: {total_records}")
    
    # Verification step
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"Index '{index_name}' exists with {count['count']} documents")
    else:
        print(f"WARNING: Index '{index_name}' does not exist after indexing!")
    
    return total_records

