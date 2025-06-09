import os
import argparse
import json
import time
import re 
import pandas as pd
import elasticsearch
import elasticsearch.helpers

# Import helper functions for Elasticsearch operations (assumed to be in a separate module)
from es import create_index, refresh, bulk_insert

def index_claim(ipath):
    """
    Comprehensive patent claims indexing function designed to:
    1. Ingest patent claim data from a CSV file
    2. Clean and transform the data
    3. Bulk index into Elasticsearch
    4. Provide detailed logging and error handling

    Args:
        ipath (str): Path to the input CSV file containing patent claims
    
    Returns:
        int: Total number of successfully indexed records
    """
    # Initial startup and configuration logging
    print('🚀 Initiating Patent Claims Indexing Process...')
    
    # Define a temporary index name for our patent claims
    # Using a tmp suffix allows for safe indexing and potential rollback
    index_name = 'claim_tmp'
    
    # Establish connection to local Elasticsearch instance
    # Assumes Elasticsearch is running on default localhost:9200
    print('🔌 Connecting to Elasticsearch...')
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
    # Safety first: Remove any existing index with the same name
    # Prevents conflicts and ensures a clean slate for indexing
    print(f"🧹 Cleaning up any existing '{index_name}' index...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define a precise mapping for our patent claims
    # This ensures each field is stored with the most appropriate data type
    # Helps with search performance and data integrity
    mapping = {
        "mappings": {
            "properties": {
                # Keyword type for exact matching of patent IDs
                "patent_id": {"type": "keyword"},
                # Integer for sequence and claim numbers
                "claim_sequence": {"type": "integer"},
                # Text type for full-text search capabilities
                "claim_text": {"type": "text"},
                # Boolean flags for claim characteristics
                "dependent": {"type": "boolean"},
                "claim_number": {"type": "integer"},
                "exemplary": {"type": "boolean"}
            }
        }
    }
    
    # Create the Elasticsearch index with our custom mapping
    print(f"🏗️  Creating index '{index_name}' with custom mapping...")
    es.indices.create(index=index_name, body=mapping)
    print(f"✅ Index '{index_name}' successfully created")
    
    # Diagnostic peek: Preview the input file to understand its structure
    print("🕵️ Previewing input file contents:")
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                # Show first 4 lines to understand file structure
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    # Track total records processed
    total_records = 0
    
    # Chunk-based reading to handle large files efficiently
    # 50,000 records per chunk to balance memory usage and performance
    print("📊 Preparing to process data in chunks...")
    chunks = pd.read_csv(
        ipath, 
        sep=',', 
        quoting=0, 
        lineterminator='\n', 
        dtype=str,  # Read all columns as strings initially
        chunksize=50000,  # Process in manageable chunks
        on_bad_lines='skip'  # Skip problematic lines instead of failing
    )
    
    # Process each chunk of data
    for chunk_idx, chunk in enumerate(chunks):
        print(f"🔄 Processing claims chunk {chunk_idx+1}...")
        
        # Clean column names by stripping whitespace
        chunk.columns = chunk.columns.str.strip()
        
        # Diagnostics: Show chunk structure
        print(f"📋 Columns in chunk: {chunk.columns.tolist()}")
        print(f"📍 First row in chunk: {chunk.iloc[0].to_dict()}")
        
        # Prepare records for bulk indexing
        records = []
        for _, claim in chunk.iterrows():
            # Robust data cleaning and type conversion
            
            # Ensure claim text is always a string, handle NaN
            claim_text = str(claim['claim_text']) if pd.notna(claim['claim_text']) else ''
            
            # Safely convert claim sequence to integer
            try:
                claim_sequence = int(claim['claim_sequence']) if pd.notna(claim['claim_sequence']) else 0
            except ValueError:
                # Fallback to 0 if conversion fails
                claim_sequence = 0
                
            # Safely convert claim number to integer
            try:
                claim_number = int(claim['claim_number']) if pd.notna(claim['claim_number']) else 0
            except ValueError:
                # Fallback to 0 if conversion fails
                claim_number = 0
            
            # Flexible boolean conversion
            # Supports multiple truthy representations
            dependent = str(claim['dependent']).lower() in ('true', 't', 'yes', 'y', '1') if pd.notna(claim['dependent']) else False
            exemplary = str(claim['exemplary']).lower() in ('true', 't', 'yes', 'y', '1') if pd.notna(claim['exemplary']) else False
            
            # Prepare Elasticsearch bulk index action
            action = {
                "_index": index_name,
                "_source": {
                    "patent_id": claim['patent_id'],
                    "claim_sequence": claim_sequence,
                    "claim_text": claim_text,
                    "dependent": dependent,
                    "claim_number": claim_number,
                    "exemplary": exemplary
                }
            }
            records.append(action)
        
        # Perform bulk indexing for the current chunk
        if records:
            print(f"🚢 Bulk indexing {len(records)} claim records...")
            success, errors = elasticsearch.helpers.bulk(es, records, refresh=True)
            
            # Error handling and logging
            if errors:
                print(f"❌ Errors during bulk indexing: {errors}")
            
            print(f"✅ Successfully indexed {success} claim records")
            total_records += success
    
    # Final index refresh to ensure all data is searchable
    print(f"🔁 Refreshing index '{index_name}'...")
    es.indices.refresh(index=index_name)
    
    # Total indexing summary
    print(f"📈 Total claim records indexed: {total_records}")
    
    # Verify index creation and document count
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"🏆 Index '{index_name}' exists with {count['count']} documents")
    else:
        print(f"⚠️ WARNING: Index '{index_name}' does not exist after indexing!")
    
    return total_records