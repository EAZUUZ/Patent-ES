import os
import argparse
import json
import time
import re
import logging
import pandas as pd
import elasticsearch
import elasticsearch.helpers

def setup_logging():
    """
    Configure logging for the patent summary indexing process.
    
    Returns:
        logging.Logger: Configured logger instance
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    return logging.getLogger(__name__)

def clean_summary_text(summary_text):
    """
    Clean and normalize summary text.
    
    Args:
        summary_text (str): Raw summary text
    
    Returns:
        str: Cleaned and normalized summary text
    """
    # Handle None or NaN values
    if pd.isna(summary_text):
        return ''
    
    # Convert to string and strip
    summary_text = str(summary_text).strip()
    
    # Remove punctuation and special characters
    # Preserve word characters and whitespace
    clean_summary = re.sub(r'[^\w\s]', '', summary_text)
    
    # Replace multiple whitespaces with a single space
    clean_summary = re.sub(r'\s+', ' ', clean_summary).strip()
    
    return clean_summary

def index_summary(input_path, es_host="http://localhost:9200"):
    """
    Index patent summary data into Elasticsearch.
    
    Patent Summary Indexing Process:
    - Reads large CSV files in chunks
    - Cleans and normalizes summary text
    - Bulk indexes data into Elasticsearch
    
    Args:
        input_path (str): Path to input TSV file containing patent summaries
        es_host (str, optional): Elasticsearch host URL. Defaults to localhost.
    
    Returns:
        int: Total number of records processed
    """
    # Setup logging
    logger = setup_logging()
    logger.info('🚀 Initiating Patent Summary Indexing Process')
    
    # Consistent, temporary index name
    index_name = 'patent_summary_tmp'
    
    # Establish Elasticsearch connection
    try:
        es_client = elasticsearch.Elasticsearch(hosts=[es_host])
    except Exception as e:
        logger.error(f"Failed to connect to Elasticsearch: {e}")
        return 0
    
    # Define Elasticsearch mapping for patent summaries
    mapping = {
        "mappings": {
            "properties": {
                "patent_id": {"type": "keyword"},  # Exact patent identifier
                "summary": {
                    "type": "text",  # Full-text searchable summary
                    "analyzer": "standard",  # Standard text analysis
                    "fields": {
                        "keyword": {
                            "type": "keyword",  # Exact match capability
                            "ignore_above": 256
                        }
                    }
                }
            }
        }
    }
    
    # Delete existing index if present
    es_client.indices.delete(index=index_name, ignore=[400, 404])
    
    # Create new index with mapping
    es_client.indices.create(index=index_name, body=mapping)
    logger.info(f"Created Elasticsearch index: {index_name}")
    
    # Debug: Preview input file
    logger.info("Previewing input file structure:")
    with open(input_path, 'r') as f:
        for i, line in enumerate(f):
            if i < 3:
                logger.info(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    # Chunked CSV processing
    total_processed = 0
    try:
        chunks = pd.read_csv(
            input_path, 
            sep='\t',  # Tab-separated values
            quoting=0, 
            lineterminator='\n', 
            dtype=str, 
            chunksize=50000,  # Process in 50k record chunks
            on_bad_lines='warn'  # Log but continue on bad lines
        )
        
        # Process each data chunk
        for chunk_idx, chunk in enumerate(chunks, 1):
            logger.info(f"Processing chunk {chunk_idx}")
            
            # Normalize column names
            chunk.columns = chunk.columns.str.strip()
            logger.info(f"Columns in chunk: {chunk.columns.tolist()}")
            logger.info(f"First row preview: {chunk.iloc[0].to_dict()}")
            
            # Prepare records for bulk indexing
            records = []
            for _, row in chunk.iterrows():
                clean_summary = clean_summary_text(row.get('summary_text', ''))
                
                # Skip empty summaries
                if not clean_summary:
                    continue
                
                record = {
                    "_index": index_name,
                    "_source": {
                        "patent_id": str(row.get('patent_id', '')).strip(),
                        "summary": clean_summary
                    }
                }
                records.append(record)
            
            # Bulk indexing
            if records:
                try:
                    success, errors = elasticsearch.helpers.bulk(
                        es_client, 
                        records, 
                        refresh=True
                    )
                    
                    total_processed += success
                    
                    if errors:
                        logger.warning(f"Indexing errors in chunk {chunk_idx}: {errors[:5]}")
                
                except Exception as bulk_error:
                    logger.error(f"Bulk indexing error: {bulk_error}")
    
    except Exception as e:
        logger.error(f"Processing error: {e}")
        return total_processed
    
    # Final logging
    logger.info(f"🏁 Indexing complete. Total records processed: {total_processed}")
    
    return total_processed

