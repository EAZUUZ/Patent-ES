import os
import pandas as pd
from elasticsearch import Elasticsearch, helpers

def index_us_app_citation(citation_file_path):
    print('🚀 Initiating US Application Citations Indexing Process...')
    
    index_name = 'us_app_citation_tmp'
    
    # Establish connection to Elasticsearch
    print('🔌 Connecting to Elasticsearch...')
    es = Elasticsearch(hosts=["http://localhost:9200"])
    
    # Delete existing index to prevent conflicts and ensure fresh indexing
    print(f"🧹 Deleting any existing index '{index_name}'...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define Elasticsearch index mapping
    mapping = {
        "mappings": {
            "properties": {
                "patent_id": {"type": "keyword"},
                "citation_sequence": {"type": "integer"},
                "citation_document_number": {"type": "keyword"},
                "citation_date": {"type": "date"},
                "record_name": {"type": "text"},
                "wipo_kind": {"type": "keyword"},
                "citation_category": {"type": "keyword"}
            }
        }
    }
    
    # Create index
    print(f"🏗️  Creating index '{index_name}'...")
    es.indices.create(index=index_name, body=mapping)
    print(f"✅ Index '{index_name}' created successfully.")
    
    # Preview input file
    print("🕵️ Previewing input file contents:")
    with open(citation_file_path, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    # Read CSV in chunks
    print("📊 Processing data in chunks...")
    chunks = pd.read_csv(
        citation_file_path,
        sep=',',
        dtype=str,  # Ensure all columns are read as strings
        chunksize=50000,
        on_bad_lines='skip'  # Skip malformed lines
    )
    
    total_records = 0
    
    for chunk_idx, chunk in enumerate(chunks):
        print(f"🔄 Processing chunk {chunk_idx+1}...")
        
        # Standardize column names
        chunk.columns = chunk.columns.str.strip()
        
        # Required columns
        required_columns = {
            "patent_id", 
            "US_app_citation_citation_sequence",
            "US_app_citation_citation_document_number",
            "US_app_citation_citation_date",
            "US_app_citation_record_name",
            "US_app_citation_wipo_kind",
            "US_app_citation_citation_category"
        }
        
        # Check for missing columns
        missing_columns = required_columns - set(chunk.columns)
        if missing_columns:
            print(f"❌ Missing required columns: {missing_columns}")
            return 0
        
        # Display chunk metadata
        print(f"📋 Columns in chunk: {chunk.columns.tolist()}")
        print(f"📍 First row in chunk: {chunk.iloc[0].to_dict() if not chunk.empty else 'No Data'}")
        
        # Handle missing values (fill NaNs with empty strings)
        chunk.fillna("", inplace=True)
        
        # Convert all columns to strings to avoid float errors
        chunk = chunk.astype(str)
        
        # Remove duplicates
        chunk.drop_duplicates(subset=['patent_id', 'US_app_citation_citation_document_number'], keep='first', inplace=True)
        
        records = []
        
        for _, row in chunk.iterrows():
            try:
                # Convert citation date
                citation_date = pd.to_datetime(row['US_app_citation_citation_date'], errors='coerce').strftime('%Y-%m-%d') \
                    if row['US_app_citation_citation_date'] else None
                
                # Construct Elasticsearch document
                record = {
                    "_index": index_name,
                    "_source": {
                        "patent_id": row['patent_id'].strip(),
                        "citation_sequence": int(row['US_app_citation_citation_sequence']) if row['US_app_citation_citation_sequence'].isdigit() else 0,
                        "citation_document_number": row['US_app_citation_citation_document_number'].strip(),
                        "citation_date": citation_date,
                        "record_name": row['US_app_citation_record_name'].strip(),
                        "wipo_kind": row['US_app_citation_wipo_kind'].strip(),
                        "citation_category": row['US_app_citation_citation_category'].strip()
                    }
                }
                records.append(record)
                
            except Exception as e:
                print(f"⚠️ Skipping record due to error: {e}")
                continue
        
        # Bulk index in Elasticsearch
        if records:
            print(f"🚢 Bulk indexing {len(records)} records...")
            success, errors = helpers.bulk(es, records, refresh=True)
            
            if errors:
                print(f"❌ Errors encountered: {errors[:5]}")
            
            print(f"✅ Indexed {success} records successfully")
            total_records += success
    
    # Refresh index
    print(f"🔁 Refreshing index '{index_name}'...")
    es.indices.refresh(index=index_name)
    
    # Final verification
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"🏆 Index '{index_name}' exists with {count['count']} documents.")
    else:
        print(f"⚠️ WARNING: Index '{index_name}' does not exist after indexing!")
    
    print(f"📈 Total records indexed: {total_records}")
    return total_records
