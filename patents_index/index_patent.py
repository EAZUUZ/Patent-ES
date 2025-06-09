# import os
# import argparse
# import json
# import time
# import re 
# import pandas as pd
# import elasticsearch
# import elasticsearch.helpers

# from es import create_index, refresh, bulk_insert

# def index_patent(ipath):
#     """
#     Comprehensive Patent Data Indexing Function

#     This sophisticated function transforms raw patent data into a structured, 
#     searchable Elasticsearch index. It's designed to handle large-scale patent 
#     document collections with precision and efficiency.

#     Key Objectives:
#     - Ingest patent data from CSV source
#     - Create a specialized Elasticsearch index for patent metadata
#     - Perform efficient bulk indexing
#     - Provide detailed error tracking and reporting
#     - Generate intermediate JSON for potential further processing

#     Args:
#         ipath (str): File path to the input CSV containing patent data

#     Returns:
#         int: Total number of successfully indexed patent records
#     """
#     # 🚀 Initialization and Setup
#     print('🌟 Initiating Comprehensive Patent Indexing Process...')
    
#     # Generate unique output path for intermediate JSON
# def index_patent(ipath):
#     """
#     Comprehensive Patent Data Indexing Function

#     This sophisticated function transforms raw patent data into a structured, 
#     searchable Elasticsearch index. It's designed to handle large-scale patent 
#     document collections with precision and efficiency.

#     Key Objectives:
#     - Ingest patent data from CSV source
#     - Create a specialized Elasticsearch index for patent metadata
#     - Perform efficient bulk indexing
#     - Provide detailed error tracking and reporting
#     - Generate intermediate JSON for potential further processing

#     Args:
#         ipath (str): File path to the input CSV containing patent data

#     Returns:
#         int: Total number of successfully indexed patent records
#     """
#     # 🚀 Initialization and Setup
#     print('🌟 Initiating Comprehensive Patent Indexing Process...')
    
#     # Generate unique output path for intermediate JSON
#     timestamp = time.strftime("%Y%m%d_%H%M%S")
#     timestamped_index_name = f'patent_tmp_{timestamp}'
#     index_name = 'patent_tmp'  # This is the consistent name other functions will use
#     opath = os.path.join(os.path.dirname(ipath), f'patent_index_{timestamp}.json')
    
#     # 🔌 Elasticsearch Connection Establishment
#     print('🌐 Establishing Elasticsearch Connection...')
#     es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
#     # 🧹 Clean Slate: Remove any existing index to prevent conflicts
#     print(f"🗑️ Preparing index environment: Removing existing '{timestamped_index_name}' if present...")
#     es.indices.delete(index=timestamped_index_name, ignore=[400, 404])
    
#     # 🏗️ Precise Elasticsearch Mapping Definition
#     # Optimized for efficient searching and aggregation of patent metadata
#     mapping = {
#         "mappings": {
#             "properties": {
#                 # Keyword fields for exact matching
#                 "patent_id": {"type": "keyword"},
#                 "patent_type": {"type": "keyword"},
                
#                 # Text fields for full-text search capabilities
#                 "patent_title": {
#                     "type": "text",
#                     "fields": {
#                         "keyword": {"type": "keyword", "ignore_above": 256}
#                     }
#                 },
#                 "patent_abstract": {
#                     "type": "text",
#                     "fields": {
#                         "keyword": {"type": "keyword", "ignore_above": 256}
#                     }
#                 },
                
#                 # Date and numeric fields for precise filtering
#                 "patent_date": {"type": "date"},
#                 "num_claims": {"type": "integer"}
#             }
#         }
#     }
    
#     # 🏗️ Create Elasticsearch Index with Custom Mapping
#     print(f"📋 Creating index '{timestamped_index_name}' with specialized patent metadata mapping...")
#     es.indices.create(index=timestamped_index_name, body=mapping)
#     print(f"✅ Index '{timestamped_index_name}' successfully initialized")
    
#     # 📊 Diagnostic: Preview Input Data
#     print("🔍 Previewing Input Data Structure:")
#     with open(ipath, 'r') as f:
#         for i, line in enumerate(f):
#             if i < 3:
#                 print(f"Raw Line {i + 1}: {line.strip()}")
#             else:
#                 break
    
#     # Performance and Error Tracking
#     total_records = 0
#     total_errors = 0
#     processing_start_time = time.time()
    
#     # Prepare JSON output file for intermediate storage
#     json_output_records = []
    
#     # 🧩 Chunk-Based Processing Strategy
#     chunks = pd.read_csv(
#         ipath, 
#         sep=',', 
#         quoting=0, 
#         lineterminator='\n', 
#         dtype=str,  # Read all columns as strings initially
#         chunksize=50000,  # Manageable chunk size
#         on_bad_lines='skip'  # Gracefully handle problematic lines
#     )
    
#     # 🔄 Chunk Processing Loop
#     for chunk_idx, chunk in enumerate(chunks, 1):
#         print(f"\n🚧 Processing Chunk {chunk_idx}...")
        
#         # Normalize column names
#         chunk.columns = chunk.columns.str.strip()
#         print(f"📋 Columns in chunk: {chunk.columns.tolist()}")
#         print(f"📍 First row in chunk: {chunk.iloc[0].to_dict()}")
        
#         # 🧼 Data Preparation for Bulk Indexing
#         records = []
#         for _, patent in chunk.iterrows():
#             # Robust data cleaning and type conversion
#             try:
#                 # Safely convert numeric fields
#                 num_claims = int(patent['num_claims']) if pd.notna(patent['num_claims']) else 0
                
#                 # Create Elasticsearch index action
#                 action = {
#                     "_index": timestamped_index_name,
#                     "_source": {
#                         "patent_id": str(patent['patent_id']).strip(),
#                         "patent_title": str(patent['patent_title']).strip(),
#                         "patent_date": patent['patent_date'],
#                         "num_claims": num_claims,
#                         "patent_type": str(patent['patent_type']).strip(),
#                         "patent_abstract": str(patent['patent_abstract']).strip()
#                     }
#                 }
                
#                 records.append(action)
                
#                 # Prepare JSON for intermediate storage
#                 json_output_records.append(action['_source'])
            
#             except Exception as e:
#                 print(f"❌ Error processing patent record: {str(e)}")
#                 total_errors += 1
        
#         # 🚢 Bulk Indexing with Comprehensive Error Handling
#         if records:
#             print(f"📤 Bulk Indexing {len(records)} Patent Records...")
#             try:
#                 # Perform bulk indexing with refresh
#                 success, errors = elasticsearch.helpers.bulk(
#                     es, 
#                     records, 
#                     refresh=True,
#                     raise_on_error=False
#                 )
                
#                 total_records += success
                
#                 # Detailed Error Reporting
#                 if errors:
#                     total_errors += len(errors)
#                     print(f"⚠️ Encountered {len(errors)} indexing errors in chunk {chunk_idx}")
            
#             except Exception as e:
#                 print(f"❌ Critical Bulk Indexing Error: {str(e)}")
    
#     # 💾 Write Intermediate JSON
#     try:
#         with open(opath, 'w') as json_file:
#             json.dump(json_output_records, json_file, indent=2)
#         print(f"💾 Intermediate JSON output saved to: {opath}")
#     except Exception as e:
#         print(f"❌ Error writing JSON output: {str(e)}")
    
#     # Create alias for the timestamped index
#     try:
#         print(f"Creating alias '{index_name}' for '{timestamped_index_name}'...")
#         # Remove existing alias if it exists
#         if es.indices.exists_alias(name=index_name):
#             es.indices.delete_alias(index='_all', name=index_name, ignore=[404])
        
#         # Credef index_patent(ipath):
#     """
#     Comprehensive Patent Data Indexing Function

#     This sophisticated function transforms raw patent data into a structured, 
#     searchable Elasticsearch index. It's designed to handle large-scale patent 
#     document collections with precision and efficiency.

#     Key Objectives:
#     - Ingest patent data from CSV source
#     - Create a specialized Elasticsearch index for patent metadata
#     - Perform efficient bulk indexing
#     - Provide detailed error tracking and reporting
#     - Generate intermediate JSON for potential further processing

#     Args:
#         ipath (str): File path to the input CSV containing patent data

#     Returns:
#         int: Total number of successfully indexed patent records
#     """
#     # 🚀 Initialization and Setup
#     print('🌟 Initiating Comprehensive Patent Indexing Process...')
    
#     # Generate unique output path for intermediate JSON
#     timestamp = time.strftime("%Y%m%d_%H%M%S")
#     timestamped_index_name = f'patent_tmp_{timestamp}'
#     index_name = 'patent_tmp'  # This is the consistent name other functions will use
#     opath = os.path.join(os.path.dirname(ipath), f'patent_index_{timestamp}.json')
    
#     # 🔌 Elasticsearch Connection Establishment
#     print('🌐 Establishing Elasticsearch Connection...')
#     es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
#     # 🧹 Clean Slate: Remove any existing index to prevent conflicts
#     print(f"🗑️ Preparing index environment: Removing existing '{timestamped_index_name}' if present...")
#     es.indices.delete(index=timestamped_index_name, ignore=[400, 404])
    
#     # 🏗️ Precise Elasticsearch Mapping Definition
#     # Optimized for efficient searching and aggregation of patent metadata
#     mapping = {
#         "mappings": {
#             "properties": {
#                 # Keyword fields for exact matching
#                 "patent_id": {"type": "keyword"},
#                 "patent_type": {"type": "keyword"},
                
#                 # Text fields for full-text search capabilities
#                 "patent_title": {
#                     "type": "text",
#                     "fields": {
#                         "keyword": {"type": "keyword", "ignore_above": 256}
#                     }
#                 },
#                 "patent_abstract": {
#                     "type": "text",
#                     "fields": {
#                         "keyword": {"type": "keyword", "ignore_above": 256}
#                     }
#                 },
                
#                 # Date and numeric fields for precise filtering
#                 "patent_date": {"type": "date"},
#                 "num_claims": {"type": "integer"}
#             }
#         }
#     }
    
#     # 🏗️ Create Elasticsearch Index with Custom Mapping
#     print(f"📋 Creating index '{timestamped_index_name}' with specialized patent metadata mapping...")
#     es.indices.create(index=timestamped_index_name, body=mapping)
#     print(f"✅ Index '{timestamped_index_name}' successfully initialized")
    
#     # 📊 Diagnostic: Preview Input Data
#     print("🔍 Previewing Input Data Structure:")
#     with open(ipath, 'r') as f:
#         for i, line in enumerate(f):
#             if i < 3:
#                 print(f"Raw Line {i + 1}: {line.strip()}")
#             else:
#                 break
    
#     # Performance and Error Tracking
#     total_records = 0
#     total_errors = 0
#     processing_start_time = time.time()
    
#     # Prepare JSON output file for intermediate storage
#     json_output_records = []
    
#     # 🧩 Chunk-Based Processing Strategy
#     chunks = pd.read_csv(
#         ipath, 
#         sep=',', 
#         quoting=0, 
#         lineterminator='\n', 
#         dtype=str,  # Read all columns as strings initially
#         chunksize=50000,  # Manageable chunk size
#         on_bad_lines='skip'  # Gracefully handle problematic lines
#     )
    
#     # 🔄 Chunk Processing Loop
#     for chunk_idx, chunk in enumerate(chunks, 1):
#         print(f"\n🚧 Processing Chunk {chunk_idx}...")
        
#         # Normalize column names
#         chunk.columns = chunk.columns.str.strip()
#         print(f"📋 Columns in chunk: {chunk.columns.tolist()}")
#         print(f"📍 First row in chunk: {chunk.iloc[0].to_dict()}")
        
#         # 🧼 Data Preparation for Bulk Indexing
#         records = []
#         for _, patent in chunk.iterrows():
#             # Robust data cleaning and type conversion
#             try:
#                 # Safely convert numeric fields
#                 num_claims = int(patent['num_claims']) if pd.notna(patent['num_claims']) else 0
                
#                 # Create Elasticsearch index action
#                 action = {
#                     "_index": timestamped_index_name,
#                     "_source": {
#                         "patent_id": str(patent['patent_id']).strip(),
#                         "patent_title": str(patent['patent_title']).strip(),
#                         "patent_date": patent['patent_date'],
#                         "num_claims": num_claims,
#                         "patent_type": str(patent['patent_type']).strip(),
#                         "patent_abstract": str(patent['patent_abstract']).strip()
#                     }
#                 }
                
#                 records.append(action)
                
#                 # Prepare JSON for intermediate storage
#                 json_output_records.append(action['_source'])
            
#             except Exception as e:
#                 print(f"❌ Error processing patent record: {str(e)}")
#                 total_errors += 1
        
#         # 🚢 Bulk Indexing with Comprehensive Error Handling
#         if records:
#             print(f"📤 Bulk Indexing {len(records)} Patent Records...")
#             try:
#                 # Perform bulk indexing with refresh
#                 success, errors = elasticsearch.helpers.bulk(
#                     es, 
#                     records, 
#                     refresh=True,
#                     raise_on_error=False
#                 )
                
#                 total_records += success
                
#                 # Detailed Error Reporting
#                 if errors:
#                     total_errors += len(errors)
#                     print(f"⚠️ Encountered {len(errors)} indexing errors in chunk {chunk_idx}")
            
#             except Exception as e:
#                 print(f"❌ Critical Bulk Indexing Error: {str(e)}")
    
#     # 💾 Write Intermediate JSON
#     try:
#         with open(opath, 'w') as json_file:
#             json.dump(json_output_records, json_file, indent=2)
#         print(f"💾 Intermediate JSON output saved to: {opath}")
#     except Exception as e:
#         print(f"❌ Error writing JSON output: {str(e)}")
    
#     # Create alias for the timestamped index
#     try:
#         print(f"Creating alias '{index_name}' for '{timestamped_index_name}'...")
#         # Remove existing alias if it exists
#         if es.indices.exists_alias(name=index_name):
#             es.indices.delete_alias(index='_all', name=index_name, ignore=[404])
        
#         # Create new alias
#         es.indices.put_alias(index=timestamped_index_name, name=index_name)
#         print(f"✅ Alias '{index_name}' successfully created")
#     except Exception as e:
#         print(f"⚠️ Error creating alias: {e}")
#         print("⚠️ Other modules may not be able to find the patent data")
    
#     # 🏁 Indexing Completion
#     processing_time = time.time() - processing_start_time
#     print("\n📈 Indexing Process Summary:")
#     print(f"🔢 Total Records Processed: {total_records}")
#     print(f"❌ Total Processing Errors: {total_errors}")
#     print(f"⏱️ Total Processing Time: {processing_time:.2f} seconds")
    
#     # Final Index Refresh
#     print(f"\n🔁 Performing final index refresh for '{timestamped_index_name}'...")
#     es.indices.refresh(index=timestamped_index_name)
    
#     # Verification
#     if es.indices.exists(index=timestamped_index_name):
#         doc_count = es.count(index=timestamped_index_name)['count']
#         print(f"🏆 Final Document Count in '{timestamped_index_name}': {doc_count}")
#     else:
#         print(f"⚠️ WARNING: Index '{timestamped_index_name}' does not exist after indexing!")
    
#     return total_recordsxception as e:
#         print(f"⚠️ Error creating alias: {e}")
#         print("⚠️ Other modules may not be able to find the patent data")
    
#     # 🏁 Indexing Completion
#     processing_time = time.time() - processing_start_time
#     print("\n📈 Indexing Process Summary:")
#     print(f"🔢 Total Records Processed: {total_records}")
#     print(f"❌ Total Processing Errors: {total_errors}")
#     print(f"⏱️ Total Processing Time: {processing_time:.2f} seconds")
    
#     # Final Index Refresh
#     print(f"\n🔁 Performing final index refresh for '{timestamped_index_name}'...")
#     es.indices.refresh(index=timestamped_index_name)
    
#     # Verification
#     if es.indices.exists(index=timestamped_index_name):
#         doc_count = es.count(index=timestamped_index_name)['count']
#         print(f"🏆 Final Document Count in '{timestamped_index_name}': {doc_count}")
#     else:
#         print(f"⚠️ WARNING: Index '{timestamped_index_name}' does not exist after indexing!")
    
#     return total_records  timestamp = time.strftime("%Y%m%d_%H%M%S")
#     timestamped_index_name = f'patent_tmp_{timestamp}'
#     index_name = 'patent_tmp'  # This is the consistent name other functions will use
#     opath = os.path.join(os.path.dirname(ipath), f'patent_index_{timestamp}.json')
    
#     # 🔌 Elasticsearch Connection Establishment
#     print('🌐 Establishing Elasticsearch Connection...')
#     es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
#     # 🧹 Clean Slate: Remove any existing index to prevent conflicts
#     print(f"🗑️ Preparing index environment: Removing existing '{timestamped_index_name}' if present...")
#     es.indices.delete(index=timestamped_index_name, ignore=[400, 404])
    
#     # 🏗️ Precise Elasticsearch Mapping Definition
#     # Optimized for efficient searching and aggregation of patent metadata
#     mapping = {
#         "mappings": {
#             "properties": {
#                 # Keyword fields for exact matching
#                 "patent_id": {"type": "keyword"},
#                 "patent_type": {"type": "keyword"},
                
#                 # Text fields for full-text search capabilities
#                 "patent_title": {
#                     "type": "text",
#                     "fields": {
#                         "keyword": {"type": "keyword", "ignore_above": 256}
#                     }
#                 },
#                 "patent_abstract": {
#                     "type": "text",
#                     "fields": {
#                         "keyword": {"type": "keyword", "ignore_above": 256}
#                     }
#                 },
                
#                 # Date and numeric fields for precise filtering
#                 "patent_date": {"type": "date"},
#                 "num_claims": {"type": "integer"}
#             }
#         }
#     }
    
#     # 🏗️ Create Elasticsearch Index with Custom Mapping
#     print(f"📋 Creating index '{timestamped_index_name}' with specialized patent metadata mapping...")
#     es.indices.create(index=timestamped_index_name, body=mapping)
#     print(f"✅ Index '{timestamped_index_name}' successfully initialized")
    
#     # 📊 Diagnostic: Preview Input Data
#     print("🔍 Previewing Input Data Structure:")
#     with open(ipath, 'r') as f:
#         for i, line in enumerate(f):
#             if i < 3:
#                 print(f"Raw Line {i + 1}: {line.strip()}")
#             else:
#                 break
    
#     # Performance and Error Tracking
#     total_records = 0
#     total_errors = 0
#     processing_start_time = time.time()
    
#     # Prepare JSON output file for intermediate storage
#     json_output_records = []
    
#     # 🧩 Chunk-Based Processing Strategy
#     chunks = pd.read_csv(
#         ipath, 
#         sep=',', 
#         quoting=0, 
#         lineterminator='\n', 
#         dtype=str,  # Read all columns as strings initially
#         chunksize=50000,  # Manageable chunk size
#         on_bad_lines='skip'  # Gracefully handle problematic lines
#     )
    
#     # 🔄 Chunk Processing Loop
#     for chunk_idx, chunk in enumerate(chunks, 1):
#         print(f"\n🚧 Processing Chunk {chunk_idx}...")
        
#         # Normalize column names
#         chunk.columns = chunk.columns.str.strip()
#         print(f"📋 Columns in chunk: {chunk.columns.tolist()}")
#         print(f"📍 First row in chunk: {chunk.iloc[0].to_dict()}")
        
#         # 🧼 Data Preparation for Bulk Indexing
#         records = []
#         for _, patent in chunk.iterrows():
#             # Robust data cleaning and type conversion
#             try:
#                 # Safely convert numeric fields
#                 num_claims = int(patent['num_claims']) if pd.notna(patent['num_claims']) else 0
                
#                 # Create Elasticsearch index action
#                 action = {
#                     "_index": timestamped_index_name,
#                     "_source": {
#                         "patent_id": str(patent['patent_id']).strip(),
#                         "patent_title": str(patent['patent_title']).strip(),
#                         "patent_date": patent['patent_date'],
#                         "num_claims": num_claims,
#                         "patent_type": str(patent['patent_type']).strip(),
#                         "patent_abstract": str(patent['patent_abstract']).strip()
#                     }
#                 }
                
#                 records.append(action)
                
#                 # Prepare JSON for intermediate storage
#                 json_output_records.append(action['_source'])
            
#             except Exception as e:
#                 print(f"❌ Error processing patent record: {str(e)}")
#                 total_errors += 1
        
#         # 🚢 Bulk Indexing with Comprehensive Error Handling
#         if records:
#             print(f"📤 Bulk Indexing {len(records)} Patent Records...")
#             try:
#                 # Perform bulk indexing with refresh
#                 success, errors = elasticsearch.helpers.bulk(
#                     es, 
#                     records, 
#                     refresh=True,
#                     raise_on_error=False
#                 )
                
#                 total_records += success
                
#                 # Detailed Error Reporting
#                 if errors:
#                     total_errors += len(errors)
#                     print(f"⚠️ Encountered {len(errors)} indexing errors in chunk {chunk_idx}")
            
#             except Exception as e:
#                 print(f"❌ Critical Bulk Indexing Error: {str(e)}")
    
#     # 💾 Write Intermediate JSON
#     try:
#         with open(opath, 'w') as json_file:
#             json.dump(json_output_records, json_file, indent=2)
#         print(f"💾 Intermediate JSON output saved to: {opath}")
#     except Exception as e:
#         print(f"❌ Error writing JSON output: {str(e)}")
    
#     # Create alias for the timestamped index
#     try:
#         print(f"Creating alias '{index_name}' for '{timestamped_index_name}'...")
#         # Remove existing alias if it exists
#         if es.indices.exists_alias(name=index_name):
#             es.indices.delete_alias(index='_all', name=index_name, ignore=[404])
        
#         # Create new alias
#         es.indices.put_alias(index=timestamped_index_name, name=index_name)
#         print(f"✅ Alias '{index_name}' successfully created")
#     except Exception as e:
#         print(f"⚠️ Error creating alias: {e}")
#         print("⚠️ Other modules may not be able to find the patent data")
    
#     # 🏁 Indexing Completion
#     processing_time = time.time() - processing_start_time
#     print("\n📈 Indexing Process Summary:")
#     print(f"🔢 Total Records Processed: {total_records}")
#     print(f"❌ Total Processing Errors: {total_errors}")
#     print(f"⏱️ Total Processing Time: {processing_time:.2f} seconds")
    
#     # Final Index Refresh
#     print(f"\n🔁 Performing final index refresh for '{timestamped_index_name}'...")
#     es.indices.refresh(index=timestamped_index_name)
    
#     # Verification
#     if es.indices.exists(index=timestamped_index_name):
#         doc_count = es.count(index=timestamped_index_name)['count']
#         print(f"🏆 Final Document Count in '{timestamped_index_name}': {doc_count}")
#     else:
#         print(f"⚠️ WARNING: Index '{timestamped_index_name}' does not exist after indexing!")
    
#     return total_records

import os
import argparse
import json
import time
import re 
import pandas as pd
import elasticsearch
import elasticsearch.helpers

from es import create_index, refresh, bulk_insert

def index_patent(ipath):
    """
    Comprehensive Patent Data Indexing Function

    This sophisticated function transforms raw patent data into a structured, 
    searchable Elasticsearch index. It's designed to handle large-scale patent 
    document collections with precision and efficiency.

    Key Objectives:
    - Ingest patent data from CSV source
    - Create a specialized Elasticsearch index for patent metadata
    - Perform efficient bulk indexing
    - Provide detailed error tracking and reporting
    - Generate intermediate JSON for potential further processing

    Args:
        ipath (str): File path to the input CSV containing patent data

    Returns:
        int: Total number of successfully indexed patent records
    """
    # 🚀 Initialization and Setup
    print('🌟 Initiating Comprehensive Patent Indexing Process...')
    
    # Generate unique output path for intermediate JSON
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    timestamped_index_name = f'patent_tmp_{timestamp}'
    index_name = 'patent_tmp'  # This is the consistent name other functions will use
    opath = os.path.join(os.path.dirname(ipath), f'patent_index_{timestamp}.json')
    
    # 🔌 Elasticsearch Connection Establishment
    print('🌐 Establishing Elasticsearch Connection...')
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
    # 🧹 Clean Slate: Remove any existing index to prevent conflicts
    print(f"🗑️ Preparing index environment: Removing existing '{timestamped_index_name}' if present...")
    es.indices.delete(index=timestamped_index_name, ignore=[400, 404])
    
    # 🏗️ Precise Elasticsearch Mapping Definition
    # Optimized for efficient searching and aggregation of patent metadata
    mapping = {
        "mappings": {
            "properties": {
                # Keyword fields for exact matching
                "patent_id": {"type": "keyword"},
                "patent_type": {"type": "keyword"},
                
                # Text fields for full-text search capabilities
                "patent_title": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                },
                "patent_abstract": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword", "ignore_above": 256}
                    }
                },
                
                # Date and numeric fields for precise filtering
                "patent_date": {"type": "date"},
                "num_claims": {"type": "integer"}
            }
        }
    }
    
    # 🏗️ Create Elasticsearch Index with Custom Mapping
    print(f"📋 Creating index '{timestamped_index_name}' with specialized patent metadata mapping...")
    es.indices.create(index=timestamped_index_name, body=mapping)
    print(f"✅ Index '{timestamped_index_name}' successfully initialized")
    
    # 📊 Diagnostic: Preview Input Data
    print("🔍 Previewing Input Data Structure:")
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 3:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    # Performance and Error Tracking
    total_records = 0
    total_errors = 0
    processing_start_time = time.time()
    
    # Prepare JSON output file for intermediate storage
    json_output_records = []
    
    # 🧩 Chunk-Based Processing Strategy
    chunks = pd.read_csv(
        ipath, 
        sep=',', 
        quoting=0, 
        lineterminator='\n', 
        dtype=str,  # Read all columns as strings initially
        chunksize=50000,  # Manageable chunk size
        on_bad_lines='skip'  # Gracefully handle problematic lines
    )
    
    # 🔄 Chunk Processing Loop
    for chunk_idx, chunk in enumerate(chunks, 1):
        print(f"\n🚧 Processing Chunk {chunk_idx}...")
        
        # Normalize column names
        chunk.columns = chunk.columns.str.strip()
        print(f"📋 Columns in chunk: {chunk.columns.tolist()}")
        print(f"📍 First row in chunk: {chunk.iloc[0].to_dict()}")
        
        # 🧼 Data Preparation for Bulk Indexing
        records = []
        for _, patent in chunk.iterrows():
            # Robust data cleaning and type conversion
            try:
                # Safely convert numeric fields
                num_claims = int(patent['num_claims']) if pd.notna(patent['num_claims']) else 0
                
                # Create Elasticsearch index action
                action = {
                    "_index": timestamped_index_name,
                    "_source": {
                        "patent_id": str(patent['patent_id']).strip(),
                        "patent_title": str(patent['patent_title']).strip(),
                        "patent_date": patent['patent_date'],
                        "num_claims": num_claims,
                        "patent_type": str(patent['patent_type']).strip(),
                        "patent_abstract": str(patent['patent_abstract']).strip()
                    }
                }
                
                records.append(action)
                
                # Prepare JSON for intermediate storage
                json_output_records.append(action['_source'])
            
            except Exception as e:
                print(f"❌ Error processing patent record: {str(e)}")
                total_errors += 1
        
        # 🚢 Bulk Indexing with Comprehensive Error Handling
        if records:
            print(f"📤 Bulk Indexing {len(records)} Patent Records...")
            try:
                # Perform bulk indexing with refresh
                success, errors = elasticsearch.helpers.bulk(
                    es, 
                    records, 
                    refresh=True,
                    raise_on_error=False
                )
                
                total_records += success
                
                # Detailed Error Reporting
                if errors:
                    total_errors += len(errors)
                    print(f"⚠️ Encountered {len(errors)} indexing errors in chunk {chunk_idx}")
            
            except Exception as e:
                print(f"❌ Critical Bulk Indexing Error: {str(e)}")
    
    # 💾 Write Intermediate JSON
    try:
        with open(opath, 'w') as json_file:
            json.dump(json_output_records, json_file, indent=2)
        print(f"💾 Intermediate JSON output saved to: {opath}")
    except Exception as e:
        print(f"❌ Error writing JSON output: {str(e)}")
    
    # Create alias for the timestamped index
    try:
        print(f"Creating alias '{index_name}' for '{timestamped_index_name}'...")
        # Remove existing alias if it exists
        if es.indices.exists_alias(name=index_name):
            es.indices.delete_alias(index='_all', name=index_name, ignore=[404])
        
        # Create new alias
        es.indices.put_alias(index=timestamped_index_name, name=index_name)
        print(f"✅ Alias '{index_name}' successfully created")
    except Exception as e:
        print(f"⚠️ Error creating alias: {e}")
        print("⚠️ Other modules may not be able to find the patent data")
    
    # 🏁 Indexing Completion
    processing_time = time.time() - processing_start_time
    print("\n📈 Indexing Process Summary:")
    print(f"🔢 Total Records Processed: {total_records}")
    print(f"❌ Total Processing Errors: {total_errors}")
    print(f"⏱️ Total Processing Time: {processing_time:.2f} seconds")
    
    # Final Index Refresh
    print(f"\n🔁 Performing final index refresh for '{timestamped_index_name}'...")
    es.indices.refresh(index=timestamped_index_name)
    
    # Verification
    if es.indices.exists(index=timestamped_index_name):
        doc_count = es.count(index=timestamped_index_name)['count']
        print(f"🏆 Final Document Count in '{timestamped_index_name}': {doc_count}")
    else:
        print(f"⚠️ WARNING: Index '{timestamped_index_name}' does not exist after indexing!")
    
    return total_records