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
    print('Starting patent indexing process...')
    index_name = 'patent_tmp'
    opath = os.path.join(os.path.dirname(ipath), 'patent.index_tmp.json')
    
    # Initialize Elasticsearch client and delete existing index
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    print(f"Deleting existing index '{index_name}' if it exists...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define mapping for Elasticsearch 
    mapping = {
        "mappings": {
            "properties": {
                "patent_id": {"type": "keyword"},
                "patent_title": {"type": "text"},
                "patent_date": {"type": "date"},
                "num_claims": {"type": "integer"},
                "patent_type": {"type": "keyword"},
                "patent_abstract": {"type": "text"}
            }
        }
    }
    
    # Create the index with proper mapping
    print(f"Creating index '{index_name}' with mapping...")
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully")
    
    # Debug: Print first few raw lines
    print("Reading sample lines from input file:")
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    total_records = 0
    chunks = pd.read_csv(ipath, sep=',', quoting=0, lineterminator='\n', dtype=str, chunksize=50000, on_bad_lines='skip')
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing chunk {chunk_idx+1}...")
        chunk.columns = chunk.columns.str.strip()
        print(f"Columns in chunk: {chunk.columns.tolist()}")
        print(f"First row in chunk: {chunk.iloc[0].to_dict()}")
        
        records = []
        for _, patent in chunk.iterrows():
            # Create the index action
            action = {
                "_index": index_name,
                "_source": {
                    "patent_id": patent['patent_id'],
                    "patent_title": patent['patent_title'],
                    "patent_date": patent['patent_date'],
                    "num_claims": int(patent['num_claims']) if pd.notna(patent['num_claims']) else 0,
                    "patent_type": patent['patent_type'],
                    "patent_abstract": patent['patent_abstract']
                }
            }
            records.append(action)
        
        # Perform bulk indexing directly using elasticsearch-helpers
        if records:
            print(f"Bulk indexing {len(records)} records...")
            success, errors = elasticsearch.helpers.bulk(es, records, refresh=True)
            if errors:
                print(f"Errors during bulk indexing: {errors}")
            print(f"Successfully indexed {success} records")
            total_records += success
    
    print(f"Refreshing index '{index_name}'...")
    es.indices.refresh(index=index_name)
    print(f"Total records indexed: {total_records}")
    
    # Verify index exists and has documents
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"Index '{index_name}' exists with {count['count']} documents")
    else:
        print(f"WARNING: Index '{index_name}' does not exist after indexing!")
    
    return total_records

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

##  Patent Summary Data
def index_summary(ipath):
    print('Indexing Brief Summary.')
    index_name = 'summary_tmp'
    opath = os.path.join(os.path.dirname(ipath), 'summary.index_tmp.json')
    create_index(index_name)
    
    # Debug: Print first few raw lines
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    chunks = pd.read_csv(ipath, sep='\t', quoting=0, lineterminator='\n', dtype=str, chunksize=50000, on_bad_lines='warn')
    for chunk in chunks:
        chunk.columns = chunk.columns.str.strip()  # Clean column names
        print("Columns in chunk:", chunk.columns.tolist())
        print("First row in chunk:", chunk.iloc[0].to_dict())  # Added first row output
        
        with open(opath, 'w') as ofp:
            for _, claim in chunk.iterrows():
                # Handle NaN values by replacing them with an empty string
                summary_text = str(claim['summary_text']) if pd.notna(claim['summary_text']) else ''

                # Clean summary_text: remove newlines, commas, and punctuations
                clean_summary = re.sub(r'[^\w\s]', '', summary_text).replace('\n', ' ')
                clean_summary = re.sub(r'\s+', ' ', clean_summary).strip()  # Replace multiple spaces with a single space

                json.dump({'index': {'_index': index_name}}, ofp)
                ofp.write('\n')
                json.dump({
                    'patent_id': claim['patent_id'],
                    'summary': clean_summary  # Use cleaned summary
                }, ofp)
                ofp.write('\n')
        
        bulk_insert(index_name, opath)
    refresh(index_name)
    os.remove(opath)
###
def index_people(ipath):
    index_name = 'patent_people_tmp'
    
    # Initialize Elasticsearch client
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    print(f"Deleting existing index '{index_name}' if it exists...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define mapping for patent_people data
    mapping = {
        "mappings": {
            "properties": {
                "patent_id": {"type": "keyword"},
                "applicant_authority": {"type": "keyword"},
                "applicant_organization": {"type": "text"},
                "applicant_full_name": {"type": "text"},
                "assignee_id": {"type": "keyword"},
                "assignee_organization": {"type": "text"},
                "assignee_full_name": {"type": "text"},
                "inventor_id": {"type": "keyword"},
                "gender_code": {"type": "keyword"},
                "inventor_full_name": {"type": "text"}
            }
        }
    }
    
    # Create the index with mapping
    print(f"Creating index '{index_name}' with mapping...")
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully")
    
    # Debug: Print first few raw lines
    print("Reading sample lines from input file:")
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    total_records = 0
    chunks = pd.read_csv(ipath, sep=',', quoting=0, lineterminator='\n', dtype=str, chunksize=50000, on_bad_lines='skip')
    
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing chunk {chunk_idx+1}...")
        chunk.columns = chunk.columns.str.strip()
        print(f"Columns in chunk: {chunk.columns.tolist()}")
        print(f"First row in chunk: {chunk.iloc[0].to_dict()}")
        
        records = []
        for _, row in chunk.iterrows():
            # Ensure all fields are strings to prevent NaN issues
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
        
        # Perform bulk indexing
        if records:
            print(f"Bulk indexing {len(records)} records...")
            try:
                success, errors = elasticsearch.helpers.bulk(es, records, refresh=True)
                if errors:
                    print(f"Errors during bulk indexing (first 5): {errors[:5]}")
                print(f"Successfully indexed {success} records")
                total_records += success
            except elasticsearch.ElasticsearchException as e:
                print(f"Elasticsearch bulk index error: {e}")
    
    print(f"Refreshing index '{index_name}'...")
    es.indices.refresh(index=index_name)
    print(f"Total records indexed: {total_records}")
    
    # Verify index exists and has documents
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"Index '{index_name}' exists with {count['count']} documents")
    else:
        print(f"WARNING: Index '{index_name}' does not exist after indexing!")
    
    return total_records

###
def index_summary(ipath):
    print('Starting summary indexing process...')
    index_name = 'summary_tmp'
    
    # Initialize Elasticsearch client
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    print(f"Deleting existing index '{index_name}' if it exists...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define mapping for summary data
    mapping = {
        "mappings": {
            "properties": {
                "patent_id": {"type": "keyword"},
                "summary_text": {"type": "text"}
            }
        }
    }
    
    # Create the index with proper mapping
    print(f"Creating index '{index_name}' with mapping...")
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully")
    
    # Debug: Print first few raw lines
    print("Reading sample lines from summary input file:")
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    total_records = 0
    chunks = pd.read_csv(ipath, sep=',', quoting=0, lineterminator='\n', dtype=str, chunksize=50000, on_bad_lines='warn')
    
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing summary chunk {chunk_idx+1}...")
        chunk.columns = chunk.columns.str.strip()
        print(f"Columns in chunk: {chunk.columns.tolist()}")
        print(f"First row in chunk: {chunk.iloc[0].to_dict()}")
        
        records = []
        for _, summary in chunk.iterrows():
            # Handle NaN values and clean the summary text
            summary_text = str(summary['summary_text']) if pd.notna(summary['summary_text']) else ''
            clean_summary = re.sub(r'[^\w\s]', '', summary_text).replace('\n', ' ')
            clean_summary = re.sub(r'\s+', ' ', clean_summary).strip()
            
            # Create the index action
            action = {
                "_index": index_name,
                "_source": {
                    "patent_id": summary['patent_id'],
                    "summary": clean_summary
                }
            }
            records.append(action)
        
        # Perform bulk indexing directly
        if records:
            print(f"Bulk indexing {len(records)} summary records...")
            success, errors = elasticsearch.helpers.bulk(es, records, refresh=True)
            if errors:
                print(f"Errors during bulk indexing: {errors}")
            print(f"Successfully indexed {success} summary records")
            total_records += success
    
    print(f"Refreshing index '{index_name}'...")
    es.indices.refresh(index=index_name)
    print(f"Total summary records indexed: {total_records}")
    
    # Verify index exists and has documents
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"Index '{index_name}' exists with {count['count']} documents")
    else:
        print(f"WARNING: Index '{index_name}' does not exist after indexing!")
    
    return total_records

##
def index_claim(ipath):
    print('Starting claims indexing process...')
    index_name = 'claim_tmp'
    
    # Initialize Elasticsearch client
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    print(f"Deleting existing index '{index_name}' if it exists...")
    es.indices.delete(index=index_name, ignore=[400, 404])
    
    # Define mapping for claims data with all fields
    mapping = {
        "mappings": {
            "properties": {
                "patent_id": {"type": "keyword"},
                "claim_sequence": {"type": "integer"},
                "claim_text": {"type": "text"},
                "dependent": {"type": "boolean"},
                "claim_number": {"type": "integer"},
                "exemplary": {"type": "boolean"}
            }
        }
    }
    
    # Create the index with proper mapping
    print(f"Creating index '{index_name}' with mapping...")
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created successfully")
    
    # Debug: Print first few raw lines
    print("Reading sample lines from claims input file:")
    with open(ipath, 'r') as f:
        for i, line in enumerate(f):
            if i < 4:
                print(f"Raw Line {i + 1}: {line.strip()}")
            else:
                break
    
    total_records = 0
    chunks = pd.read_csv(ipath, sep=',', quoting=0, lineterminator='\n', dtype=str, chunksize=50000, on_bad_lines='skip')
    
    for chunk_idx, chunk in enumerate(chunks):
        print(f"Processing claims chunk {chunk_idx+1}...")
        chunk.columns = chunk.columns.str.strip()
        print(f"Columns in chunk: {chunk.columns.tolist()}")
        print(f"First row in chunk: {chunk.iloc[0].to_dict()}")
        
        records = []
        for _, claim in chunk.iterrows():
            # Handle NaN values and clean data
            claim_text = str(claim['claim_text']) if pd.notna(claim['claim_text']) else ''
            
            # Convert fields to appropriate types
            try:
                claim_sequence = int(claim['claim_sequence']) if pd.notna(claim['claim_sequence']) else 0
            except ValueError:
                claim_sequence = 0
                
            try:
                claim_number = int(claim['claim_number']) if pd.notna(claim['claim_number']) else 0
            except ValueError:
                claim_number = 0
                
            # Convert dependent and exemplary to boolean
            dependent = str(claim['dependent']).lower() in ('true', 't', 'yes', 'y', '1') if pd.notna(claim['dependent']) else False
            exemplary = str(claim['exemplary']).lower() in ('true', 't', 'yes', 'y', '1') if pd.notna(claim['exemplary']) else False
            
            # Create the index action
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
        
        # Perform bulk indexing directly
        if records:
            print(f"Bulk indexing {len(records)} claim records...")
            success, errors = elasticsearch.helpers.bulk(es, records, refresh=True)
            if errors:
                print(f"Errors during bulk indexing: {errors}")
            print(f"Successfully indexed {success} claim records")
            total_records += success
    
    print(f"Refreshing index '{index_name}'...")
    es.indices.refresh(index=index_name)
    print(f"Total claim records indexed: {total_records}")
    
    # Verify index exists and has documents
    if es.indices.exists(index=index_name):
        count = es.count(index=index_name)
        print(f"Index '{index_name}' exists with {count['count']} documents")
    else:
        print(f"WARNING: Index '{index_name}' does not exist after indexing!")
    
    return total_records

# def index_patentsview_for_elasticsearch(args):
#     print("Starting index_patentsview_for_elasticsearch process...")
    
#     es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
#     if not es.indices.exists(index='patent_tmp'):
#         print("ERROR: Index 'patent_tmp' does not exist.")
#         return
    
#     print("Verified that 'patent_tmp' index exists")

#     # Patentsview index mapping 
#     if not es.indices.exists(index='patentsview'):
#         mapping = {
#             "mappings": {
#                 "properties": {
#                     "patent_id": {"type": "keyword"},
#                     "patent_title": {"type": "text"},
#                     "patent_date": {"type": "date"},
#                     "num_claims": {"type": "integer"},
#                     "patent_type": {"type": "keyword"},
#                     "patent_abstract": {"type": "text"},
#                     "summary": {"type": "text"},
#                     "claims_text": {"type": "text"},
#                     "claims": {
#                         "type": "nested",
#                         "properties": {
#                             "claim_sequence": {"type": "integer"},
#                             "claim_text": {"type": "text"},
#                             "dependent": {"type": "boolean"},
#                             "claim_number": {"type": "integer"},
#                             "exemplary": {"type": "boolean"}
#                         }
#                     },
#                     "people": {
#                         "type": "nested",
#                         "properties": {
#                             "applicant_authority": {"type": "keyword"},
#                             "applicant_organization": {"type": "text"},
#                             "applicant_full_name": {"type": "text"},
#                             "assignee_id": {"type": "keyword"},
#                             "assignee_organization": {"type": "text"},
#                             "assignee_full_name": {"type": "text"},
#                             "inventor_id": {"type": "keyword"},
#                             "gender_code": {"type": "keyword"},
#                             "inventor_full_name": {"type": "text"}
#                         }
#                     },
#                     "cpc_classes": {
#                         "type": "nested",
#                         "properties": {
#                             "cpc_section": {"type": "keyword"},
#                             "cpc_class": {"type": "keyword"},
#                             "cpc_subclass": {"type": "keyword"},
#                             "cpc_group": {"type": "keyword"},
#                             "cpc_type": {"type": "keyword"},
#                             "cpc_group_title": {"type": "text"},
#                             "cpc_class_title": {"type": "text"}
#                         }
#                     }
#                 }
#             }
#         }
#         print("Creating 'patentsview' index...")
#         es.indices.create(index='patentsview', body=mapping)
#         print("Created 'patentsview' index successfully")

#     try:
#         hits = elasticsearch.helpers.scan(es, index='patent_tmp', query={"query": {"match_all": {}}})
#         actions = []
#         processed_count = 0
        
#         for hit in hits:
#             patent = hit['_source']
#             pid = patent['patent_id']
            
#             # Fetch summary from summary_tmp if it exists
#             if es.indices.exists(index='summary_tmp'):
#                 summary_response = es.search(
#                     index='summary_tmp',
#                     query={'match': {'patent_id': pid}},
#                     size=1
#                 )
#                 if summary_response['hits']['total']['value'] > 0:
#                     patent['summary'] = summary_response['hits']['hits'][0]['_source']['summary']
            
#             # Fetch claims from claim_tmp if it exists
#             if es.indices.exists(index='claim_tmp'):
#                 claims_response = es.search(
#                     index='claim_tmp',
#                     query={'match': {'patent_id': pid}},
#                     size=200
#                 )
                
#                 if claims_response['hits']['total']['value'] > 0:
#                     claims_hits = claims_response['hits']['hits']
                    
#                     claims_objects = []
#                     claims_texts = []
                    
#                     for claim_hit in claims_hits:
#                         claim_source = claim_hit['_source']
#                         claims_objects.append({
#                             "claim_sequence": claim_source.get('claim_sequence', 0),
#                             "claim_text": claim_source.get('claim_text', ''),
#                             "dependent": claim_source.get('dependent', False),
#                             "claim_number": claim_source.get('claim_number', 0),
#                             "exemplary": claim_source.get('exemplary', False)
#                         })
#                         claims_texts.append(claim_source.get('claim_text', ''))
                    
#                     patent['claims'] = claims_objects
#                     patent['claims_text'] = " ".join(claims_texts)
            
#             # Fetch people data from patent_people_tmp if it exists
#             if es.indices.exists(index='patent_people_tmp'):
#                 people_response = es.search(
#                     index='patent_people_tmp',
#                     query={'match': {'patent_id': pid}},
#                     size=100
#                 )
                
#                 if people_response['hits']['total']['value'] > 0:
#                     people_hits = people_response['hits']['hits']
                    
#                     people_objects = []
#                     for people_hit in people_hits:
#                         people_source = people_hit['_source']
#                         people_objects.append(people_source)
                    
#                     patent['people'] = people_objects
            
#             # Fetch CPC classifications from cpc_classes_tmp if it exists
#             if es.indices.exists(index='cpc_classes_tmp'):
#                 cpc_response = es.search(
#                     index='cpc_classes_tmp',
#                     query={'match': {'patent_id': pid}},
#                     size=50
#                 )
                
#                 if cpc_response['hits']['total']['value'] > 0:
#                     cpc_hits = cpc_response['hits']['hits']
                    
#                     cpc_objects = []
#                     for cpc_hit in cpc_hits:
#                         cpc_source = cpc_hit['_source']
#                         cpc_objects.append({
#                             "cpc_section": cpc_source.get('cpc_section', ''),
#                             "cpc_class": cpc_source.get('cpc_class', ''),
#                             "cpc_subclass": cpc_source.get('cpc_subclass', ''),
#                             "cpc_group": cpc_source.get('cpc_group', ''),
#                             "cpc_type": cpc_source.get('cpc_type', ''),
#                             "cpc_group_title": cpc_source.get('cpc_group_title', ''),
#                             "cpc_class_title": cpc_source.get('cpc_class_title', '')
#                         })
                    
#                     patent['cpc_classes'] = cpc_objects

#             action = {
#                 "_index": "patentsview",
#                 "_source": patent
#             }
#             actions.append(action)
            
#             processed_count += 1
#             if processed_count % 1000 == 0:
#                 print(f"Processed {processed_count} patents")
#                 if actions:
#                     success, errors = elasticsearch.helpers.bulk(es, actions, refresh=False)
#                     if errors:
#                         print(f"Errors during bulk indexing: {errors}")
#                     actions = []
        
#         if actions:
#             print(f"Processing final batch of {len(actions)} patents...")
#             success, errors = elasticsearch.helpers.bulk(es, actions, refresh=True)
#             if errors:
#                 print(f"Errors during final bulk indexing: {errors}")
        
#         print(f"Successfully processed {processed_count} patents to 'patentsview' index")
#         es.indices.refresh(index='patentsview')
        
#         count_result = es.count(index='patentsview')
#         print(f"Final count in 'patentsview' index: {count_result['count']} documents")
        
#     except elasticsearch.NotFoundError as e:
#         print(f"Elasticsearch error: {e}")
#     except Exception as e:
#         print(f"Error during indexing: {e}")

def index_patentsview_for_elasticsearch(args):
    print("Starting index_patentsview_for_elasticsearch process...")
    
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
    # Get all indices and find the one that starts with patent_tmp
    indices = es.indices.get("*")
    patent_indices = [idx for idx in indices if idx.startswith('patent_tmp')]
    
    if not patent_indices:
        print("ERROR: No index starting with 'patent_tmp' exists.")
        return
    
    # Use the first patent_tmp index found (or you could use the most recent one based on timestamp)
    patent_tmp_index = patent_indices[0]
    print(f"Found patent index: {patent_tmp_index}")

    # Patentsview index mapping 
    if not es.indices.exists(index='patentsview'):
        mapping = {
            "mappings": {
                "properties": {
                    "patent_id": {"type": "keyword"},
                    "patent_title": {"type": "text"},
                    "patent_date": {"type": "date"},
                    "num_claims": {"type": "integer"},
                    "patent_type": {"type": "keyword"},
                    "patent_abstract": {"type": "text"},
                    "summary": {"type": "text"},
                    "claims_text": {"type": "text"},
                    "claims": {
                        "type": "nested",
                        "properties": {
                            "claim_sequence": {"type": "integer"},
                            "claim_text": {"type": "text"},
                            "dependent": {"type": "boolean"},
                            "claim_number": {"type": "integer"},
                            "exemplary": {"type": "boolean"}
                        }
                    },
                    "people": {
                        "type": "nested",
                        "properties": {
                            "applicant_authority": {"type": "keyword"},
                            "applicant_organization": {"type": "text"},
                            "applicant_full_name": {"type": "text"},
                            "assignee_id": {"type": "keyword"},
                            "assignee_organization": {"type": "text"},
                            "assignee_full_name": {"type": "text"},
                            "inventor_id": {"type": "keyword"},
                            "gender_code": {"type": "keyword"},
                            "inventor_full_name": {"type": "text"}
                        }
                    },
                    "cpc_classes": {
                        "type": "nested",
                        "properties": {
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
            }
        }
        print("Creating 'patentsview' index...")
        es.indices.create(index='patentsview', body=mapping)
        print("Created 'patentsview' index successfully")

    try:
        # Use the found patent index instead of hardcoded 'patent_tmp'
        hits = elasticsearch.helpers.scan(es, index=patent_tmp_index, query={"query": {"match_all": {}}})
        actions = []
        processed_count = 0
        
        for hit in hits:
            patent = hit['_source']
            pid = patent['patent_id']
            
            # Fetch summary from summary_tmp if it exists
            if es.indices.exists(index='patent_summary_tmp'):
                summary_response = es.search(
                    index='patent_summary_tmp',
                    query={'match': {'patent_id': pid}},
                    size=1
                )
                if summary_response['hits']['total']['value'] > 0:
                    patent['summary'] = summary_response['hits']['hits'][0]['_source']['summary']
            
            # Fetch claims from claim_tmp if it exists
            if es.indices.exists(index='claim_tmp'):
                claims_response = es.search(
                    index='claim_tmp',
                    query={'match': {'patent_id': pid}},
                    size=200
                )
                
                if claims_response['hits']['total']['value'] > 0:
                    claims_hits = claims_response['hits']['hits']
                    
                    claims_objects = []
                    claims_texts = []
                    
                    for claim_hit in claims_hits:
                        claim_source = claim_hit['_source']
                        claims_objects.append({
                            "claim_sequence": claim_source.get('claim_sequence', 0),
                            "claim_text": claim_source.get('claim_text', ''),
                            "dependent": claim_source.get('dependent', False),
                            "claim_number": claim_source.get('claim_number', 0),
                            "exemplary": claim_source.get('exemplary', False)
                        })
                        claims_texts.append(claim_source.get('claim_text', ''))
                    
                    patent['claims'] = claims_objects
                    patent['claims_text'] = " ".join(claims_texts)
            
            # Fetch people data from patent_people_tmp if it exists
            if es.indices.exists(index='patent_people_tmp'):
                people_response = es.search(
                    index='patent_people_tmp',
                    query={'match': {'patent_id': pid}},
                    size=100
                )
                
                if people_response['hits']['total']['value'] > 0:
                    people_hits = people_response['hits']['hits']
                    
                    people_objects = []
                    for people_hit in people_hits:
                        people_source = people_hit['_source']
                        people_objects.append(people_source)
                    
                    patent['people'] = people_objects
            
            # Fetch CPC classifications from cpc_classes_tmp if it exists
            if es.indices.exists(index='cpc_classes_tmp'):
                cpc_response = es.search(
                    index='cpc_classes_tmp',
                    query={'match': {'patent_id': pid}},
                    size=50
                )
                
                if cpc_response['hits']['total']['value'] > 0:
                    cpc_hits = cpc_response['hits']['hits']
                    
                    cpc_objects = []
                    for cpc_hit in cpc_hits:
                        cpc_source = cpc_hit['_source']
                        cpc_objects.append({
                            "cpc_section": cpc_source.get('cpc_section', ''),
                            "cpc_class": cpc_source.get('cpc_class', ''),
                            "cpc_subclass": cpc_source.get('cpc_subclass', ''),
                            "cpc_group": cpc_source.get('cpc_group', ''),
                            "cpc_type": cpc_source.get('cpc_type', ''),
                            "cpc_group_title": cpc_source.get('cpc_group_title', ''),
                            "cpc_class_title": cpc_source.get('cpc_class_title', '')
                        })
                    
                    patent['cpc_classes'] = cpc_objects

            action = {
                "_index": "patentsview",
                "_source": patent
            }
            actions.append(action)
            
            processed_count += 1
            if processed_count % 1000 == 0:
                print(f"Processed {processed_count} patents")
                if actions:
                    success, errors = elasticsearch.helpers.bulk(es, actions, refresh=False)
                    if errors:
                        print(f"Errors during bulk indexing: {errors}")
                    actions = []
        
        if actions:
            print(f"Processing final batch of {len(actions)} patents...")
            success, errors = elasticsearch.helpers.bulk(es, actions, refresh=True)
            if errors:
                print(f"Errors during final bulk indexing: {errors}")
        
        print(f"Successfully processed {processed_count} patents to 'patentsview' index")
        es.indices.refresh(index='patentsview')
        
        count_result = es.count(index='patentsview')
        print(f"Final count in 'patentsview' index: {count_result['count']} documents")
        
    except elasticsearch.NotFoundError as e:
        print(f"Elasticsearch error: {e}")
    except Exception as e:
        print(f"Error during indexing: {e}")

if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('--patent', type=str, help='Path to patent_data.csv')
    pparser.add_argument('--classes', type=str, help='Path to patent_classes.csv')  # Add classes argument
    pparser.add_argument('--people', type=str, help='Path to patent_people.csv')
    pparser.add_argument('--summary', type=str, help='Path to patent_brief_sum.csv')
    pparser.add_argument('--claim', type=str, help='Path to patent_claims.csv')
    args = pparser.parse_args()
    
    records_indexed = 0

    # Process patent file first
    if args.patent:
        print(f"Processing patent file: {args.patent}")
        records_indexed = index_patent(args.patent)

    # Process patent classes immediately after patents
    if args.classes:
        print(f"Processing patent classes file: {args.classes}")
        classes_records = index_classes(args.classes)  

    # Process people file
    if args.people:
        print(f"Processing people file: {args.people}")
        people_records = index_people(args.people)

    # Process summary file
    if args.summary:
        print(f"Processing summary file: {args.summary}")
        summary_records = index_summary(args.summary)

    # Process claims file
    if args.claim:
        print(f"Processing claims file: {args.claim}")
        claim_records = index_claim(args.claim)

    # Ensure patentsview is indexed only if patents were processed
    if records_indexed > 0:
        print("Waiting for Elasticsearch to process documents...")
        time.sleep(2)
        index_patentsview_for_elasticsearch(args)
    else:
        print("No patent records were indexed. Skipping patentsview indexing.")
