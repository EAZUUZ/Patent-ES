import os
import argparse
import json
import time
import re 
import pandas as pd
import elasticsearch
import elasticsearch.helpers

from es import create_index, refresh, bulk_insert

from index_claim import index_claim 
from index_class import index_classes
from index_patent import index_patent
from index_people import index_people
from index_summary import index_summary
from index_us_app_citation import index_us_app_citation
from index_us_citation import index_us_citations

def index_patentsview_for_elasticsearch(args):
    print("Starting index_patentsview_for_elasticsearch process...")
    
    es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
    
    # Check if the patent index exists, either as direct index or as alias
    if not es.indices.exists(index='patent_tmp'):
        # Try to find any index that might contain patent data (as fallback)
        try:
            indices = es.indices.get('patent_tmp_*')
            if indices:
                # Get the most recent index (assuming timestamp format)
                most_recent = sorted(indices.keys())[-1]
                print(f"WARNING: 'patent_tmp' not found, but found similar index: {most_recent}")
                print(f"Creating alias 'patent_tmp' to {most_recent}...")
                # Create alias
                es.indices.put_alias(index=most_recent, name='patent_tmp')
                print(f"Created alias 'patent_tmp' to {most_recent}")
            else:
                print("ERROR: No patent indices found. Make sure to run index_patent first.")
                return
        except Exception as e:
            print(f"ERROR: Index 'patent_tmp' does not exist and fallback failed: {e}")
            print("Make sure to run index_patent first.")
            return
    
    print("Verified that 'patent_tmp' index exists")

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
                    },
                    "us_app_citations": {
                        "type": "nested",
                        "properties": {
                            "citation_sequence": {"type": "integer"},
                            "citation_document_number": {"type": "keyword"},
                            "citation_date": {"type": "date"},
                            "record_name": {"type": "text"},
                            "wipo_kind": {"type": "keyword"},
                            "citation_category": {"type": "keyword"}
                        }
                    },
                    "us_citations": {
                        "type": "nested",
                        "properties": {
                            "citation_sequence": {"type": "integer"},
                            "citation_document_number": {"type": "keyword"},
                            "citation_date": {"type": "date"},
                            "record_name": {"type": "text"},
                            "wipo_kind": {"type": "keyword"},
                            "citation_category": {"type": "keyword"}
                        }
                    }
                }
            }
        }
        print("Creating 'patentsview' index...")
        es.indices.create(index='patentsview', body=mapping)
        print("Created 'patentsview' index successfully")

    try:
        hits = elasticsearch.helpers.scan(es, index='patent_tmp', query={"query": {"match_all": {}}})
        actions = []
        processed_count = 0
        
        for hit in hits:
            patent = hit['_source']
            pid = patent['patent_id']

            # Fetch US citations from us_citation_tmp if it exists
            if es.indices.exists(index='us_citation_tmp'):
                us_citation_response = es.search(
                    index='us_citation_tmp',
                    query={'match': {'patent_id': pid}},
                    size=100
                )
                if us_citation_response['hits']['total']['value'] > 0:
                    us_citation_hits = us_citation_response['hits']['hits']
                    us_citation_objects = []
                    for us_citation_hit in us_citation_hits:
                        us_citation_source = us_citation_hit['_source']
                        us_citation_objects.append({
                            "citation_sequence": us_citation_source.get('citation_sequence', 0),
                            "citation_document_number": us_citation_source.get('citation_document_number', ''),
                            "citation_date": us_citation_source.get('citation_date', ''),
                            "record_name": us_citation_source.get('record_name', ''),
                            "wipo_kind": us_citation_source.get('wipo_kind', ''),
                            "citation_category": us_citation_source.get('citation_category', '')
                        })
                    patent['us_citations'] = us_citation_objects

            # Fetch US application citations from us_app_citation_tmp if it exists
            if es.indices.exists(index='us_app_citation_tmp'):
                citations_response = es.search(
                    index='us_app_citation_tmp',
                    query={'match': {'patent_id': pid}},
                    size=100
                )
                if citations_response['hits']['total']['value'] > 0:
                    citations_hits = citations_response['hits']['hits']
                    citations_objects = []
                    for citation_hit in citations_hits:
                        citation_source = citation_hit['_source']
                        citations_objects.append({
                            "citation_sequence": citation_source.get('citation_sequence', 0),
                            "citation_document_number": citation_source.get('citation_document_number', ''),
                            "citation_date": citation_source.get('citation_date', ''),
                            "record_name": citation_source.get('record_name', ''),
                            "wipo_kind": citation_source.get('wipo_kind', ''),
                            "citation_category": citation_source.get('citation_category', '')
                        })
                    patent['us_app_citations'] = citations_objects

            # Fetch summary from summary_tmp if it exists
            if es.indices.exists(index='summary_tmp'):
                summary_response = es.search(
                    index='summary_tmp',
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
        print(f"ElasticSearch error: {e}")
    except Exception as e:
        print(f"Error during indexing: {e}")

if __name__ == "__main__":
    pparser = argparse.ArgumentParser()
    pparser.add_argument('--patent', type=str, help='Path to patent_data.csv')
    pparser.add_argument('--UScitation', type=str, help='Path to us_citation.csv')
    pparser.add_argument('--USappcitation', type=str, help='Path to g_us_application_citation.csv')
    pparser.add_argument('--classes', type=str, help='Path to patent_classes.csv')
    pparser.add_argument('--people', type=str, help='Path to patent_people.csv')
    pparser.add_argument('--summary', type=str, help='Path to patent_brief_sum.csv')
    pparser.add_argument('--claim', type=str, help='Path to patent_claims.csv')
    args = pparser.parse_args()
    
    try:
        records_indexed = 0

        # Process patent file first
        if args.patent:
            print(f"Processing patent file: {args.patent}")
            try:
                records_indexed = index_patent(args.patent)
            except Exception as e:
                print(f"ERROR in patent indexing: {e}")
                # Continue with other processing
        
        # Process US citations file
        if args.UScitation:
            print(f"Processing US citations file: {args.UScitation}")
            try:
                us_citation_records = index_us_citations(args.UScitation)
            except Exception as e:
                print(f"ERROR in US citations indexing: {e}")
        
        # Process US application citations file
        if args.USappcitation:
            print(f"Processing US application citations file: {args.USappcitation}")
            try:
                citation_records = index_us_app_citation(args.USappcitation)
            except Exception as e:
                print(f"ERROR in US application citations indexing: {e}")
        
        # Process patent classes immediately after patents
        if args.classes:
            print(f"Processing patent classes file: {args.classes}")
            try:
                classes_records = index_classes(args.classes)
            except Exception as e:
                print(f"ERROR in patent classes indexing: {e}")

        # Process people file
        if args.people:
            print(f"Processing people file: {args.people}")
            try:
                people_records = index_people(args.people)
            except Exception as e:
                print(f"ERROR in people indexing: {e}")

        # Process summary file
        if args.summary:
            print(f"Processing summary file: {args.summary}")
            try:
                summary_records = index_summary(args.summary)
            except Exception as e:
                print(f"ERROR in summary indexing: {e}")

        # Process claims file
        if args.claim:
            print(f"Processing claims file: {args.claim}")
            try:
                claim_records = index_claim(args.claim)
            except Exception as e:
                print(f"ERROR in claims indexing: {e}")

        # Ensure patentsview is indexed only if patents were processed
        if records_indexed > 0:
            print("Waiting for ElasticSearch to process documents...")
            time.sleep(2)
            try:
                index_patentsview_for_elasticsearch(args)
            except Exception as e:
                print(f"ERROR in patentsview indexing: {e}")
        else:
            # Check if patent_tmp already exists (from previous runs)
            es = elasticsearch.Elasticsearch(hosts=["http://localhost:9200"])
            if es.indices.exists(index='patent_tmp'):
                print("Patent index exists from previous run. Proceeding with patentsview indexing.")
                try:
                    index_patentsview_for_elasticsearch(args)
                except Exception as e:
                    print(f"ERROR in patentsview indexing: {e}")
            else:
                print("No patent records were indexed. Skipping patentsview indexing.")
    except Exception as e:
        print(f"CRITICAL ERROR: {e}")
        # Exit with error code
        import sys
        sys.exit(1)