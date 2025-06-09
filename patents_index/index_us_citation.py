import pandas as pd
from elasticsearch import Elasticsearch, helpers

def index_us_citations(citation_file_path):
    """
    Indexes US patent citations from a CSV file into an Elasticsearch index.

    This function reads a CSV file containing US patent citations, processes the data in manageable chunks,
    and indexes it into an Elasticsearch cluster. It ensures efficient handling of large files and robust error management.

    Args:
        citation_file_path (str): The file path to the CSV containing the citation data.

    Returns:
        int: The total number of records successfully indexed.
    """
    index_name = 'us_citations'

    # Establish connection to Elasticsearch
    try:
        es = Elasticsearch(hosts=["http://localhost:9200"])
        if not es.ping():
            print("⚠️ Elasticsearch connection failed. Ensure the server is running.")
            return 0
    except Exception as e:
        print(f"⚠️ Error connecting to Elasticsearch: {e}")
        return 0

    # Ensure a fresh index by deleting any existing one
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
    except Exception as e:
        print(f"⚠️ Error deleting existing index: {e}")

    # Define the index mapping
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

    # Create the index with the specified mapping
    try:
        es.indices.create(index=index_name, body=mapping)
    except Exception as e:
        print(f"⚠️ Error creating index: {e}")
        return 0

    # Define the chunk size for processing large files
    chunk_size = 50000
    total_indexed = 0

    try:
        # Read and process the CSV file in chunks
        for chunk in pd.read_csv(citation_file_path, dtype=str, chunksize=chunk_size):
            if chunk.empty:
                print("⚠️ Skipping empty chunk.")
                continue

            # Standardize column names by stripping any leading/trailing whitespace
            chunk.columns = chunk.columns.str.strip()

            # Define the required columns
            required_columns = {
                "patent_id",
                "US_citation_citation_sequence",
                "US_citation_citation_document_number",
                "US_citation_citation_date",
                "US_citation_record_name",
                "US_citation_wipo_kind",
                "US_citation_citation_category"
            }

            # Check for missing columns
            missing_columns = required_columns - set(chunk.columns)
            if missing_columns:
                print(f"⚠️ Missing required columns: {missing_columns}. Proceeding with available data.")

            # Remove duplicates based on 'patent_id' and 'US_citation_citation_document_number'
            chunk.drop_duplicates(subset=['patent_id', 'US_citation_citation_document_number'], inplace=True)

            # Prepare the data for bulk indexing
            actions = []
            for _, row in chunk.iterrows():
                try:
                    # Convert the citation date to the 'YYYY-MM-DD' format
                    citation_date = pd.to_datetime(row.get('US_citation_citation_date', ''), errors='coerce')
                    citation_date = citation_date.strftime('%Y-%m-%d') if pd.notna(citation_date) else None

                    # Construct the document for Elasticsearch
                    document = {
                        "_index": index_name,
                        "_source": {
                            "patent_id": row.get('patent_id', '').strip() if pd.notna(row.get('patent_id')) else '',
                            "citation_sequence": int(row.get('US_citation_citation_sequence', 0)) if str(row.get('US_citation_citation_sequence', '0')).isdigit() else None,
                            "citation_document_number": row.get('US_citation_citation_document_number', '').strip() if pd.notna(row.get('US_citation_citation_document_number')) else '',
                            "citation_date": citation_date,
                            "record_name": row.get('US_citation_record_name', '').strip() if pd.notna(row.get('US_citation_record_name')) else '',
                            "wipo_kind": row.get('US_citation_wipo_kind', '').strip() if pd.notna(row.get('US_citation_wipo_kind')) else '',
                            "citation_category": row.get('US_citation_citation_category', '').strip() if pd.notna(row.get('US_citation_citation_category')) else ''
                        }
                    }
                    actions.append(document)
                except Exception as e:
                    print(f"⚠️ Error processing row: {e}")
                    continue

            # Perform bulk indexing
            if actions:
                try:
                    success, _ = helpers.bulk(es, actions)
                    total_indexed += success
                except Exception as e:
                    print(f"⚠️ Error in bulk indexing: {e}")

    except Exception as e:
        print(f"⚠️ Error reading CSV file: {e}")
        return total_indexed

    # Refresh the index to make the documents searchable
    try:
        es.indices.refresh(index=index_name)
        # Verify the number of documents indexed
        doc_count = es.count(index=index_name)['count']
        print(f"✅ Total documents indexed: {doc_count}")
    except Exception as e:
        print(f"⚠️ Error refreshing or counting index: {e}")

    return total_indexed
