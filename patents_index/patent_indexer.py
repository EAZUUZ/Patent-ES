import pandas as pd
import elasticsearch
from elasticsearch import Elasticsearch, helpers
import logging
from typing import Iterator, Dict
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class PatentIndexer:
    def __init__(
        self,
        hosts: list = ['http://localhost:9200'],
        index_name: str = 'patents',
        chunk_size: int = 10000,
        mapping: dict = None
    ):
        self.hosts = hosts
        self.index_name = index_name
        self.chunk_size = chunk_size
        self.mapping = mapping or self._default_mapping()
        self.es = None

    def _default_mapping(self) -> dict:
        """Define default mapping for patent index"""
        return {
            "mappings": {
                "properties": {
                    "patent_number": {"type": "keyword"},
                    "title": {"type": "text"},
                    "abstract": {"type": "text"},
                    "filing_date": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                    "grant_date": {"type": "date", "format": "yyyy-MM-dd||yyyy/MM/dd||epoch_millis"},
                    "inventors": {"type": "text"},
                    "assignee": {"type": "keyword"}
                }
            },
            "settings": {
                "index": {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
        }

    def connect_elasticsearch(self) -> None:
        """Create Elasticsearch connection"""
        try:
            self.es = Elasticsearch(self.hosts)
            if not self.es.ping():
                raise elasticsearch.ConnectionError("Could not connect to Elasticsearch")
            logger.info("Successfully connected to Elasticsearch")
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {e}")
            raise

    def create_index(self) -> None:
        """Create index with mapping if it doesn't exist"""
        try:
            if not self.es.indices.exists(index=self.index_name):
                self.es.indices.create(index=self.index_name, body=self.mapping)
                logger.info(f"Created index: {self.index_name}")
            else:
                logger.info(f"Index {self.index_name} already exists")
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            raise

    def process_csv_in_chunks(self, file_path: str) -> Iterator[Dict]:
        """Process large CSV file in chunks"""
        try:
            for chunk in pd.read_csv(
                file_path,
                chunksize=self.chunk_size,
                dtype=str,  # Prevent type inference for better memory usage
                na_filter=False  # Speed up processing by not checking for NA
            ):
                for record in chunk.to_dict('records'):
                    yield {
                        "_index": self.index_name,
                        "_source": record
                    }
        except Exception as e:
            logger.error(f"Error processing CSV file: {e}")
            raise

    def index_documents(self, file_path: str) -> None:
        """Index documents from CSV file"""
        try:
            total_indexed = 0
            start_time = datetime.now()

            # Process and index in chunks
            success, failed = helpers.bulk(
                self.es,
                self.process_csv_in_chunks(file_path),
                chunk_size=self.chunk_size,
                max_retries=3,
                raise_on_exception=False
            )

            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info(f"Indexing completed in {duration:.2f} seconds")
            logger.info(f"Successfully indexed {success} documents")
            if failed:
                logger.warning(f"Failed to index {len(failed)} documents")

        except Exception as e:
            logger.error(f"Error during indexing: {e}")
            raise

    def run(self, file_path: str) -> None:
        """Main execution flow"""
        try:
            self.connect_elasticsearch()
            self.create_index()
            self.index_documents(file_path)
        except Exception as e:
            logger.error(f"Application error: {e}")
            sys.exit(1)

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Index patent data into Elasticsearch")
    parser.add_argument("file_path", help="Path to the CSV file containing patent data")
    parser.add_argument("--host", default="http://localhost:9200", help="Elasticsearch host URL")
    parser.add_argument("--index", default="patents", help="Name of the Elasticsearch index")
    parser.add_argument("--chunk-size", type=int, default=10000, help="Size of chunks for processing")

    args = parser.parse_args()

    indexer = PatentIndexer(
        hosts=[args.host],
        index_name=args.index,
        chunk_size=args.chunk_size
    )

    indexer.run(args.file_path)