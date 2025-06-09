# Patents Elasticsearch Project

## Purpose
Set up Elasticsearch locally with all CSV files ingested for patent data analysis.

## Data Sources
- **patent_data**: Metadata including title and abstract
- **patent_people**: People and organization/company data  
- **patent_claims**: Claim sequence and claim data
- **patent_summary**: Brief summary of patents
- **patent_citation**STUFFF**: US Patent Citations 

## Elasticsearch Indices

| Index | Documents | Size | Description |
|-------|-----------|------|-------------|
| `patentsview` | 94,623,460 | 31.1GB | Main patent data with nested fields |
| `claim_tmp` | 130,566,650 | 39GB | Patent claims data |
| `us_app_citation_tmp` | 72,260,158 | 6.3GB | US application citations |
| `patent_people_tmp` | 22,492,856 | 2.8GB | Patent inventors and assignees |
| `cpc_classes_tmp` | 5,379,780 | 726.8MB | CPC classification data |
| `patent_tmp_20250529_120045` | 8,980,130 | 5.6GB | Temporary patent data |

## Sample Query

Search for agriculture-related patents:

```bash
curl -X GET "http://localhost:9200/patentsview/_search?pretty" \
  -H 'Content-Type: application/json' \
  -d'{
    "size": 5,
    "query": {
      "multi_match": {
        "query": "agriculture",
        "fields": ["patent_title", "patent_abstract", "claims_text"]
      }
    },
    "_source": [
      "patent_id",
      "patent_title", 
      "people.inventor_full_name",
      "people.assignee_organization",
      "cpc_classes.cpc_class",
      "cpc_classes.cpc_class_title"
    ]
  }'
```

## Key Fields Available for Querying

### Main Patent Data (`patentsview` index)
- `patent_id` (keyword)
- `patent_title` (text)
- `patent_abstract` (text)
- `patent_date` (date)
- `patent_type` (keyword)
- `num_claims` (integer)
- `claims_text` (text)
- `summary` (text)

### Nested Objects
- **`people`**: Inventors, assignees, applicants
  - `inventor_full_name`, `assignee_organization`, `gender_code`
- **`cpc_classes`**: Patent classifications
  - `cpc_class`, `cpc_class_title`, `cpc_section`, `cpc_group`
- **`claims`**: Individual patent claims
  - `claim_text`, `claim_number`, `dependent`, `exemplary`
- **`us_citations`** & **`us_app_citations`**: Citation data
  - `citation_document_number`, `citation_category`, `citation_date`

## Setup Instructions

1. **Install Elasticsearch 8.17.2**
2. **Start Elasticsearch service**
3. **Verify installation**: `curl localhost:9200`
4. **Check indices**: `curl localhost:9200/_cat/indices?v`
5. **View mappings**: `curl localhost:9200/_mapping?pretty`

## Usage Examples

```bash
# Get all indices
curl localhost:9200/_cat/indices?v

# Search specific index
curl localhost:9200/patentsview/_search?pretty

# Get field mappings
curl localhost:9200/patentsview/_mapping?pretty
```

## Technical Details
- **Elasticsearch Version**: 8.17.2
- **Cluster Name**: elasticsearch
- **Node Name**: tji-alienware
- **Total Data Size**: ~85GB across all indices
- **Index Status**: All indices show "yellow" health (single node setup)