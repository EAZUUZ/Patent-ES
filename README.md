# Patents Elasticsearch Project

## Purpose
Set up Elasticsearch locally with all CSV files ingested for patent data analysis.

## Data Sources
- **patent_data**: Metadata including title and abstract
- **patent_people**: People and organization/company data  
- **patent_claims**: Claim sequence and claim data
- **patent_summary**: Brief summary of patents


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

## Complete Elasticsearch Mappings

```bash
curl localhost:9200/_mapping?pretty
```

```json
{
  "us_citations" : {
    "mappings" : {
      "properties" : {
        "citation_category" : {
          "type" : "keyword"
        },
        "citation_date" : {
          "type" : "date"
        },
        "citation_document_number" : {
          "type" : "keyword"
        },
        "citation_sequence" : {
          "type" : "integer"
        },
        "patent_id" : {
          "type" : "keyword"
        },
        "record_name" : {
          "type" : "text"
        },
        "wipo_kind" : {
          "type" : "keyword"
        }
      }
    }
  },
  "claim_tmp" : {
    "mappings" : {
      "properties" : {
        "claim_number" : {
          "type" : "integer"
        },
        "claim_sequence" : {
          "type" : "integer"
        },
        "claim_text" : {
          "type" : "text"
        },
        "dependent" : {
          "type" : "boolean"
        },
        "exemplary" : {
          "type" : "boolean"
        },
        "patent_id" : {
          "type" : "keyword"
        }
      }
    }
  },
  "patent_people_tmp" : {
    "mappings" : {
      "properties" : {
        "applicant_authority" : {
          "type" : "keyword"
        },
        "applicant_full_name" : {
          "type" : "text"
        },
        "applicant_organization" : {
          "type" : "text"
        },
        "assignee_full_name" : {
          "type" : "text"
        },
        "assignee_id" : {
          "type" : "keyword"
        },
        "assignee_organization" : {
          "type" : "text"
        },
        "gender_code" : {
          "type" : "keyword"
        },
        "inventor_full_name" : {
          "type" : "text"
        },
        "inventor_id" : {
          "type" : "keyword"
        },
        "patent_id" : {
          "type" : "keyword"
        }
      }
    }
  },
  "patent_summary_tmp" : {
    "mappings" : {
      "properties" : {
        "patent_id" : {
          "type" : "keyword"
        },
        "summary" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          },
          "analyzer" : "standard"
        }
      }
    }
  },
  "patentsview" : {
    "mappings" : {
      "properties" : {
        "claims" : {
          "type" : "nested",
          "properties" : {
            "claim_number" : {
              "type" : "integer"
            },
            "claim_sequence" : {
              "type" : "integer"
            },
            "claim_text" : {
              "type" : "text"
            },
            "dependent" : {
              "type" : "boolean"
            },
            "exemplary" : {
              "type" : "boolean"
            }
          }
        },
        "claims_text" : {
          "type" : "text"
        },
        "cpc_classes" : {
          "type" : "nested",
          "properties" : {
            "cpc_class" : {
              "type" : "keyword"
            },
            "cpc_class_title" : {
              "type" : "text"
            },
            "cpc_group" : {
              "type" : "keyword"
            },
            "cpc_group_title" : {
              "type" : "text"
            },
            "cpc_section" : {
              "type" : "keyword"
            },
            "cpc_subclass" : {
              "type" : "keyword"
            },
            "cpc_type" : {
              "type" : "keyword"
            }
          }
        },
        "num_claims" : {
          "type" : "integer"
        },
        "patent_abstract" : {
          "type" : "text"
        },
        "patent_date" : {
          "type" : "date"
        },
        "patent_id" : {
          "type" : "keyword"
        },
        "patent_title" : {
          "type" : "text"
        },
        "patent_type" : {
          "type" : "keyword"
        },
        "people" : {
          "type" : "nested",
          "properties" : {
            "applicant_authority" : {
              "type" : "keyword"
            },
            "applicant_full_name" : {
              "type" : "text"
            },
            "applicant_organization" : {
              "type" : "text"
            },
            "assignee_full_name" : {
              "type" : "text"
            },
            "assignee_id" : {
              "type" : "keyword"
            },
            "assignee_organization" : {
              "type" : "text"
            },
            "gender_code" : {
              "type" : "keyword"
            },
            "inventor_full_name" : {
              "type" : "text"
            },
            "inventor_id" : {
              "type" : "keyword"
            },
            "patent_id" : {
              "type" : "text",
              "fields" : {
                "keyword" : {
                  "type" : "keyword",
                  "ignore_above" : 256
                }
              }
            }
          }
        },
        "summary" : {
          "type" : "text"
        },
        "us_app_citations" : {
          "type" : "nested",
          "properties" : {
            "citation_category" : {
              "type" : "keyword"
            },
            "citation_date" : {
              "type" : "date"
            },
            "citation_document_number" : {
              "type" : "keyword"
            },
            "citation_sequence" : {
              "type" : "integer"
            },
            "record_name" : {
              "type" : "text"
            },
            "wipo_kind" : {
              "type" : "keyword"
            }
          }
        },
        "us_citations" : {
          "type" : "nested",
          "properties" : {
            "citation_category" : {
              "type" : "keyword"
            },
            "citation_date" : {
              "type" : "date"
            },
            "citation_document_number" : {
              "type" : "keyword"
            },
            "citation_sequence" : {
              "type" : "integer"
            },
            "record_name" : {
              "type" : "text"
            },
            "wipo_kind" : {
              "type" : "keyword"
            }
          }
        }
      }
    }
  },
  "patent_tmp_20250529_120045" : {
    "mappings" : {
      "properties" : {
        "num_claims" : {
          "type" : "integer"
        },
        "patent_abstract" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "patent_date" : {
          "type" : "date"
        },
        "patent_id" : {
          "type" : "keyword"
        },
        "patent_title" : {
          "type" : "text",
          "fields" : {
            "keyword" : {
              "type" : "keyword",
              "ignore_above" : 256
            }
          }
        },
        "patent_type" : {
          "type" : "keyword"
        }
      }
    }
  },
  "cpc_classes_tmp" : {
    "mappings" : {
      "properties" : {
        "cpc_class" : {
          "type" : "keyword"
        },
        "cpc_class_title" : {
          "type" : "text"
        },
        "cpc_group" : {
          "type" : "keyword"
        },
        "cpc_group_title" : {
          "type" : "text"
        },
        "cpc_section" : {
          "type" : "keyword"
        },
        "cpc_subclass" : {
          "type" : "keyword"
        },
        "cpc_type" : {
          "type" : "keyword"
        },
        "patent_id" : {
          "type" : "keyword"
        }
      }
    }
  },
  "us_app_citation_tmp" : {
    "mappings" : {
      "properties" : {
        "citation_category" : {
          "type" : "keyword"
        },
        "citation_date" : {
          "type" : "date"
        },
        "citation_document_number" : {
          "type" : "keyword"
        },
        "citation_sequence" : {
          "type" : "integer"
        },
        "patent_id" : {
          "type" : "keyword"
        },
        "record_name" : {
          "type" : "text"
        },
        "wipo_kind" : {
          "type" : "keyword"
        }
      }
    }
  }
}
```

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

## Repository Structure

```
Patent-ES/
├── README.md                    # Main documentation
├── patents_index/              # Index configuration files
├── patent-system/              # Complete patent analysis system
│   ├── backend/               # API backend service
│   ├── frontend/             # Web interface
│   ├── data/                 # Data processing scripts
│   ├── docker-compose.yml    # Docker configuration
│   └── README.md            # Patent system documentation
└── scripts/                   # Elasticsearch utilities
```

## Technical Details
- **Elasticsearch Version**: 8.17.2
- **Total Data Size**: ~85GB across all indices
- **Index Status**: All indices show "yellow" health (single node setup)

## Patent Analysis System
See `patent-system/README.md` for complete documentation of the web application and API.