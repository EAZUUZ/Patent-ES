# Purpose of the elasticsearch folder is to set up ES locally with all the csv files ingested

<h4>
patent_data (meta data incl title and abstract)
patent_people (people and org/company data)
patent_claims (claim sequence, claim data)
patent_summary (brief summary of patents)

**MISSING Citation Data**
</h4>

</h1 title="CMD">
curl -X GET "http://localhost:9200/patentsview/_search?pretty" -H 'Content-Type: application/json' -d'
{
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
</h1>

### MAPPING ###
eadlzarabi@tji-alienware:~/Desktop/NSF/Elasticsearch$ curl localhost:9200/_cat/indices?v
health status index                      uuid                   pri rep docs.count docs.deleted store.size pri.store.size dataset.size
yellow open   us_app_citation_tmp        wm0eu8E-ROaOvtOcMUWLGQ   1   1   72260158            0      6.3gb          6.3gb        6.3gb
yellow open   cpc_classes_tmp            yzHDlDEySkSaZ4_osDawGQ   1   1    5379780            0    726.8mb        726.8mb      726.8mb
yellow open   us_citations               8ZnjzllJQ3e6v04Aymrrag   1   1          0            0       249b           249b         249b
yellow open   patentsview                groMn28DQIGEe_pnPhvGVg   1   1   94623460            0     31.1gb         31.1gb       31.1gb
yellow open   claim_tmp                  qViSN5aISjGXsNrV0lmq1w   1   1  130566650            0       39gb           39gb         39gb
yellow open   patent_summary_tmp         4Hn6ia1kRr2wjDRBEXbueQ   1   1          0            0       249b           249b         249b
yellow open   patent_people_tmp          QNQFKNiWRXmLAHBJPFVSoA   1   1   22492856            0      2.8gb          2.8gb        2.8gb
yellow open   patent_tmp_20250529_120045 tzHr5OycQfyjlROWOS5jVw   1   1    8980130            0      5.6gb          5.6gb        5.6gb
eadlzarabi@tji-alienware:~/Desktop/NSF/Elasticsearch$ curl localhost:9200/_mapping?pretty
{
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
  }
}
eadlzarabi@tji-alienware:~/Desktop/NSF/Elasticsearch$ 




