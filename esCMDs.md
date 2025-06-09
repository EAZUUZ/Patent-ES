curl -X GET "http://localhost:9200/patentsview/_search" -H "Content-Type: application/json" -d '
{
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "patent_abstract": "technology"
          }
        }
      ]
    }
  },
  "_source": ["patent_id", "patent_title", "cpc_classes", "people"],
  "size": 10
}'


# Unique Patents for "technology"
curl -X GET "http://localhost:9200/patentsview/_search" -H "Content-Type: application/json" -d '
{
  "size": 0,
  "aggs": {
    "unique_patents": {
      "cardinality": {
        "field": "patent_id"
      }
    }
  }
}'

