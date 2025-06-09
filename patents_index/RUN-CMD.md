
    
### PLEASE MAKE SURE FIRST THAT ES IS INSTALLED AND RUNNING
### sudo systemctl start elasticsearch #to start ES
### sudo systemctl status elasticsearch #to check status of ES
### curl localhost:9200 #to check if we are properly connected to ES 

# Shell Script CMD for instantiating ES 
python3 ~/Desktop/NSF/Elasticsearch/patents_index/index_global.py     \
    --patent ~/Desktop/datasets/Patents/patent_data.csv  \
    --UScitation ~/Desktop/datasets/Patents/g_us_patent_citation.csv \
    --USappcitation ~/Desktop/datasets/Patents/g_us_application_citation.csv \
    --classes  ~/Desktop/datasets/Patents/patent_classes.csv   \
    --people ~/Desktop/datasets/Patents/patent_people.csv     \
    --summary ~/Desktop/datasets/Patents/patents_brief_sum.csv     \
    --claim ~/Desktop/datasets/Patents/patents_claims.csv

python3 ~/Desktop/NSF/Elasticsearch/patents_index/index_global.py \
 --patent ~/Desktop/datasets/Patents/patent_data.csv

# Below is curl command for getting data from ES
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
