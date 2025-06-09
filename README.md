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


