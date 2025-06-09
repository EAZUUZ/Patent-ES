from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch
import json
import traceback
from transformers import pipeline

app = Flask(__name__)
CORS(app)

# Connect to Elasticsearch on host machine
es = Elasticsearch(['http://host.docker.internal:9200'])

# Initialize the text-to-text LLM pipeline
nlp = pipeline("text2text-generation", model="google/flan-t5-base")

@app.route('/api/health', methods=['GET'])
def health_check():
    """Simple endpoint to verify the API is working"""
    return jsonify({"status": "ok"})

@app.route('/api/es_check', methods=['GET'])
def es_check():
    """Check if Elasticsearch is reachable"""
    try:
        info = es.info()
        indices = es.indices.get_alias(index="*")
        return jsonify({
            "connection": "success",
            "elasticsearch_info": str(info),
            "indices": list(indices.keys())
        })
    except Exception as e:
        return jsonify({
            "connection": "failed",
            "error": str(e),
            "trace": traceback.format_exc()
        })

@app.route('/api/direct_query', methods=['POST'])
def direct_query():
    """For debugging - directly query ES with exact search term"""
    data = request.json
    search_term = data.get('term', 'hydrocarbon')
    
    try:
        query = {
            "query": {
                "match": {
                    "patent_abstract": search_term
                }
            },
            "size": 6000
        }
        
        response = es.search(index="patentsview", body=query)
        return jsonify({
            "success": True,
            "hit_count": len(response['hits']['hits']),
            "sample": response['hits']['hits'][0] if response['hits']['hits'] else {}
        })
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        })

@app.route('/api/query', methods=['POST'])
def query_patents():
    data = request.json
    user_query = data.get('query', '')
    
    print(f"Received query: {user_query}")
    
    # Extract search terms without using LLM for simplicity and reliability
    search_terms = extract_keywords(user_query)
    print(f"Extracted search terms: {search_terms}")
    
    # Build a simple query
    es_query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"patent_abstract": search_terms}},
                    {"match": {"patent_title": search_terms}},
                    {"match": {"summary": search_terms}}
                ]
            }
        },
        "size": 6000
    }
    
    print(f"Query being sent to ES: {es_query}")
    
    try:
        # Execute the query against Elasticsearch
        response = es.search(index="patentsview", body=es_query)
        print(f"Got {len(response['hits']['hits'])} results from ES")
        
        # Process results for visualization
        results = process_for_visualization(response)
        return jsonify(results)
        
    except Exception as e:
        print(f"Error querying Elasticsearch: {str(e)}")
        print(traceback.format_exc())
        return jsonify({
            "error": str(e),
            "message": "Failed to connect to Elasticsearch",
            "total_count": 0,
            "patents": [],
            "timeline": [],
            "cpc_sections": [],
            "inventors": []
        })

def extract_keywords(query):
    """Simple keyword extraction from natural language query"""
    # Remove common words like "get", "me", "all", "with", "the", "word"
    common_words = ["get", "me", "all", "with", "the", "word", "about", "containing", "related", "to"]
    words = query.lower().split()
    keywords = [word for word in words if word not in common_words]
    
    # Join remaining words back together
    return " ".join(keywords)

def process_for_visualization(response):
    hits = response['hits']['hits']
    
    # Extract patent data
    patents = []
    for hit in hits:
        source = hit['_source']
        
        # Extract inventors from nested people array
        inventors = []
        if 'people' in source:
            for person in source['people']:
                if person.get('inventor_full_name'):
                    inventors.append({
                        'name': person.get('inventor_full_name', ''),
                        'id': person.get('inventor_id', '')
                    })
        
        # Extract CPC classes - handle both direct and nested structures
        cpc_classes = []
        if 'cpc_classes' in source:
            if isinstance(source['cpc_classes'], list):
                for cpc in source['cpc_classes']:
                    if isinstance(cpc, dict):
                        if cpc.get('cpc_class'):
                            cpc_classes.append(cpc.get('cpc_class'))
                        if cpc.get('cpc_section'):
                            cpc_classes.append(cpc.get('cpc_section'))
                    elif isinstance(cpc, str):
                        cpc_classes.append(cpc)
            elif isinstance(source['cpc_classes'], str):
                cpc_classes.append(source['cpc_classes'])
        
        # Handle date field which could be a string or object
        patent_date = ''
        if 'patent_date' in source:
            if isinstance(source['patent_date'], dict) and 'from' in source['patent_date']:
                patent_date = source['patent_date']['from']
            elif isinstance(source['patent_date'], str):
                patent_date = source['patent_date']
        
        patents.append({
            'id': source.get('patent_id', ''),
            'title': source.get('patent_title', ''),
            'date': patent_date,
            'abstract': source.get('patent_abstract', ''),
            'cpc_classes': cpc_classes,
            'inventors': inventors,
            'num_claims': source.get('num_claims', 0)
        })
    
    # Prepare visualization data
    vis_data = {
        'total_count': len(patents),
        'patents': patents,
        'timeline': calculate_timeline(patents),
        'cpc_sections': calculate_cpc_sections(patents),
        'inventors': calculate_inventors(patents)
    }
    
    return vis_data

def calculate_timeline(patents):
    # Group patents by year
    timeline = {}
    for patent in patents:
        if 'date' in patent and patent['date']:
            # Extract year from date string (assuming format like '2015-01-01' or '2015')
            try:
                year = patent['date'][:4]  # Take first 4 characters as year
                if year.isdigit() and 1790 <= int(year) <= 2030:  # Sanity check for patent years
                    timeline[year] = timeline.get(year, 0) + 1
            except:
                pass  # Skip if date parsing fails
    
    # Sort by year
    sorted_timeline = [{'year': year, 'count': count} for year, count in sorted(timeline.items())]
    return sorted_timeline

def calculate_cpc_sections(patents):
    # Count CPC sections
    sections = {}
    for patent in patents:
        for cpc in patent.get('cpc_classes', []):
            if cpc and len(cpc) > 0:
                # Extract just the section letter (typically first character)
                section = cpc[0].upper()
                if section.isalpha():
                    sections[section] = sections.get(section, 0) + 1
    
    return [{'section': section, 'count': count} for section, count in sections.items()]

def calculate_inventors(patents):
    # Count inventions by inventor
    inventors = {}
    for patent in patents:
        for inventor in patent.get('inventors', []):
            if isinstance(inventor, dict) and 'name' in inventor:
                name = inventor['name']
                inventors[name] = inventors.get(name, 0) + 1
    
    # Sort by count and take top 20
    sorted_inventors = sorted(inventors.items(), key=lambda x: x[1], reverse=True)
    return [{'name': name, 'count': count} for name, count in sorted_inventors[:20]]

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
