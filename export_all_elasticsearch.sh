#!/bin/bash

# Complete Elasticsearch Export Script
# This will export ALL indices including mappings and data

EXPORT_DIR="$HOME/elasticsearch_complete_export"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
FINAL_DIR="${EXPORT_DIR}_${TIMESTAMP}"

echo "=== Elasticsearch Complete Export ==="
echo "Export directory: $FINAL_DIR"
echo "Starting export at: $(date)"

# Create export directory
mkdir -p "$FINAL_DIR"

# Get all indices (excluding system indices starting with .)
echo "Getting list of indices..."
indices=$(curl -s "localhost:9200/_cat/indices?h=index" | grep -v "^\.")

echo "Found indices:"
echo "$indices"
echo ""

# Export cluster info
echo "Exporting cluster information..."
curl -s "localhost:9200/" > "$FINAL_DIR/cluster_info.json"
curl -s "localhost:9200/_cat/indices?v" > "$FINAL_DIR/indices_list.txt"
curl -s "localhost:9200/_mapping?pretty" > "$FINAL_DIR/all_mappings.json"

# Function to export an index completely
export_index() {
    local index_name=$1
    local index_dir="$FINAL_DIR/$index_name"
    
    echo "Processing index: $index_name"
    mkdir -p "$index_dir"
    
    # Get index info
    curl -s "localhost:9200/$index_name" > "$index_dir/${index_name}_info.json"
    
    # Get mapping
    curl -s "localhost:9200/$index_name/_mapping?pretty" > "$index_dir/${index_name}_mapping.json"
    
    # Get settings
    curl -s "localhost:9200/$index_name/_settings?pretty" > "$index_dir/${index_name}_settings.json"
    
    # Get document count
    doc_count=$(curl -s "localhost:9200/$index_name/_count" | jq -r '.count')
    echo "  Document count: $doc_count"
    echo "$doc_count" > "$index_dir/${index_name}_count.txt"
    
    # Export data using scroll API for large datasets
    echo "  Exporting data..."
    
    # Initial search with scroll
    scroll_id=$(curl -s -X GET "localhost:9200/$index_name/_search?scroll=10m&size=1000" \
        -H 'Content-Type: application/json' \
        -d '{"query":{"match_all":{}}}' | \
        tee "$index_dir/${index_name}_data_001.json" | \
        jq -r '._scroll_id')
    
    # Continue scrolling through all data
    batch_num=2
    while true; do
        batch_file="$index_dir/${index_name}_data_$(printf "%03d" $batch_num).json"
        
        response=$(curl -s -X GET "localhost:9200/_search/scroll" \
            -H 'Content-Type: application/json' \
            -d "{\"scroll\":\"10m\",\"scroll_id\":\"$scroll_id\"}")
        
        echo "$response" > "$batch_file"
        
        # Check if we got any hits
        hits=$(echo "$response" | jq -r '.hits.hits | length')
        if [ "$hits" -eq 0 ]; then
            rm "$batch_file"  # Remove empty file
            break
        fi
        
        # Update scroll_id for next iteration
        scroll_id=$(echo "$response" | jq -r '._scroll_id')
        batch_num=$((batch_num + 1))
        
        echo "    Exported batch $((batch_num-1))"
    done
    
    # Clear the scroll
    curl -s -X DELETE "localhost:9200/_search/scroll" \
        -H 'Content-Type: application/json' \
        -d "{\"scroll_id\":\"$scroll_id\"}" > /dev/null
    
    echo "  Completed: $index_name"
    echo ""
}

# Export each index
for index in $indices; do
    export_index "$index"
done

# Create a restore script
echo "Creating restore script..."
cat > "$FINAL_DIR/restore_indices.sh" << 'EOF'
#!/bin/bash

# Restore script for Elasticsearch indices
# Usage: ./restore_indices.sh [elasticsearch_host]

ES_HOST=${1:-"localhost:9200"}
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Restoring to Elasticsearch at: $ES_HOST"

for index_dir in */; do
    if [ -d "$index_dir" ]; then
        index_name=$(basename "$index_dir")
        echo "Restoring index: $index_name"
        
        # Create index with mapping and settings
        if [ -f "$index_dir/${index_name}_mapping.json" ] && [ -f "$index_dir/${index_name}_settings.json" ]; then
            # Extract just the mappings and settings
            mappings=$(cat "$index_dir/${index_name}_mapping.json" | jq ".[\"$index_name\"].mappings")
            settings=$(cat "$index_dir/${index_name}_settings.json" | jq ".[\"$index_name\"].settings")
            
            # Create index
            curl -X PUT "$ES_HOST/$index_name" -H 'Content-Type: application/json' -d "{
                \"mappings\": $mappings,
                \"settings\": $settings
            }"
        fi
        
        # Bulk import data files
        for data_file in "$index_dir"/${index_name}_data_*.json; do
            if [ -f "$data_file" ]; then
                echo "  Importing $(basename "$data_file")"
                # Convert search results to bulk format and import
                cat "$data_file" | jq -r '.hits.hits[] | "{\"index\":{\"_index\":\"'$index_name'\",\"_id\":\"" + ._id + "\"}}\n" + (._source | tostring)' | curl -X POST "$ES_HOST/_bulk" -H 'Content-Type: application/x-ndjson' --data-binary @-
            fi
        done
        
        echo "Completed: $index_name"
    fi
done

echo "Restore completed!"
EOF

chmod +x "$FINAL_DIR/restore_indices.sh"

# Create summary report
echo "Creating export summary..."
cat > "$FINAL_DIR/export_summary.txt" << EOF
Elasticsearch Export Summary
===========================
Export Date: $(date)
Export Directory: $FINAL_DIR

Indices Exported:
$(echo "$indices" | sed 's/^/- /')

Total Size: $(du -sh "$FINAL_DIR" | cut -f1)

Files Structure:
- cluster_info.json: Basic cluster information
- indices_list.txt: List of all indices
- all_mappings.json: Complete mappings for all indices
- restore_indices.sh: Script to restore all indices
- [index_name]/: Directory for each index containing:
  - [index]_info.json: Index information
  - [index]_mapping.json: Index mapping
  - [index]_settings.json: Index settings
  - [index]_count.txt: Document count
  - [index]_data_*.json: Data files (batched)

To restore:
1. Extract this archive
2. Run: ./restore_indices.sh [elasticsearch_host]
EOF

echo ""
echo "=== Export Complete ==="
echo "Export directory: $FINAL_DIR"
echo "Total size: $(du -sh "$FINAL_DIR" | cut -f1)"
echo "Completed at: $(date)"

# Create compressed archive
echo ""
echo "Creating compressed archive..."
cd "$(dirname "$FINAL_DIR")"
tar -czf "${FINAL_DIR}.tar.gz" "$(basename "$FINAL_DIR")"
echo "Archive created: ${FINAL_DIR}.tar.gz"
echo "Archive size: $(du -sh "${FINAL_DIR}.tar.gz" | cut -f1)"

echo ""
echo "Export Summary:"
echo "- Raw export: $FINAL_DIR"
echo "- Compressed: ${FINAL_DIR}.tar.gz"
echo "- Includes restore script for easy import"