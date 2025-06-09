# Check if Docker daemon is running
sudo systemctl status docker

# If it's not running, start it
sudo systemctl start docker

# Check Docker version
docker --version

# Test basic Docker functionality
docker ps

# Test if Docker works
docker run hello-world

### Restarting Docker
# Stop current containers
docker compose down

# Rebuild and restart
docker compose up --build
###

# Patent Analysis System

## Overview
Web application and API for searching and analyzing the patent database indexed in Elasticsearch.

## Components
- **Backend**: Flask/FastAPI service with search endpoints
- **Frontend**: React/Vue web interface
- **Data Processing**: Scripts for CSV ingestion and processing

## Quick Start
```bash
# Start all services
docker-compose up -d

# Access web interface
open http://localhost:3000

# API documentation
open http://localhost:8000/docs