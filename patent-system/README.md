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