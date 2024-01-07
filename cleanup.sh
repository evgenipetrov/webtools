#!/bin/bash

# Initialize Conda in this script
eval "$(conda shell.bash hook)"

# Deactivate the current Conda environment
conda deactivate

# Remove Conda environment 'webtools'
conda env remove --name webtools

# Stop and remove Docker containers starting with "screamingfrog"
docker ps -a --format "{{.Names}}" | grep ^screamingfrog | xargs -I {} docker stop {}
docker ps -a --format "{{.Names}}" | grep ^screamingfrog | xargs -I {} docker rm {}

# SQLite database cleanup
DB_PATH="./src/db.sqlite3"
if [ -f "$DB_PATH" ]; then
    rm "$DB_PATH"
    echo "SQLite database removed."
else
    echo "SQLite database file not found."
fi

# Delete Django migration files
find . -path "*/migrations/*.py" -not -name "__init__.py" -delete
find . -path "*/migrations/*.pyc" -delete

echo "Cleanup completed."
