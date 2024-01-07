#!/bin/bash

# Define your conda environment name
ENV_NAME=webtools
SEO_SPIDER_VERSION=19.4

# Define Docker image name, tag, and relative Dockerfile path
DOCKER_IMAGE_NAME=screamingfrogseospider
DOCKER_IMAGE_TAG=latest
DOCKERFILE_RELATIVE_PATH=./docker/screamingfrogseospider.dockerfile

# Check if the conda environment exists
if conda env list | grep -q "^${ENV_NAME}"; then
    echo "Updating conda environment: ${ENV_NAME}"
    conda env update -n ${ENV_NAME} -f environment.yml
else
    echo "Creating conda environment: ${ENV_NAME}"
    conda env create -f environment.yml
fi

# Build the Docker image with no cache, passing build args
docker build . --no-cache -f ${DOCKERFILE_RELATIVE_PATH} --build-arg USER_NAME=${USER} --build-arg SEO_SPIDER_VERSION=${SEO_SPIDER_VERSION} -t ${DOCKER_IMAGE_NAME}:${DOCKER_IMAGE_TAG}

# Activate conda environment
eval "$(conda shell.bash hook)"
conda activate ${ENV_NAME}

# Django makemigrations and migrate
python src/manage.py makemigrations
python src/manage.py migrate

echo "Setup completed."
