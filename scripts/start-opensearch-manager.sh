#!/bin/bash

# OpenSearch Manager Startup Script
# Port: 38103 (OpenSearch Manager Service)
# This script helps start the opensearch manager service in development mode
# with proper environment variable detection and instance management.

set -e

# Configuration - Update this path to your dev-assets checkout location
DEV_ASSETS_LOCATION="${DEV_ASSETS_LOCATION:-/home/krickert/IdeaProjects/gitea/dev-assets}"

# Source shared utilities from dev-assets
source "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh"

# Check dependencies
check_dependencies "docker" "java"

# Service configuration
SERVICE_NAME="OpenSearch Manager"
SERVICE_PORT="38103"
DESCRIPTION="OpenSearch indexing and search management service"

# Validate we're in the correct directory
validate_project_structure "build.gradle" "src/main/resources/application.properties"

# Set environment variables
export QUARKUS_HTTP_PORT="$SERVICE_PORT"

# Set registration host using Docker bridge detection
set_registration_host "opensearch-manager" "OPENSEARCH_MANAGER_HOST"

# Set additional environment variables if needed
# (OpenSearch manager uses shared infrastructure, so minimal additional config)

print_status "header" "Starting $SERVICE_NAME"
print_status "info" "Port: $SERVICE_PORT"
print_status "info" "Description: $DESCRIPTION"
print_status "info" "Dev Assets Location: $DEV_ASSETS_LOCATION"
print_status "info" "Configuration:"
echo "  Service Host: $OPENSEARCH_MANAGER_HOST"
echo "  HTTP/gRPC Port: $QUARKUS_HTTP_PORT"
echo

# Check if already running and offer to kill
if check_port "$SERVICE_PORT" "$SERVICE_NAME"; then
    print_status "warning" "$SERVICE_NAME is already running on port $SERVICE_PORT."
    read -p "Would you like to kill the existing process and restart? (y/N) " -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        kill_process_on_port "$SERVICE_PORT" "$SERVICE_NAME"
    else
        print_status "info" "Cancelled by user."
        exit 0
    fi
fi

print_status "info" "Starting $SERVICE_NAME in Quarkus dev mode..."
print_status "info" "DevServices will automatically start: MySQL, Kafka, Consul, OpenSearch, etc."
print_status "info" "Press Ctrl+C to stop"
echo

# Start using the app's own gradlew with the detected registration host and compose file path
./gradlew quarkusDev -Dservice.registration.host=$OPENSEARCH_MANAGER_HOST -Dquarkus.compose.devservices.files=$HOME/.pipeline/compose-devservices.yml