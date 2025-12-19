#!/bin/bash

# OpenSearch Manager Startup Script
# Port: 38103 (OpenSearch Manager Service)
# This script helps start the opensearch manager service in development mode
# with proper environment variable detection and instance management.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$SCRIPT_DIR/.."

# ============================================================================
# Bootstrap Helper Scripts from GitHub (like gradlew)
# ============================================================================

DEV_ASSETS_REPO="https://raw.githubusercontent.com/ai-pipestream/dev-assets/main"
HELPERS_DIR="$PROJECT_ROOT/.dev-helpers"
DEV_ASSETS_LOCATION="${DEV_ASSETS_LOCATION:-$HELPERS_DIR}"

bootstrap_helpers() {
  # Check if DEV_ASSETS_LOCATION is explicitly set by user
  if [ -n "${DEV_ASSETS_LOCATION_OVERRIDE}" ] && [ -f "${DEV_ASSETS_LOCATION_OVERRIDE}/scripts/shared-utils.sh" ]; then
    DEV_ASSETS_LOCATION="${DEV_ASSETS_LOCATION_OVERRIDE}"
    echo "ℹ️  Using dev-assets from: $DEV_ASSETS_LOCATION"
    return 0
  fi

  # Check if already bootstrapped
  if [ -f "$HELPERS_DIR/scripts/shared-utils.sh" ]; then
    DEV_ASSETS_LOCATION="$HELPERS_DIR"
    return 0
  fi

  # Bootstrap from GitHub
  echo "🔄 Bootstrapping helper scripts from GitHub..."
  mkdir -p "$HELPERS_DIR/scripts"
  
  if ! curl -fsSL "$DEV_ASSETS_REPO/scripts/shared-utils.sh" -o "$HELPERS_DIR/scripts/shared-utils.sh"; then
    echo "❌ ERROR: Could not download helper scripts from GitHub"
    echo "   Please check your network connection and try again"
    exit 1
  fi
  
  chmod +x "$HELPERS_DIR/scripts/shared-utils.sh"
  DEV_ASSETS_LOCATION="$HELPERS_DIR"
  echo "✓ Helper scripts downloaded to $HELPERS_DIR"
}

# Bootstrap the helpers
bootstrap_helpers

# Source shared utilities
if [ -f "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh" ]; then
  source "$DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
else
  echo "❌ ERROR: Could not find shared-utils.sh at $DEV_ASSETS_LOCATION/scripts/shared-utils.sh"
  exit 1
fi

# Verify functions are available
if ! type check_dependencies >/dev/null 2>&1; then
  echo "❌ ERROR: Helper functions not loaded properly"
  exit 1
fi

# Service configuration
SERVICE_NAME="OpenSearch Manager"
SERVICE_PORT="38103"
DESCRIPTION="OpenSearch indexing and search management service"

# Check dependencies
check_dependencies "docker" "java"

# Validate we're in the correct directory
validate_project_structure "build.gradle" "src/main/resources/application.properties"

# Set environment variables
export QUARKUS_HTTP_PORT="$SERVICE_PORT"

# Set registration host using Docker bridge detection
set_registration_host "opensearch-manager" "OPENSEARCH_MANAGER_HOST"

print_status "header" "Starting $SERVICE_NAME"
print_status "info" "Port: $SERVICE_PORT"
print_status "info" "Description: $DESCRIPTION"
print_status "info" "Configuration:"
echo "  Service Host: $OPENSEARCH_MANAGER_HOST"
echo "  HTTP/gRPC Port: $QUARKUS_HTTP_PORT"
echo

# #region agent log - Port check instrumentation
echo '{"sessionId":"debug-opensearch","runId":"startup-check","hypothesisId":"A","location":"start-opensearch-manager.sh:93","message":"Checking if port is in use","data":{"port":"'$SERVICE_PORT'","service":"'$SERVICE_NAME'"},"timestamp":'$(date +%s%3N)'}' >> /home/krickert/IdeaProjects/.cursor/debug.log
# #endregion

# Check if already running and offer to kill
if check_port "$SERVICE_PORT" "$SERVICE_NAME"; then
    # #region agent log - Port conflict detected
    echo '{"sessionId":"debug-opensearch","runId":"startup-check","hypothesisId":"A","location":"start-opensearch-manager.sh:95","message":"Port conflict detected","data":{"port":"'$SERVICE_PORT'","service":"'$SERVICE_NAME'"},"timestamp":'$(date +%s%3N)'}' >> /home/krickert/IdeaProjects/.cursor/debug.log
    # #endregion
    print_status "warning" "$SERVICE_NAME is already running on port $SERVICE_PORT."
    read -p "Would you like to kill the existing process and restart? (y/N) " -r response
    if [[ "$response" =~ ^[Yy]$ ]]; then
        kill_process_on_port "$SERVICE_PORT" "$SERVICE_NAME"
    else
        print_status "info" "Cancelled by user."
        exit 0
    fi
else
    # #region agent log - Port is free
    echo '{"sessionId":"debug-opensearch","runId":"startup-check","hypothesisId":"A","location":"start-opensearch-manager.sh:98","message":"Port is free","data":{"port":"'$SERVICE_PORT'"},"timestamp":'$(date +%s%3N)'}' >> /home/krickert/IdeaProjects/.cursor/debug.log
    # #endregion
fi

print_status "info" "Starting $SERVICE_NAME in Quarkus dev mode..."
print_status "info" "DevServices will automatically start: MySQL, Kafka, Consul, OpenSearch, etc."
print_status "info" "Press Ctrl+C to stop"
echo

# #region agent log - Compose file check
COMPOSE_FILE="$HOME/.pipeline/compose-devservices.yml"
if [ -f "$COMPOSE_FILE" ]; then
    echo '{"sessionId":"debug-opensearch","runId":"compose-check","hypothesisId":"B","location":"start-opensearch-manager.sh:110","message":"Compose file exists","data":{"file":"'$COMPOSE_FILE'"},"timestamp":'$(date +%s%3N)'}' >> /home/krickert/IdeaProjects/.cursor/debug.log
else
    echo '{"sessionId":"debug-opensearch","runId":"compose-check","hypothesisId":"B","location":"start-opensearch-manager.sh:110","message":"Compose file missing","data":{"file":"'$COMPOSE_FILE'"},"timestamp":'$(date +%s%3N)'}' >> /home/krickert/IdeaProjects/.cursor/debug.log
fi
# #endregion

# Start using the app's own gradlew with the detected registration host and compose file path
# #region agent log - Starting gradlew
echo '{"sessionId":"debug-opensearch","runId":"gradle-start","hypothesisId":"C","location":"start-opensearch-manager.sh:115","message":"Starting gradlew quarkusDev","data":{"composeFile":"'$COMPOSE_FILE'","registrationHost":"'$OPENSEARCH_MANAGER_HOST'"},"timestamp":'$(date +%s%3N)'}' >> /home/krickert/IdeaProjects/.cursor/debug.log
# #endregion
./gradlew quarkusDev -Dservice.registration.host=$OPENSEARCH_MANAGER_HOST -Dquarkus.compose.devservices.files=$HOME/.pipeline/compose-devservices.yml
