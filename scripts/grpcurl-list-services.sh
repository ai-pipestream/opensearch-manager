#!/usr/bin/env bash
# List all gRPC services exposed by opensearch-manager.
# Usage: ./grpcurl-list-services.sh [host:port]
# Default: localhost:38103

set -e
HOST="${1:-localhost:38103}"
grpcurl -plaintext "$HOST" list
