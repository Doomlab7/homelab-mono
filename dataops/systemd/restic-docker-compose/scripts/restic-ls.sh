#!/bin/bash
# List files in a snapshot

# Move to the script's directory
cd "$(dirname "$0")/.."  # Now in dataops/docker directory

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    # Export all variables from .env file
    export $(grep -v '^#' .env | xargs)
fi

# Get the snapshot ID from the argument or default to "latest"
SNAPSHOT_ID=${1:-latest}

# Run the restic ls command with explicit repository path
docker compose run --rm --entrypoint restic backup ls "$SNAPSHOT_ID" -r sftp:${SFTP_USER}@${SFTP_HOST}:${SFTP_PATH}
