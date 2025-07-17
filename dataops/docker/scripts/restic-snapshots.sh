#!/bin/bash
# List all restic snapshots

# Move to the script's directory
cd "$(dirname "$0")/.."  # Now in dataops/docker directory

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    # Export all variables from .env file
    export $(grep -v '^#' .env | xargs)
fi

# Run the restic snapshots command with explicit repository path
docker compose run --rm --entrypoint restic backup snapshots -r sftp:${SFTP_USER}@${SFTP_HOST}:${SFTP_PATH}
