#!/bin/bash
# Compare two snapshots to see what changed

# Move to the script's directory
cd "$(dirname "$0")/.."  # Now in dataops/docker directory

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    # Export all variables from .env file
    export $(grep -v '^#' .env | xargs)
fi

# Get the snapshot IDs from arguments or use defaults
SNAPSHOT_ID1=${1:-"latest~1"}
SNAPSHOT_ID2=${2:-"latest"}

# Run the restic diff command with explicit repository path
docker compose run --rm --entrypoint restic backup diff "$SNAPSHOT_ID1" "$SNAPSHOT_ID2" -r sftp:${SFTP_USER}@${SFTP_HOST}:${SFTP_PATH}
