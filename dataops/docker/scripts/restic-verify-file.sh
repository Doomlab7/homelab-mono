#!/bin/bash
# Restore a specific file from the latest snapshot for verification

# Move to the script's directory
cd "$(dirname "$0")/.."  # Now in dataops/docker directory

# Load environment variables from .env file if it exists
if [ -f .env ]; then
    # Export all variables from .env file
    export $(grep -v '^#' .env | xargs)
fi

# Get the file path from the argument or use default
FILE_PATH=${1:-"/source/path/to/file"}

# Create temp directory for restored files
mkdir -p /tmp/restic-verify

# Run the restic restore command for a specific file with explicit repository path
docker compose run --rm -v /tmp/restic-verify:/restore_target \
    --entrypoint restic backup restore latest \
    --target /restore_target --path "$FILE_PATH" \
    -r sftp:${SFTP_USER}@${SFTP_HOST}:${SFTP_PATH}

echo "File restored to /tmp/restic-verify for verification"
echo "Use 'ls -la /tmp/restic-verify' to see the restored file"
