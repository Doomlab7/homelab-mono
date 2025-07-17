# Restic read-only operations with Docker

# Set default working directory for restic commands
DOCKER_DIR := "dataops/docker"
SCRIPTS_DIR := DOCKER_DIR + "/scripts"

# ==========================================================================
# Read-Only Operations
# ==========================================================================

# List all snapshots in the repository
restic-snapshots:
    @echo "Listing snapshots..."
    {{SCRIPTS_DIR}}/restic-snapshots.sh

# Check repository integrity
restic-check:
    @echo "Checking repository integrity..."
    {{SCRIPTS_DIR}}/restic-check.sh

# List files in a snapshot (defaults to latest)
restic-ls snapshot_id="latest":
    @echo "Listing files in snapshot {{snapshot_id}}..."
    {{SCRIPTS_DIR}}/restic-ls.sh {{snapshot_id}}

# Compare two snapshots to see what changed
restic-diff snapshot_id1="latest~1" snapshot_id2="latest":
    @echo "Comparing snapshots {{snapshot_id1}} and {{snapshot_id2}}..."
    {{SCRIPTS_DIR}}/restic-diff.sh {{snapshot_id1}} {{snapshot_id2}}

# Show a summary of repository stats
restic-stats:
    @echo "Showing repository stats..."
    {{SCRIPTS_DIR}}/restic-stats.sh

# ==========================================================================
# File Recovery (for spot checking)
# ==========================================================================

# Restore a specific file from the latest snapshot for verification
restic-verify-file file_path="/source/path/to/file":
    @echo "Restoring {{file_path}} to /tmp/restic-verify for spot checking..."
    {{SCRIPTS_DIR}}/restic-verify-file.sh "{{file_path}}"

