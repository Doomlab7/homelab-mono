#!/bin/bash

# Script to monitor sermon directory using gotify watch
# Used with a systemd service or standalone
# Can run in local or remote (SSH) mode

# Configuration

# Target directory to monitor
DIR_PATH="/tank/encrypted/docker/nextcloud-zfs/nextcloud/data/__groupfolders/1/Sermons/Raw Upload"

# Mode: Set to "ssh" or "local"
# If ssh, will connect to SSH_HOST as SSH_USER to monitor the directory
# If local, will monitor the directory directly on this machine
MODE="ssh"

# Remote SSH settings (only used if MODE="ssh")
SSH_USER="nic"
SSH_HOST="ghost"

# Log settings
LOG_DIR="$HOME/.local/state/sermon-monitor"
LOG_FILE="$LOG_DIR/sermon-monitor.log"
MAX_LOG_SIZE_KB=1024 # 1MB max log size

# Create log directory if it doesn't exist
mkdir -p "$LOG_DIR" 2>/dev/null

# Create or truncate log file if it's too large
if [ -f "$LOG_FILE" ] && [ $(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0) -gt $((MAX_LOG_SIZE_KB * 1024)) ]; then
  # Keep the last 20 lines when rotating
  tail -n 20 "$LOG_FILE" >"${LOG_FILE}.tmp" 2>/dev/null && mv "${LOG_FILE}.tmp" "$LOG_FILE" 2>/dev/null
  echo "$(date '+%Y-%m-%d %H:%M:%S') - Log file rotated due to size limit" >>"$LOG_FILE"
fi

# Ensure log file exists
touch "$LOG_FILE" 2>/dev/null

# Log function
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Create a temp file to store the previous state
TEMP_DIR="/tmp/sermon-monitor"
PREV_STATE_FILE="$TEMP_DIR/previous_state.txt"
mkdir -p "$TEMP_DIR" 2>/dev/null || true

# Function to get directory listing based on mode
get_directory_listing() {
  if [ "$MODE" = "ssh" ]; then
    # Remote mode - use SSH
    ssh $SSH_USER@$SSH_HOST "ls -la '$DIR_PATH' | grep -v '^\.\.$' | grep -v '^\.$'" ||
      echo "Error: Failed to connect to $SSH_HOST"
  else
    # Local mode - direct access
    if [ -d "$DIR_PATH" ]; then
      ls -la "$DIR_PATH" | grep -v '^\.\.$' | grep -v '^\.$'
    else
      echo "Error: Directory $DIR_PATH does not exist locally"
    fi
  fi
}

# Removed unused function - was leftover from previous version

# Display startup message based on mode
if [ "$MODE" = "ssh" ]; then
  log "Starting sermon file monitor for $SSH_USER@$SSH_HOST:$DIR_PATH (SSH mode)"
else
  log "Starting sermon file monitor for $DIR_PATH (local mode)"
fi

# Run initial check to establish baseline
CURRENT_LISTING=$(get_directory_listing)
echo "$CURRENT_LISTING" >"$PREV_STATE_FILE"
log "Initial state captured. Watching for changes."

# Monitor continuously
while true; do
  # Sleep for 30 seconds between checks
  sleep 30

  # Get current listing
  CURRENT_LISTING=$(get_directory_listing)

  # Find new files by comparing with previous state
  NEW_FILES=$(diff --new-line-format="%L" --old-line-format="" --unchanged-line-format="" \
    "$PREV_STATE_FILE" <(echo "$CURRENT_LISTING") | grep -v "^total" | grep -v "^drwx")

  # If new files found, notify
  if [ -n "$NEW_FILES" ]; then
    # Filter out directories and system entries, keep only regular files
    FILE_ENTRIES=$(echo "$NEW_FILES" | grep -v "^d" | grep -v "^total" | grep -v "^\.$" | grep -v "^\.\.")

    # Count how many actual files (non-empty lines)
    NEW_COUNT=$(echo "$FILE_ENTRIES" | grep -v "^$" | wc -l)

    # Only proceed if we have actual files
    if [ "$NEW_COUNT" -gt 0 ]; then
      # Create notification message
      if [ "$NEW_COUNT" -eq 1 ]; then
        TITLE="New Sermon File Added"
        # Extract filename from listing line
        FILENAME=$(echo "$FILE_ENTRIES" | awk '{print $NF}')
        MESSAGE="New sermon file detected: $FILENAME"
      else
        TITLE="New Sermon Files Added"
        # Format filenames only for readability
        FILELIST=$(echo "$FILE_ENTRIES" | awk '{print $NF}' | sort)
        MESSAGE="$NEW_COUNT new sermon files detected:\n$FILELIST"
      fi

      # Send notification
      gotify push --title "$TITLE" "$MESSAGE"
      log "Notification sent for new files: $NEW_COUNT"
    fi

    # Update previous state
    echo "$CURRENT_LISTING" >"$PREV_STATE_FILE"
  fi
done

# If we get here, gotify watch has exited
log "gotify watch exited unexpectedly"
exit 1
