#!/bin/bash

# Script to monitor sermon directory using gotify watch
# Used with a systemd service

# Directory to monitor
WATCH_DIR="/tank/encrypted/docker/nextcloud-zfs/nextcloud/data/__groupfolders/1/Sermons/Raw Upload"
# Log file
LOG_FILE="/var/log/sermon-monitor.log"

# Create log directory if it doesn't exist
mkdir -p "$(dirname "$LOG_FILE")" 2>/dev/null || true
touch "$LOG_FILE" 2>/dev/null || true

# Log function
log() {
  echo "$(date '+%Y-%m-%d %H:%M:%S') - $1" | tee -a "$LOG_FILE"
}

# Check if the watch directory exists
# if [ ! -d "$WATCH_DIR" ]; then
#   log "Error: Watch directory $WATCH_DIR does not exist"
#   exit 1
# fi

log "Starting gotify watch on $WATCH_DIR"

# Use gotify watch to monitor the directory and send notifications automatically
# when the directory contents change
# gotify watch -t "New Sermon Files" "ls -la '$WATCH_DIR' | sort"

# test with ssh
gotify watch -t "New Sermon Files" "ssh nic@ghost 'ls -la /tank/encrypted/docker/nextcloud-zfs/nextcloud/data/__groupfolders/1/Sermons/Raw\ Upload | sort'"

# If we get here, gotify watch has exited
log "gotify watch exited"
exit $?
