# Olivet Sermon Notifier

A systemd service to monitor for new sermon files and send Gotify notifications using the gotify watch feature.

## Overview

This utility continuously monitors for new sermon files in a designated directory and sends Gotify notifications when changes are detected. It uses:

- A bash script utilizing `gotify watch` for real-time directory monitoring
- A systemd service to run the script as a background service
- The gotify CLI's built-in watch functionality for monitoring and notification

## Requirements

- gotify/cli (installed and configured)
- systemd (standard on most Linux distributions)

## Installation

### 1. Ensure gotify CLI is installed

If you haven't installed gotify CLI yet, follow the instructions in the main repository README.

Quick install:
```bash
curl -s https://i.paynepride.com/gotify/cli\?as\=gotify | bash
sudo mv gotify /usr/local/bin/
```

### 2. Initialize gotify CLI

Run the initialization wizard:
```bash
gotify init
```

Follow the prompts to configure your Gotify server URL and token.

### 3. Make the script executable

```bash
chmod +x check-sermons.sh
```

### 4. Install the systemd service

```bash
# Copy the service file to the systemd directory
sudo cp sermon-monitor.service /etc/systemd/system/

# Reload systemd to recognize the new service
sudo systemctl daemon-reload

# Enable and start the service
sudo systemctl enable sermon-monitor.service
sudo systemctl start sermon-monitor.service
```

## Configuration

The following settings can be modified in `check-sermons.sh`:

- `WATCH_DIR`: Directory to monitor for new sermon files
- `LOG_FILE`: Location of the log file

## Verification and Testing

### Check Service Status

```bash
# Verify the service is active and running
sudo systemctl status sermon-monitor.service
```

### Restart the Service

If you need to restart the service:

```bash
sudo systemctl restart sermon-monitor.service
```

### Check Logs

```bash
# View service logs
sudo journalctl -u sermon-monitor.service

# View the script's log file
sudo cat /var/log/sermon-monitor.log
```

### Test with a New File

Create a test file in the monitored directory to trigger a notification:

```bash
touch "/tank/encrypted/docker/nextcloud-zfs/nextcloud/data/__groupfolders/1/Sermons/Raw Upload/test-file.txt"
```

Then run the service manually to check if it works:

```bash
sudo systemctl start sermon-monitor.service
```
