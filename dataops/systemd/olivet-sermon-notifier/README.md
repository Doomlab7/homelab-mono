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

#### Option A: System-wide installation (requires sudo/root)

```bash
# Enable and start the service
sudo systemctl enable ./sermon-monitor.service
sudo systemctl start ./sermon-monitor.service
```

#### Option B: User-level installation (no sudo required)

```bash
# Enable and start the service for your user
systemctl --user enable ./sermon-monitor.service
systemctl --user start ./sermon-monitor.service

# Enable lingering so the service runs even when you're not logged in
# OPTIONAL
loginctl enable-linger $USER
```

## Configuration

The following settings can be modified in `check-sermons.sh`:

- `DIR_PATH`: Target directory to monitor for new sermon files
- `MODE`: Set to "ssh" for remote monitoring or "local" for local monitoring
- `SSH_USER` and `SSH_HOST`: Remote server credentials (only used if MODE="ssh")
- `LOG_DIR` and `LOG_FILE`: Log file locations
- `MAX_LOG_SIZE_KB`: Maximum log file size before rotation (default: 1MB)

## Verification and Testing

### Check Service Status

```bash
# For system-wide installation
sudo systemctl status sermon-monitor.service

# For user-level installation
systemctl --user status sermon-monitor.service
```

### Restart the Service

If you need to restart the service:

```bash
# For system-wide installation
sudo systemctl restart sermon-monitor.service

# For user-level installation
systemctl --user restart sermon-monitor.service
```

### Check Logs

```bash
# View service logs (system-wide)
sudo journalctl -u sermon-monitor.service

# View service logs (user-level)
journalctl --user -u sermon-monitor.service

# View the script's log file
cat ~/.local/state/sermon-monitor/sermon-monitor.log
```