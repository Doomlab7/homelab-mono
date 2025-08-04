# Archivebox Docker Compose Setup

This directory contains the Docker Compose configuration for running Archivebox and its associated Telegram bot.

## Prerequisites

1. Copy `.env.example` to `.env` and update the values accordingly:
   ```bash
   cp .env.example .env
   ```

2. For the Telegram bot to work properly, you need to provide a `cookies.txt` file in this directory. Instructions for getting the cookies can be found in the [source repo](https://github.com/pypeaday/ArchiveboxTelegramBot?tab=readme-ov-file#how-to-use).

## Configuration

The `.env` file contains all necessary configuration values. Make sure to:

1. Set the correct path for `ARCHIVEBOX_DATA_DIRECTORY`
2. Update the Telegram bot configuration with your own values:
   - `ARCHIVEBOX_BOT_CHATIDS`
   - `ARCHIVEBOX_BOT_BOT_TOKEN`
   - `ARCHIVEBOX_BOT_CSRFMIDDLEWARETOKEN`

## Usage

Start the services:
```bash
docker compose up -d
```

Access Archivebox at: `http://localhost:8004` (or whatever port you configured)

## Notes

- The Archivebox service must be running before the Telegram bot can connect to it
- Make sure the cookies.txt file is present before starting the services
- The Telegram bot requires valid authentication tokens and chat IDs to function properly