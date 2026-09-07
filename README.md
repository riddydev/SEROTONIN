# Serotonin Telegram Football Bot

Serotonin is a Telegram bot that looks up football teams and displays recent
results, form, home/away scoring averages, clean sheets, and head-to-head
matches. It uses the [football-data.org v4 API](https://www.football-data.org/).

## Required Replit Secrets

Add these as Replit Secrets. Do not put them in source code, `.env` files, or
chat messages:

- `TELEGRAM_BOT_TOKEN` — the token from BotFather
- `FOOTBALL_DATA_API_KEY` — a football-data.org API token

The bot validates both values at startup and exits with a clear message if
either is missing. Existing tokens that were previously committed or exposed
in logs should be revoked and replaced.

Optional environment variables:

- `FOOTBALL_DATA_API_BASE_URL` — defaults to `https://api.football-data.org/v4`
- `SEROTONIN_CACHE_DB` — defaults to `cache.db`
- `LOG_LEVEL` — defaults to `INFO`

## Running

The canonical command is:

```bash
python serotonin.py
```

The Replit `Telegram Bot` workflow and deployment configuration both use this
same command. Only one process should run for a given Telegram bot token.
Serotonin also uses a local lock file to prevent duplicate local workflow
instances and performs a Telegram polling preflight check before starting.

## Commands

- `/help` — show available commands
- `/team Manchester City` — find a team and choose a competition
- `/h2h Man City vs Liverpool` — show recent head-to-head results

Team statistics can be filtered by competition or combined across leagues.
The Refresh button bypasses the local match cache.

## Tests

Run the standard-library test suite with:

```bash
python -m unittest discover -s tests -v
```

## Troubleshooting

### Telegram polling conflict

Telegram allows only one long-polling process per bot token. If startup
reports a polling conflict, stop the other Replit workflow, deployment, local
process, or hosting service using the same token. Then start only the
`Telegram Bot` workflow.

### Football API errors

Check that `FOOTBALL_DATA_API_KEY` exists and is valid, and that the API plan
allows the requested competitions. The bot retries transient timeouts and
rate limits without exposing credentials.

### No matches or team results

Try the club's common name or nickname, such as `Barca`, `Juve`, `Spurs`, or
`Man City`. Results depend on the competitions and completed matches available
from football-data.org.