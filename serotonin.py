"""Serotonin: a Telegram football statistics bot.

The bot intentionally keeps its public surface small:
  /help
  /team <team name>
  /h2h <team 1> vs <team 2>

Runtime credentials are read from Replit Secrets/environment variables and are
never included in log messages or user-facing errors.
"""

from __future__ import annotations

import asyncio
import fcntl
import html
import json
import logging
import os
import re
import sqlite3
import time
import unicodedata
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx
from rapidfuzz import fuzz
from telegram import Bot, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import Conflict, NetworkError, TelegramError
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)


logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# HTTPX logs request URLs. Telegram bot URLs contain the bot token, so keep
# those loggers quiet even when application logging is set to INFO.
for _logger_name in ("httpx", "httpcore", "telegram.request", "telegram.ext._updater"):
    logging.getLogger(_logger_name).setLevel(logging.WARNING)


class StartupConfigurationError(RuntimeError):
    """Raised when the bot cannot safely start."""


class FootballAPIError(RuntimeError):
    """A safe, user-facing football API failure."""

    def __init__(self, message: str, status_code: int | None = None):
        super().__init__(message)
        self.public_message = message
        self.status_code = status_code


@dataclass(frozen=True)
class Settings:
    telegram_bot_token: str
    football_data_api_key: str
    football_api_base_url: str = "https://api.football-data.org/v4"
    cache_db_path: str = "cache.db"


def load_settings(environ: dict[str, str] | None = None) -> Settings:
    """Load and validate required settings without exposing their values."""

    values = os.environ if environ is None else environ
    token = values.get("TELEGRAM_BOT_TOKEN", "").strip()
    api_key = values.get("FOOTBALL_DATA_API_KEY", "").strip()
    missing = []
    if not token:
        missing.append("TELEGRAM_BOT_TOKEN")
    if not api_key:
        missing.append("FOOTBALL_DATA_API_KEY")
    if missing:
        raise StartupConfigurationError(
            "Missing required Replit Secrets: " + ", ".join(missing)
        )

    base_url = values.get(
        "FOOTBALL_DATA_API_BASE_URL", "https://api.football-data.org/v4"
    ).strip()
    if not base_url.startswith(("https://", "http://")):
        raise StartupConfigurationError(
            "FOOTBALL_DATA_API_BASE_URL must be an HTTP(S) URL"
        )

    return Settings(
        telegram_bot_token=token,
        football_data_api_key=api_key,
        football_api_base_url=base_url.rstrip("/"),
        cache_db_path=values.get("SEROTONIN_CACHE_DB", "cache.db").strip()
        or "cache.db",
    )


def normalize_team_name(text: str) -> str:
    """Normalize accents, common suffixes, punctuation, and whitespace."""

    text = unicodedata.normalize("NFKD", text or "")
    text = text.encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"\b(fc|cf|afc|sc|ac|cd|fk|sk|bk)\b", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


class APICache:
    """Small persistent JSON cache with a time-to-live."""

    def __init__(self, ttl_seconds: int = 3600, db_path: str = "cache.db"):
        self.ttl = ttl_seconds
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS cache (
                        key TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        timestamp REAL NOT NULL
                    )
                    """
                )
        except sqlite3.Error as exc:
            logger.warning("Cache initialization failed: %s", exc.__class__.__name__)

    def get(self, key: str) -> Any | None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    "SELECT value, timestamp FROM cache WHERE key = ?", (key,)
                ).fetchone()
            if not row:
                return None
            value_json, timestamp = row
            if time.time() - timestamp >= self.ttl:
                with sqlite3.connect(self.db_path) as conn:
                    conn.execute("DELETE FROM cache WHERE key = ?", (key,))
                return None
            return json.loads(value_json)
        except (sqlite3.Error, ValueError, TypeError) as exc:
            logger.warning("Cache read failed: %s", exc.__class__.__name__)
            return None

    def set(self, key: str, value: Any) -> None:
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """
                    INSERT OR REPLACE INTO cache (key, value, timestamp)
                    VALUES (?, ?, ?)
                    """,
                    (key, json.dumps(value), time.time()),
                )
        except (sqlite3.Error, TypeError, ValueError) as exc:
            logger.warning("Cache write failed: %s", exc.__class__.__name__)


class RateLimiter:
    """Simple process-local per-user request limiter."""

    def __init__(self, max_requests: int = 15, window_seconds: int = 60):
        self.max_requests = max_requests
        self.window = window_seconds
        self.requests: dict[int, list[float]] = {}

    def is_allowed(self, user_id: int) -> bool:
        now = time.monotonic()
        recent = [
            timestamp
            for timestamp in self.requests.get(user_id, [])
            if now - timestamp < self.window
        ]
        if len(recent) >= self.max_requests:
            self.requests[user_id] = recent
            return False
        recent.append(now)
        self.requests[user_id] = recent
        return True

    def get_wait_time(self, user_id: int) -> int:
        recent = self.requests.get(user_id, [])
        if not recent:
            return 0
        return max(0, int(self.window - (time.monotonic() - recent[0])))


class SingleInstanceLock:
    """Prevent two local workflow processes from polling the same bot."""

    def __init__(self, path: str = ".serotonin.lock"):
        self.path = Path(path)
        self._file: Any | None = None

    def __enter__(self) -> "SingleInstanceLock":
        self._file = self.path.open("w")
        try:
            fcntl.flock(self._file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            self._file.close()
            self._file = None
            raise StartupConfigurationError(
                "Another local Serotonin process is already running. "
                "Stop the existing Telegram Bot workflow before starting another."
            ) from exc
        return self

    def __exit__(self, *_: Any) -> None:
        if self._file is not None:
            with suppress(OSError):
                fcntl.flock(self._file.fileno(), fcntl.LOCK_UN)
            self._file.close()
            self._file = None


class FootballAPIAdapter:
    """Base adapter for football API services."""

    def __init__(
        self,
        base_url: str,
        api_key: str,
        client: httpx.AsyncClient | None = None,
        cache_instance: APICache | None = None,
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(12.0, connect=5.0)
        )
        self._owns_client = client is None
        self.cache = cache_instance or cache

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()


class FootballDataOrgAdapter(FootballAPIAdapter):
    """Async football-data.org v4 adapter."""

    COMPETITION_MAP = {
        "PL": "Premier League (England)",
        "PD": "La Liga (Spain)",
        "SA": "Serie A (Italy)",
        "BL1": "Bundesliga (Germany)",
        "FL1": "Ligue 1 (France)",
        "CL": "UEFA Champions League",
        "DFB": "DFB Pokal (Germany Cup)",
        "CDR": "Copa del Rey (Spain Cup)",
        "UECL": "UEFA Europa Conference League",
        "PPL": "Primeira Liga (Portugal)",
        "PT1": "Primeira Liga (Portugal)",
        "NL1": "Eredivisie (Netherlands)",
        "GRE": "Super League (Greece)",
    }

    TEAM_ALIASES = {
        "arsenal": ["arsenal fc", "gunners", "gooners"],
        "aston villa": ["villa", "villans", "villains", "lions", "avfc"],
        "bournemouth": ["afc bournemouth", "cherries", "boscombe"],
        "brentford": ["bees", "brentford fc"],
        "brighton": ["brighton & hove albion", "seagulls", "albion"],
        "burnley": ["clarets", "burnley fc"],
        "chelsea": ["chelsea fc", "blues", "pensioners"],
        "crystal palace": ["palace", "eagles", "glaziers"],
        "everton": ["everton fc", "toffeemen", "toffees", "blues"],
        "fulham": ["cottagers", "fulham fc"],
        "leeds": ["leeds united", "whites", "peacocks", "united"],
        "liverpool": ["liverpool fc", "reds", "liv"],
        "manchester city": ["man city", "city", "citizens", "cityzens", "sky blues"],
        "manchester united": ["man united", "united", "red devils"],
        "newcastle": ["newcastle united", "magpies", "toon", "toon army", "castle"],
        "nottingham forest": ["forest", "notts forest", "reds", "tricky trees"],
        "sunderland": ["black cats", "mackems", "safc"],
        "tottenham": ["tottenham hotspur", "spurs", "lilywhites"],
        "west ham": ["west ham united", "hammers", "irons"],
        "wolves": ["wolverhampton wanderers", "wolves", "wanderers", "old gold"],
        "barca": ["fc barcelona", "barcelona", "barça", "culers", "blaugrana"],
        "barcelona": ["fc barcelona", "barça", "barca", "culers"],
        "real madrid": ["madrid", "real", "los blancos", "merengues", "vikingos"],
        "atletico": ["atlético de madrid", "atletico madrid", "colchoneros", "indios"],
        "athletic": ["athletic club", "athletic bilbao", "los leones", "lions"],
        "betis": ["real betis balompié", "real betis", "verdiblancos", "heliopolitanos"],
        "sevilla": ["sevilla fc", "seville", "nervionenses", "palanganas"],
        "valencia": ["valencia cf", "los che", "valencia", "murcielagos"],
        "villareal": ["villarreal cf", "yellow submarine", "submarino amarillo", "groguets"],
        "girona": ["girona fc", "gironins"],
        "osasuna": ["ca osasuna", "rojillos", "gorritxoak"],
        "celta": ["celta de vigo", "celtics", "celestes"],
        "elche": ["elche cf", "franjiverdes", "ilos"],
        "oviedo": ["real oviedo", "carbayones", "oviedistas"],
        "juve": ["juventus fc", "juventus", "old lady", "vecchia signora", "bianconeri", "zebras"],
        "inter": ["fc internazionale milano", "inter milan", "nerazzurri", "beneamata"],
        "milan": ["ac milan", "rossoneri", "diavolo"],
        "napoli": ["ssc napoli", "partenopei", "azzurri"],
        "roma": ["as roma", "giallorossi", "lupa", "magica"],
        "lazio": ["ss lazio", "biancocelesti", "aquile"],
        "atalanta": ["atalanta bc", "dea", "bergamo", "nerazzurri"],
        "fiorentina": ["acf fiorentina", "viola", "gigliati"],
        "bologna": ["bologna fc", "rossoblu", "felsinei"],
        "torino": ["torino fc", "granata", "toro"],
        "udinese": ["udinese calcio", "bianconeri", "zebre", "udine"],
        "como": ["como 1907", "lariani", "azzurri"],
        "cremonese": ["us cremonese", "grigiorossi", "tigrotti"],
        "pisa": ["pisa sc", "nerazzurri", "torre"],
        "bayern": ["fc bayern münchen", "bayern munich", "fcb", "munich", "die roten"],
        "dortmund": ["borussia dortmund", "bvb", "schwarzgelben", "signal iduna"],
        "hamburg": ["hamburger sv", "hsv", "die rothosen"],
        "cologne": ["1. fc köln", "cologne", "die geißböcke"],
        "schalke": ["fc schalke 04", "schalke", "königsblau"],
        "leverkusen": ["bayer leverkusen", "werkself", "xabi alonso"],
        "borussia": ["borussia mönchengladbach", "fohlen", "colts"],
        "wolfsburg": ["vfl wolfsburg", "die wölfe"],
        "psg": ["paris saint-germain", "paris sg", "les parisiens"],
        "marseille": ["olympique de marseille", "om", "phocéens"],
        "lyon": ["olympique lyonnais", "ol"],
        "monaco": ["as monaco", "monegasques"],
        "lille": ["losc lille", "losc"],
        "man": ["manchester united", "manchester city"],
    }

    def _request_url(self, path: str) -> str:
        return f"{self.base_url}/{path.lstrip('/')}"

    async def _request_json(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        retries: int = 2,
    ) -> dict[str, Any]:
        headers = {"X-Auth-Token": self.api_key}
        for attempt in range(retries + 1):
            try:
                response = await self.client.get(
                    self._request_url(path), headers=headers, params=params
                )
            except httpx.TimeoutException as exc:
                if attempt < retries:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                raise FootballAPIError(
                    "The football service timed out. Please try again shortly."
                ) from exc
            except httpx.HTTPError as exc:
                if attempt < retries:
                    await asyncio.sleep(1.5 * (attempt + 1))
                    continue
                raise FootballAPIError(
                    "The football service could not be reached. Please try again shortly."
                ) from exc

            if response.status_code == 429 and attempt < retries:
                retry_after = response.headers.get("Retry-After", "")
                try:
                    delay = min(max(float(retry_after), 1.0), 30.0)
                except ValueError:
                    delay = 2.0 * (attempt + 1)
                await asyncio.sleep(delay)
                continue
            if response.status_code == 401:
                raise FootballAPIError(
                    "The football API credentials were rejected. Please contact the bot owner.",
                    status_code=401,
                )
            if response.status_code == 429:
                raise FootballAPIError(
                    "The football API is rate-limiting requests. Please try again shortly.",
                    status_code=429,
                )
            if response.status_code >= 500:
                raise FootballAPIError(
                    "The football service is temporarily unavailable. Please try again shortly.",
                    status_code=response.status_code,
                )
            if response.status_code != 200:
                raise FootballAPIError(
                    "The football service returned an unexpected response. Please try again.",
                    status_code=response.status_code,
                )
            try:
                return response.json()
            except (ValueError, json.JSONDecodeError) as exc:
                raise FootballAPIError(
                    "The football service returned invalid data. Please try again."
                ) from exc
        raise FootballAPIError("The football service could not be reached.")

    @staticmethod
    def filter_candidates(
        query: str, all_teams: Iterable[tuple[int, str, list[str]]]
    ) -> list[tuple[int, str, list[str], list[str]]]:
        q_norm = normalize_team_name(query)
        if not q_norm:
            return []
        candidates = []
        for team_id, full_name, competitions in all_teams:
            names = {normalize_team_name(full_name)}
            for base, aliases in FootballDataOrgAdapter.TEAM_ALIASES.items():
                if normalize_team_name(base) in names:
                    names.update(normalize_team_name(alias) for alias in aliases)
            if any(
                q_norm == name
                or q_norm in name
                or name in q_norm
                or (q_norm.split() and q_norm.split()[0] in name)
                for name in names
            ):
                candidates.append((team_id, full_name, list(names), competitions))
        return candidates

    @staticmethod
    def rank_candidates(
        query: str, candidates: Iterable[tuple[int, str, list[str], list[str]]]
    ) -> tuple[tuple[int, str] | None, float, list[str] | None]:
        q_norm = normalize_team_name(query)
        best: tuple[int, str] | None = None
        best_score = 0.0
        best_competitions: list[str] | None = None
        for team_id, full_name, variants, competitions in candidates:
            joined = " ".join(variants)
            score = fuzz.WRatio(q_norm, joined) * 0.6 + fuzz.partial_ratio(
                q_norm, joined
            ) * 0.4
            if q_norm in variants:
                score += 20
            if score > best_score:
                best = (team_id, full_name)
                best_score = score
                best_competitions = competitions
        return best, best_score, best_competitions

    async def get_team_all_competitions(self, team_id: int) -> list[str]:
        try:
            data = await self._request_json(f"teams/{team_id}")
        except FootballAPIError as exc:
            if exc.status_code in (404, 409):
                return []
            raise
        return [
            competition["code"]
            for competition in data.get("activeCompetitions", [])
            if competition.get("code")
        ]

    async def find_team_competitions(self, team_name: str) -> dict[str, Any] | None:
        cache_key = f"team_search_{normalize_team_name(team_name)}"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        team_competitions: dict[int, set[str]] = {}
        all_teams: dict[int, tuple[str, set[str]]] = {}
        for competition_code in self.COMPETITION_MAP:
            try:
                data = await self._request_json(
                    f"competitions/{competition_code}/teams", retries=1
                )
            except FootballAPIError as exc:
                if exc.status_code in (403, 404):
                    continue
                raise
            for team in data.get("teams", []):
                team_id = team.get("id")
                name = team.get("name")
                if not team_id or not name:
                    continue
                all_teams[team_id] = (name, set())
                team_competitions.setdefault(team_id, set()).add(competition_code)

        if not all_teams:
            return None

        candidates = self.filter_candidates(
            team_name,
            [
                (team_id, name, sorted(team_competitions.get(team_id, set())))
                for team_id, (name, _) in all_teams.items()
            ],
        )
        if not candidates:
            fuzzy_candidates = []
            query = normalize_team_name(team_name)
            for team_id, (name, _) in all_teams.items():
                if fuzz.WRatio(query, normalize_team_name(name)) >= 80:
                    fuzzy_candidates.append(
                        (
                            team_id,
                            name,
                            [normalize_team_name(name)],
                            sorted(team_competitions.get(team_id, set())),
                        )
                    )
            candidates = fuzzy_candidates

        best_team, best_score, _ = self.rank_candidates(team_name, candidates)
        if not best_team or best_score < 50:
            return None

        team_id, full_name = best_team
        competitions = sorted(team_competitions.get(team_id, set()))
        for code in await self.get_team_all_competitions(team_id):
            if code not in competitions:
                competitions.append(code)
        result = {"team_id": team_id, "name": full_name, "competitions": competitions}
        self.cache.set(cache_key, result)
        return result

    async def get_team_matches(
        self,
        team_id: int,
        *,
        limit: int = 100,
        date_from: str | None = None,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        cache_key = f"team_matches_{team_id}_{limit}"
        if date_from is None and not force_refresh:
            cached = self.cache.get(cache_key)
            if cached:
                return cached
        params: dict[str, Any] = {"status": "FINISHED", "limit": limit}
        if date_from:
            params["dateFrom"] = date_from
        data = await self._request_json(f"teams/{team_id}/matches", params=params)
        if date_from is None:
            self.cache.set(cache_key, data)
        return data


cache = APICache(ttl_seconds=3600)
rate_limiter = RateLimiter()
API_SERVERS: list[dict[str, Any]] = []


def init_api_servers(
    api_key: str,
    base_url: str = "https://api.football-data.org/v4",
    client: httpx.AsyncClient | None = None,
) -> FootballDataOrgAdapter:
    """Initialize the configured API provider exactly once."""

    global API_SERVERS
    adapter = FootballDataOrgAdapter(base_url, api_key, client=client)
    API_SERVERS = [{"type": "football-data.org", "adapter": adapter}]
    logger.info("Loaded football-data.org API")
    return adapter


def current_adapter() -> FootballDataOrgAdapter:
    if not API_SERVERS:
        raise FootballAPIError("The football API is not configured.")
    return API_SERVERS[0]["adapter"]


def calculate_team_stats(
    team_id: int,
    team_name: str,
    matches: list[dict[str, Any]],
    title: str | None = None,
) -> dict[str, Any]:
    """Calculate the statistics displayed by the team command."""

    ordered = sorted(matches, key=lambda match: match.get("utcDate", ""), reverse=True)
    recent_matches = []
    for match in ordered:
        score = match.get("score", {}).get("fullTime", {})
        if score.get("home") is not None and score.get("away") is not None:
            recent_matches.append(match)
        if len(recent_matches) == 10:
            break

    stats: dict[str, Any] = {
        "team_name": team_name,
        "title": title or team_name,
        "wins": 0,
        "draws": 0,
        "losses": 0,
        "home_wins": 0,
        "home_draws": 0,
        "home_losses": 0,
        "away_wins": 0,
        "away_draws": 0,
        "away_losses": 0,
        "goals_for": 0,
        "goals_against": 0,
        "home_matches": 0,
        "away_matches": 0,
        "home_goals_for": 0,
        "home_goals_against": 0,
        "away_goals_for": 0,
        "away_goals_against": 0,
        "clean_sheets_h": 0,
        "clean_sheets_a": 0,
        "matches_count": len(recent_matches),
    }

    for match in recent_matches:
        home = match["homeTeam"]["id"] == team_id
        home_goals = match["score"]["fullTime"]["home"]
        away_goals = match["score"]["fullTime"]["away"]
        team_goals, opponent_goals = (
            (home_goals, away_goals) if home else (away_goals, home_goals)
        )
        stats["goals_for"] += team_goals
        stats["goals_against"] += opponent_goals
        location = "home" if home else "away"
        stats[f"{location}_matches"] += 1
        stats[f"{location}_goals_for"] += team_goals
        stats[f"{location}_goals_against"] += opponent_goals
        if opponent_goals == 0:
            stats[f"clean_sheets_{'h' if home else 'a'}"] += 1
        if team_goals > opponent_goals:
            result = "wins"
        elif team_goals == opponent_goals:
            result = "draws"
        else:
            result = "losses"
        stats[result] += 1
        stats[f"{location}_{result}"] += 1
    return stats


def format_team_stats(stats: dict[str, Any], updated_at: datetime | None = None) -> str:
    """Format calculated stats as Telegram-safe HTML."""

    title = html.escape(str(stats["title"]))
    total = stats["matches_count"]
    win_pct = round((stats["wins"] / total) * 100) if total else 0

    def average(key: str, count_key: str) -> float:
        count = stats[count_key]
        return round(stats[key] / count, 2) if count else 0

    timestamp = (updated_at or datetime.now()).strftime("%m/%d/%Y %H:%M")
    return (
        f"<b>{title}</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "<b>📊 Form (Last 10 Matches)</b>\n"
        f"├─ <b>Overall:</b> {stats['wins']}W {stats['draws']}D "
        f"{stats['losses']}L ({win_pct}% win rate)\n"
        f"├─ 🏠 <b>Home:</b> {stats['home_wins']}W {stats['home_draws']}D "
        f"{stats['home_losses']}L\n"
        f"└─ ✈️ <b>Away:</b> {stats['away_wins']}W {stats['away_draws']}D "
        f"{stats['away_losses']}L\n\n"
        "<b>⚽ Scoring Average</b>\n"
        f"├─ 🏠 <b>Home:</b> {average('home_goals_for', 'home_matches')} for | "
        f"{average('home_goals_against', 'home_matches')} against\n"
        f"└─ ✈️ <b>Away:</b> {average('away_goals_for', 'away_matches')} for | "
        f"{average('away_goals_against', 'away_matches')} against\n\n"
        "<b>🛡️ Clean Sheets</b>\n"
        f"├─ 🏠 <b>Home:</b> {stats['clean_sheets_h']}\n"
        f"└─ ✈️ <b>Away:</b> {stats['clean_sheets_a']}\n\n"
        f"<b>📈 Average Goals/Match:</b> "
        f"{round(stats['goals_for'] / total, 2) if total else 0}\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 Updated: <code>{html.escape(timestamp)}</code>"
    )


def callback_data(*parts: object) -> str:
    """Build Telegram callback data and enforce its 64-byte limit."""

    data = ":".join(str(part) for part in parts)
    if len(data.encode("utf-8")) > 64:
        raise ValueError("Callback data exceeds Telegram's 64-byte limit")
    return data


def parse_callback_data(data: str) -> tuple[str, int | None, str | None]:
    """Parse compact, validated callback data."""

    parts = data.split(":")
    action = parts[0]
    if action in {"retry", "cancel"} and len(parts) == 1:
        return action, None, None
    if action in {"all", "back"} and len(parts) == 2:
        return action, int(parts[1]), None
    if action in {"comp", "refresh"} and len(parts) == 3:
        team_id = int(parts[1])
        competition = parts[2]
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,20}", competition):
            raise ValueError("Invalid competition callback")
        return action, team_id, competition
    raise ValueError("Invalid callback data")


def build_team_keyboard(team_id: int, competitions: list[str]) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton(
                f"📊 {FootballDataOrgAdapter.COMPETITION_MAP.get(code, code)}",
                callback_data=callback_data("comp", team_id, code),
            )
        ]
        for code in competitions
    ]
    buttons.append(
        [InlineKeyboardButton("📈 All Leagues", callback_data=callback_data("all", team_id))]
    )
    buttons.append(
        [
            InlineKeyboardButton("❌ Wrong Team?", callback_data=callback_data("retry")),
            InlineKeyboardButton("✅ Cancel", callback_data=callback_data("cancel")),
        ]
    )
    return InlineKeyboardMarkup(buttons)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    await update.message.reply_text(
        """
<b>⚽ Football Statistics Bot</b>

<b>Available Commands:</b>

📋 <b>/team</b> &lt;team_name&gt;
Find a team and view recent statistics.
Example: <code>/team Manchester City</code>

🔥 <b>/h2h</b> &lt;team1&gt; vs &lt;team2&gt;
View recent head-to-head results.
Example: <code>/h2h Man City vs Liverpool</code>

🆘 <b>/help</b>
Show this help message.

<b>Interactive Features:</b>
• Select a league for competition-specific stats.
• Use “All Leagues” for combined statistics.
• Refresh a result to bypass the cache.

Use common nicknames such as “Man City”, “Barca”, or “Spurs”.
""",
        parse_mode="HTML",
    )


def _user_id(update: Update) -> int:
    user = update.effective_user
    return user.id if user else 0


async def h2h_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user_id = _user_id(update)
    if not rate_limiter.is_allowed(user_id):
        await update.message.reply_text(
            f"⏱️ You are making requests too quickly. Please wait "
            f"{rate_limiter.get_wait_time(user_id)} seconds."
        )
        return
    if not context.args:
        await update.message.reply_text(
            "📖 Usage: /h2h Team1 vs Team2\n\n"
            "Example: <code>/h2h Man City vs Liverpool</code>",
            parse_mode="HTML",
        )
        return

    teams = re.split(r"\s+vs\s+", " ".join(context.args), maxsplit=1, flags=re.I)
    if len(teams) != 2 or not all(team.strip() for team in teams):
        await update.message.reply_text(
            "❌ Format: /h2h Team1 vs Team2\n\n"
            "Example: <code>/h2h Man City vs Liverpool</code>",
            parse_mode="HTML",
        )
        return

    team1_name, team2_name = (team.strip() for team in teams)
    status = await update.message.reply_text(
        f"🔥 Finding H2H matches: <b>{html.escape(team1_name)}</b> vs "
        f"<b>{html.escape(team2_name)}</b>...",
        parse_mode="HTML",
    )
    try:
        adapter = current_adapter()
        team1_info, team2_info = await asyncio.gather(
            adapter.find_team_competitions(team1_name),
            adapter.find_team_competitions(team2_name),
        )
        if not team1_info:
            await status.edit_text(
                f"❌ Could not find: <b>{html.escape(team1_name)}</b>",
                parse_mode="HTML",
            )
            return
        if not team2_info:
            await status.edit_text(
                f"❌ Could not find: <b>{html.escape(team2_name)}</b>",
                parse_mode="HTML",
            )
            return

        date_from = (datetime.now(timezone.utc) - timedelta(days=3 * 365)).strftime(
            "%Y-%m-%d"
        )
        data1, data2 = await asyncio.gather(
            adapter.get_team_matches(team1_info["team_id"], date_from=date_from),
            adapter.get_team_matches(team2_info["team_id"], date_from=date_from),
        )
        all_matches = {
            match.get("id"): match
            for data in (data1, data2)
            for match in data.get("matches", [])
            if match.get("id")
        }
        h2h_matches = [
            match
            for match in all_matches.values()
            if {
                match.get("homeTeam", {}).get("id"),
                match.get("awayTeam", {}).get("id"),
            }
            == {team1_info["team_id"], team2_info["team_id"]}
            and match.get("score", {}).get("fullTime", {}).get("home") is not None
            and match.get("score", {}).get("fullTime", {}).get("away") is not None
        ]
        h2h_matches.sort(key=lambda match: match.get("utcDate", ""), reverse=True)
        if not h2h_matches:
            await status.edit_text(
                f"📭 No head-to-head matches found between "
                f"<b>{html.escape(team1_info['name'])}</b> and "
                f"<b>{html.escape(team2_info['name'])}</b> in the last 3 years.",
                parse_mode="HTML",
            )
            return

        lines = [
            f"🔥 <b>{html.escape(team1_info['name'])} H2H "
            f"{html.escape(team2_info['name'])}</b>\n",
            "━━━━━━━━━━━━━━━━━━━━━━━\n",
        ]
        for match in h2h_matches[:5]:
            try:
                match_date = datetime.fromisoformat(
                    match.get("utcDate", "").replace("Z", "+00:00")
                ).strftime("%d %b %Y")
            except ValueError:
                match_date = "Unknown date"
            home = html.escape(match.get("homeTeam", {}).get("name", "Home"))
            away = html.escape(match.get("awayTeam", {}).get("name", "Away"))
            score = match["score"]["fullTime"]
            competition = html.escape(match.get("competition", {}).get("name", ""))
            lines.append(f"📅 <i>{match_date}</i>")
            if competition:
                lines.append(f" — {competition}")
            lines.append(f"\n{home}  {score['home']}\n{away}  {score['away']}\n")
        await status.edit_text("\n".join(lines), parse_mode="HTML")
    except FootballAPIError as exc:
        await status.edit_text(f"❌ {html.escape(exc.public_message)}", parse_mode="HTML")
    except Exception:
        logger.exception("Unexpected error in h2h command")
        await status.edit_text(
            "❌ Something went wrong while loading H2H data. Please try again later."
        )


async def team_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    user_id = _user_id(update)
    if not rate_limiter.is_allowed(user_id):
        await update.message.reply_text(
            f"⏱️ You are making requests too quickly. Please wait "
            f"{rate_limiter.get_wait_time(user_id)} seconds."
        )
        return
    if not context.args:
        await update.message.reply_text("📖 Usage: /team Team Name\n\nTry /help for more info")
        return

    team_name = " ".join(context.args).strip()
    status = await update.message.reply_text(
        f"🔍 Searching for <b>{html.escape(team_name)}</b>...", parse_mode="HTML"
    )
    try:
        info = await current_adapter().find_team_competitions(team_name)
        if not info:
            await status.edit_text(
                f"❌ <b>Team not found:</b> {html.escape(team_name)}\n\n"
                "Try another spelling, nickname, or abbreviation.",
                parse_mode="HTML",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔄 Try Again", callback_data=callback_data("retry"))]]
                ),
            )
            return
        if not info["competitions"]:
            await status.edit_text(
                f"❌ No competitions found for <b>{html.escape(info['name'])}</b>.",
                parse_mode="HTML",
            )
            return

        team_title = html.escape(info["name"])
        league_lines = "\n".join(
            f"• {html.escape(FootballDataOrgAdapter.COMPETITION_MAP.get(code, code))}"
            for code in info["competitions"]
        )
        await status.edit_text(
            f"⚽ <b>{team_title}</b>\n"
            "─────────────────\n"
            f"Found in {len(info['competitions'])} league(s):\n\n"
            f"{league_lines}\n\n<b>Select an option:</b>",
            reply_markup=build_team_keyboard(info["team_id"], info["competitions"]),
            parse_mode="HTML",
        )
    except FootballAPIError as exc:
        await status.edit_text(f"❌ {html.escape(exc.public_message)}", parse_mode="HTML")
    except Exception:
        logger.exception("Unexpected error in team command")
        await status.edit_text(
            "❌ Something went wrong while searching. Please try again later."
        )


async def send_team_stats(
    query: Any,
    team_id: int,
    comp_code: str | None = None,
    force_refresh: bool = False,
) -> None:
    try:
        data = await current_adapter().get_team_matches(
            team_id, force_refresh=force_refresh
        )
        matches = data.get("matches", [])
        if comp_code and comp_code != "all":
            matches = [
                match
                for match in matches
                if match.get("competition", {}).get("code") == comp_code
            ]
            competition_name = FootballDataOrgAdapter.COMPETITION_MAP.get(
                comp_code, comp_code
            )
            title_suffix = f" - {competition_name}"
        else:
            title_suffix = " - All Leagues"

        team_name = data.get("team", {}).get("name", "Team")
        stats = calculate_team_stats(team_id, team_name, matches, team_name + title_suffix)
        if not stats["matches_count"]:
            await query.edit_message_text(
                f"📭 No completed matches found for <b>{html.escape(stats['title'])}</b>.",
                parse_mode="HTML",
            )
            return

        await query.edit_message_text(
            format_team_stats(stats),
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(
                [
                    [
                        InlineKeyboardButton(
                            "🔄 Refresh",
                            callback_data=callback_data(
                                "refresh", team_id, comp_code or "all"
                            ),
                        )
                    ],
                    [InlineKeyboardButton("⬅️ Back", callback_data=callback_data("all", team_id))],
                ]
            ),
        )
    except FootballAPIError as exc:
        await query.edit_message_text(f"❌ {html.escape(exc.public_message)}", parse_mode="HTML")
    except Exception:
        logger.exception("Unexpected error while formatting team stats")
        await query.edit_message_text(
            "❌ Something went wrong while loading statistics. Please try again later."
        )


async def competition_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    try:
        action, team_id, competition = parse_callback_data(query.data or "")
    except (ValueError, TypeError):
        await query.answer("This button has expired. Please search again.", show_alert=True)
        return

    await query.answer()
    if action == "retry":
        await query.edit_message_text(
            "🔍 Try again with a different search term.\n\n"
            "Use: <code>/team NewTeamName</code>",
            parse_mode="HTML",
        )
    elif action == "cancel":
        await query.edit_message_text("✅ Search cancelled. Use /team to search again.")
    elif action in {"all", "comp", "refresh"} and team_id is not None:
        await send_team_stats(
            query,
            team_id,
            competition if action in {"comp", "refresh"} else None,
            force_refresh=action == "refresh",
        )


async def telegram_error_handler(
    update: object, context: ContextTypes.DEFAULT_TYPE
) -> None:
    error = context.error
    if isinstance(error, Conflict):
        logger.critical(
            "Telegram polling conflict: another process or deployment is using this bot token. "
            "Stop the other process and run only one Serotonin instance."
        )
        return
    logger.error("Unhandled Telegram error: %s", error.__class__.__name__)
    if isinstance(update, Update) and update.effective_message:
        with suppress(TelegramError):
            await update.effective_message.reply_text(
                "❌ Something went wrong processing that request. Please try again."
            )


async def preflight_telegram(bot: Bot) -> None:
    """Detect an already-running long poller before starting this process."""

    try:
        await bot.get_me()
        await bot.delete_webhook(drop_pending_updates=False)
        await bot.get_updates(timeout=0)
    except Conflict as exc:
        raise StartupConfigurationError(
            "Telegram polling conflict: another process or deployment is already "
            "using this bot token. Stop it before starting Serotonin."
        ) from exc
    except TelegramError as exc:
        raise StartupConfigurationError(
            "Telegram startup check failed. Verify the bot token and Telegram availability."
        ) from exc


async def poll_updates(app: Application) -> None:
    """Run one explicit polling loop so Telegram conflicts fail clearly."""

    offset: int | None = None
    backoff = 2.0
    while True:
        try:
            updates = await app.bot.get_updates(
                offset=offset, timeout=30, allowed_updates=Update.ALL_TYPES
            )
            backoff = 2.0
            for update in updates:
                offset = update.update_id + 1
                await app.process_update(update)
        except Conflict as exc:
            raise StartupConfigurationError(
                "Telegram polling conflict: another process or deployment is already "
                "using this bot token. Stop it before starting Serotonin."
            ) from exc
        except NetworkError:
            logger.warning(
                "Telegram network connection lost; retrying in %.0f seconds", backoff
            )
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)
        except TelegramError:
            logger.warning("Telegram polling error; retrying in %.0f seconds", backoff)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30.0)


def build_application(token: str) -> Application:
    application = ApplicationBuilder().token(token).build()
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("team", team_command))
    application.add_handler(CommandHandler("h2h", h2h_command))
    application.add_handler(CallbackQueryHandler(competition_callback))
    application.add_error_handler(telegram_error_handler)
    return application


async def run_bot(settings: Settings) -> None:
    adapter = init_api_servers(
        settings.football_data_api_key,
        settings.football_api_base_url,
    )
    application = build_application(settings.telegram_bot_token)
    await application.initialize()
    try:
        await preflight_telegram(application.bot)
        await application.start()
        logger.info("Serotonin bot is running with one explicit Telegram poller")
        await poll_updates(application)
    finally:
        if application.running:
            await application.stop()
        await application.shutdown()
        await adapter.close()


def main() -> None:
    try:
        settings = load_settings()
        with SingleInstanceLock():
            asyncio.run(run_bot(settings))
    except KeyboardInterrupt:
        logger.info("Serotonin bot stopped")
    except StartupConfigurationError as exc:
        logger.critical("Startup failed: %s", exc)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()