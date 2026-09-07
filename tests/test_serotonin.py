import os
import unittest
from datetime import datetime
from unittest.mock import patch

from serotonin import (
    FootballDataOrgAdapter,
    StartupConfigurationError,
    calculate_team_stats,
    callback_data,
    format_team_stats,
    load_settings,
    normalize_team_name,
    parse_callback_data,
)


def match(match_id, date, home_id, home_name, away_id, away_name, home_goals, away_goals):
    return {
        "id": match_id,
        "utcDate": date,
        "homeTeam": {"id": home_id, "name": home_name},
        "awayTeam": {"id": away_id, "name": away_name},
        "score": {"fullTime": {"home": home_goals, "away": away_goals}},
    }


class TeamMatchingTests(unittest.TestCase):
    def test_normalization_handles_accents_suffixes_and_punctuation(self):
        self.assertEqual(normalize_team_name("FC Barcelona!"), "barcelona")
        self.assertEqual(normalize_team_name("Bayern München"), "bayern munchen")

    def test_alias_matching_finds_man_city(self):
        candidates = FootballDataOrgAdapter.filter_candidates(
            "Man City", [(1, "Manchester City", ["PL"])]
        )
        self.assertEqual(candidates[0][0], 1)


class StatisticsTests(unittest.TestCase):
    def test_calculates_home_away_form_and_averages(self):
        matches = [
            match(1, "2026-01-03T00:00:00Z", 10, "Test FC", 20, "Away", 2, 0),
            match(2, "2026-01-02T00:00:00Z", 30, "Home", 10, "Test FC", 1, 1),
            match(3, "2026-01-01T00:00:00Z", 10, "Test FC", 40, "Away", 0, 3),
        ]
        stats = calculate_team_stats(10, "Test FC", matches)
        self.assertEqual((stats["wins"], stats["draws"], stats["losses"]), (1, 1, 1))
        self.assertEqual((stats["home_matches"], stats["away_matches"]), (2, 1))
        self.assertEqual(stats["clean_sheets_h"], 1)
        self.assertEqual(stats["home_goals_for"], 2)
        self.assertIn("33% win rate", format_team_stats(stats, datetime(2026, 1, 4)))

    def test_formatter_escapes_dynamic_html(self):
        stats = calculate_team_stats(1, "<Team>", [])
        self.assertIn("&lt;Team&gt;", format_team_stats(stats))


class CallbackTests(unittest.TestCase):
    def test_callbacks_are_compact_and_refresh_is_parseable(self):
        data = callback_data("refresh", 12345, "PL")
        self.assertLessEqual(len(data.encode("utf-8")), 64)
        self.assertEqual(parse_callback_data(data), ("refresh", 12345, "PL"))

    def test_team_search_callback_does_not_embed_unbounded_input(self):
        self.assertEqual(parse_callback_data("retry"), ("retry", None, None))

    def test_invalid_callback_is_rejected(self):
        with self.assertRaises(ValueError):
            parse_callback_data("comp:1:<unsafe>")


class ConfigurationTests(unittest.TestCase):
    def test_missing_secrets_are_reported_without_values(self):
        with self.assertRaises(StartupConfigurationError) as raised:
            load_settings({})
        self.assertIn("TELEGRAM_BOT_TOKEN", str(raised.exception))
        self.assertIn("FOOTBALL_DATA_API_KEY", str(raised.exception))

    def test_settings_load_from_environment(self):
        env = {
            "TELEGRAM_BOT_TOKEN": "test-token",
            "FOOTBALL_DATA_API_KEY": "test-key",
        }
        with patch.dict(os.environ, env, clear=True):
            settings = load_settings()
        self.assertEqual(settings.telegram_bot_token, "test-token")
        self.assertEqual(settings.football_data_api_key, "test-key")


if __name__ == "__main__":
    unittest.main()