from __future__ import annotations

from datetime import date, datetime, timezone
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.launch_library import (
    load_or_fetch_launch_rows,
    normalize_launch,
    split_launch_sections,
)


RAW_LAUNCH = {
    "id": "abc-123",
    "name": "Falcon 9 Block 5 | ExampleSat",
    "window_start": "2026-04-19T15:30:00Z",
    "window_end": "2026-04-19T17:30:00Z",
    "status": {"name": "Go for Launch"},
    "launch_service_provider": {"name": "SpaceX"},
    "rocket": {"configuration": {"full_name": "Falcon 9 Block 5", "name": "Falcon 9"}},
    "pad": {
        "name": "Space Launch Complex 40",
        "location": {"name": "Cape Canaveral SFS, FL, USA"},
    },
    "mission": {"name": "ExampleSat Mission", "type": "Communications"},
    "image": {"image_url": "https://example.test/launch.jpg"},
}


class LaunchLibraryTests(unittest.TestCase):
    def test_normalize_launch_shape(self) -> None:
        launch = normalize_launch(RAW_LAUNCH)

        self.assertEqual(launch["id"], "abc-123")
        self.assertEqual(launch["name"], "Falcon 9 Block 5 | ExampleSat")
        self.assertEqual(launch["window_start"], "2026-04-19T15:30:00Z")
        self.assertEqual(launch["window_end"], "2026-04-19T17:30:00Z")
        self.assertEqual(launch["status"], "Go for Launch")
        self.assertEqual(launch["provider"], "SpaceX")
        self.assertEqual(launch["vehicle"], "Falcon 9 Block 5")
        self.assertEqual(launch["pad_name"], "Space Launch Complex 40")
        self.assertEqual(launch["location_name"], "Cape Canaveral SFS, FL, USA")
        self.assertEqual(launch["mission_name"], "ExampleSat Mission")
        self.assertEqual(launch["mission_type"], "Communications")
        self.assertEqual(launch["image_url"], "https://example.test/launch.jpg")

    def test_split_launch_sections_avoids_duplicates(self) -> None:
        rows = [
            RAW_LAUNCH,
            {
                **RAW_LAUNCH,
                "id": "future-1",
                "name": "Future Launch",
                "window_start": "2026-04-20T10:00:00Z",
                "window_end": None,
            },
        ]

        sections = split_launch_sections(rows, today=date(2026, 4, 19))

        self.assertEqual([launch["id"] for launch in sections["today"]], ["abc-123"])
        self.assertEqual([launch["id"] for launch in sections["upcoming"]], ["future-1"])

    def test_load_or_fetch_returns_empty_cached_rows_when_api_unavailable_later(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            cache_dir = Path(temp)
            now = datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc)
            with patch("scripts.launch_library.fetch_launch_library_rows", return_value=[]):
                rows = load_or_fetch_launch_rows(
                    cache_dir=cache_dir,
                    start=date(2026, 4, 19),
                    end=date(2026, 5, 1),
                    now=now,
                    timeout=5,
                    force_refresh=False,
                    cache_max_age_hours=6,
                )
            self.assertEqual(rows, [])


if __name__ == "__main__":
    unittest.main()
