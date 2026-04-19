from __future__ import annotations

from datetime import datetime, timezone
import json
import tempfile
import unittest
from pathlib import Path

from scripts.build_insights import normalize_insights_base_url
from scripts.space_track_insights import build_insights_manifest, build_space_track_insights, write_insights_output


GP_ROWS = [
    {
        "NORAD_CAT_ID": "25544",
        "OBJECT_NAME": "ISS (ZARYA)",
        "OBJECT_ID": "1998-067A",
        "MEAN_MOTION": "15.49",
        "ECCENTRICITY": "0.0004",
        "INCLINATION": "51.6",
    },
    {
        "NORAD_CAT_ID": "44713",
        "OBJECT_NAME": "STARLINK-1008",
        "OBJECT_ID": "2019-074A",
        "MEAN_MOTION": "15.05",
        "ECCENTRICITY": "0.0001",
        "INCLINATION": "53.0",
    },
    {
        "NORAD_CAT_ID": "44714",
        "OBJECT_NAME": "STARLINK-1009",
        "OBJECT_ID": "2019-074B",
        "MEAN_MOTION": "15.05",
        "ECCENTRICITY": "0.0001",
        "INCLINATION": "53.0",
    },
]

SATCAT_ROWS = [
    {
        "NORAD_CAT_ID": "25544",
        "OBJECT_NAME": "ISS (ZARYA)",
        "OBJECT_TYPE": "PAYLOAD",
        "OPS_STATUS_CODE": "+",
        "OWNER": "US",
        "COUNTRY": "US",
        "LAUNCH_DATE": "1998-11-20",
        "PERIGEE": "410",
        "APOGEE": "423",
    },
    {
        "NORAD_CAT_ID": "44713",
        "OBJECT_NAME": "STARLINK-1008",
        "OBJECT_TYPE": "PAYLOAD",
        "OPS_STATUS_CODE": "+",
        "OWNER": "US",
        "COUNTRY": "US",
        "LAUNCH_DATE": "2026-04-19",
        "PERIGEE": "540",
        "APOGEE": "550",
    },
    {
        "NORAD_CAT_ID": "44714",
        "OBJECT_NAME": "STARLINK-1009",
        "OBJECT_TYPE": "PAYLOAD",
        "OPS_STATUS_CODE": "+",
        "OWNER": "US",
        "COUNTRY": "US",
        "LAUNCH_DATE": "2026-04-18",
        "PERIGEE": "540",
        "APOGEE": "550",
    },
    {
        "NORAD_CAT_ID": "90001",
        "OBJECT_NAME": "OLD ROCKET R/B",
        "OBJECT_TYPE": "ROCKET BODY",
        "OWNER": "CIS",
        "LAUNCH_DATE": "1970-01-01",
        "PERIGEE": "1200",
        "APOGEE": "35000",
    },
]

DECAY_ROWS = [
    {
        "NORAD_CAT_ID": "90001",
        "OBJECT_NAME": "OLD ROCKET R/B",
        "DECAY_EPOCH": "2026-04-19T14:00:00",
    }
]


class SpaceTrackInsightsTests(unittest.TestCase):
    def test_builds_expected_app_insights_shape(self) -> None:
        insights = build_space_track_insights(
            gp_rows=GP_ROWS,
            satcat_rows=SATCAT_ROWS,
            decay_rows=DECAY_ROWS,
            satcat_debut_rows=[],
            generated_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(insights["schema_version"], 1)
        self.assertEqual(insights["last_updated"], "2026-04-19T12:00:00Z")
        self.assertEqual(insights["counts"]["gp"], 3)
        self.assertEqual(insights["counts"]["satcat"], 4)
        self.assertEqual(insights["highlights"]["biggest_constellation"]["name"], "STARLINK")
        self.assertEqual(insights["today"]["launches"], [])
        self.assertEqual(insights["upcoming"]["launches"], [])
        self.assertEqual(len(insights["today"]["reentries"]), 1)
        self.assertEqual(insights["breakdowns"]["by_orbit"][0]["key"], "LEO")
        self.assertEqual(insights["highlights"]["lowest_active_orbit"]["category"], "Payload")
        self.assertEqual(insights["highlights"]["lowest_active_orbit"]["category_key"], "payload")
        self.assertEqual(insights["highlights"]["lowest_active_orbit"]["country"], "United States")
        self.assertEqual(insights["highlights"]["lowest_active_orbit"]["country_key"], "US")
        self.assertIn({"key": "payload", "label": "Payload", "count": 3}, insights["breakdowns"]["by_category"])
        self.assertIn({"key": "CIS", "label": "Commonwealth of Independent States", "count": 1}, insights["breakdowns"]["by_country"])
        self.assertIn("active_vs_debris", insights["trends"])

    def test_launch_sections_are_explicit_inputs_not_space_track_launch_dates(self) -> None:
        launch = {
            "id": "launch-1",
            "name": "Example Launch",
            "window_start": "2026-04-19T12:00:00Z",
            "window_end": None,
            "status": "Go",
            "provider": "Example Provider",
            "vehicle": "Example Rocket",
            "pad_name": "Example Pad",
            "location_name": "Example Location",
            "mission_name": "Example Mission",
            "mission_type": "Test",
            "image_url": None,
        }
        insights = build_space_track_insights(
            gp_rows=GP_ROWS,
            satcat_rows=SATCAT_ROWS,
            decay_rows=DECAY_ROWS,
            today_launches=[launch],
            upcoming_launches=[],
            generated_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(insights["today"]["launches"], [launch])
        self.assertEqual(insights["upcoming"]["launches"], [])

    def test_output_is_written_as_current_json(self) -> None:
        insights = build_space_track_insights(
            gp_rows=GP_ROWS,
            satcat_rows=SATCAT_ROWS,
            decay_rows=DECAY_ROWS,
            generated_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
        )
        with tempfile.TemporaryDirectory() as temp:
            output_dir = Path(temp)
            write_insights_output(output_dir, insights)
            written = json.loads((output_dir / "current.json").read_text(encoding="utf-8"))
            self.assertEqual(written["counts"]["merged"], 4)

    def test_insights_base_url_points_to_insights(self) -> None:
        self.assertEqual(
            normalize_insights_base_url("satellite-catalog-mirror.example.workers.dev"),
            "https://satellite-catalog-mirror.example.workers.dev/insights",
        )
        self.assertEqual(
            normalize_insights_base_url("https://satellite-catalog-mirror.example.workers.dev/catalog"),
            "https://satellite-catalog-mirror.example.workers.dev/insights",
        )
        self.assertEqual(
            normalize_insights_base_url("https://satellite-catalog-mirror.example.workers.dev/insights"),
            "https://satellite-catalog-mirror.example.workers.dev/insights",
        )

    def test_manifest_urls_match_worker_insights_routes(self) -> None:
        raw = b'{"ok":true}\n'
        manifest = build_insights_manifest(
            raw,
            datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
            "https://satellite-catalog-mirror.example.workers.dev/insights",
        )

        self.assertEqual(
            manifest["manifest"]["url"],
            "https://satellite-catalog-mirror.example.workers.dev/insights/manifest.json",
        )
        self.assertEqual(
            manifest["insights"]["url"],
            "https://satellite-catalog-mirror.example.workers.dev/insights/current.json",
        )
        self.assertEqual(
            manifest["insights_gzip"]["url"],
            "https://satellite-catalog-mirror.example.workers.dev/insights/current.json.gz",
        )

    def test_country_breakdown_uses_only_grounded_country_labels(self) -> None:
        insights = build_space_track_insights(
            gp_rows=[
                {"NORAD_CAT_ID": "1", "OBJECT_NAME": "ITALY SAT", "MEAN_MOTION": "14.2", "ECCENTRICITY": "0.001"},
                {"NORAD_CAT_ID": "2", "OBJECT_NAME": "KOREA SAT", "MEAN_MOTION": "14.2", "ECCENTRICITY": "0.001"},
                {"NORAD_CAT_ID": "3", "OBJECT_NAME": "ORG SAT", "MEAN_MOTION": "14.2", "ECCENTRICITY": "0.001"},
                {"NORAD_CAT_ID": "4", "OBJECT_NAME": "COMPANY SAT", "MEAN_MOTION": "14.2", "ECCENTRICITY": "0.001"},
            ],
            satcat_rows=[
                {"NORAD_CAT_ID": "1", "OBJECT_TYPE": "PAYLOAD", "OPS_STATUS_CODE": "+", "OWNER": "IT", "PERIGEE": "500", "APOGEE": "600"},
                {"NORAD_CAT_ID": "2", "OBJECT_TYPE": "PAYLOAD", "OPS_STATUS_CODE": "+", "OWNER": "SKOR", "PERIGEE": "500", "APOGEE": "600"},
                {"NORAD_CAT_ID": "3", "OBJECT_TYPE": "PAYLOAD", "OPS_STATUS_CODE": "+", "OWNER": "ITSO", "PERIGEE": "500", "APOGEE": "600"},
                {"NORAD_CAT_ID": "4", "OBJECT_TYPE": "PAYLOAD", "OPS_STATUS_CODE": "+", "OWNER": "ORB", "PERIGEE": "500", "APOGEE": "600"},
            ],
            decay_rows=[],
            generated_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
        )

        self.assertIn({"key": "IT", "label": "Italy", "count": 1}, insights["breakdowns"]["by_country"])
        self.assertIn({"key": "SKOR", "label": "South Korea", "count": 1}, insights["breakdowns"]["by_country"])
        self.assertNotIn("Itso", [row["label"] for row in insights["breakdowns"]["by_country"]])
        self.assertNotIn("Orb", [row["label"] for row in insights["breakdowns"]["by_country"]])
        self.assertEqual(insights["breakdowns"]["by_operator"], [])

    def test_orbit_highlights_ignore_zero_or_invalid_altitudes(self) -> None:
        insights = build_space_track_insights(
            gp_rows=[
                {"NORAD_CAT_ID": "10", "OBJECT_NAME": "ZERO ORBIT", "MEAN_MOTION": "14.2", "ECCENTRICITY": "0.001"},
                {"NORAD_CAT_ID": "11", "OBJECT_NAME": "VALID LOW", "MEAN_MOTION": "14.2", "ECCENTRICITY": "0.001"},
                {"NORAD_CAT_ID": "12", "OBJECT_NAME": "VALID HIGH", "MEAN_MOTION": "1.0", "ECCENTRICITY": "0.001"},
            ],
            satcat_rows=[
                {"NORAD_CAT_ID": "10", "OBJECT_TYPE": "PAYLOAD", "OPS_STATUS_CODE": "+", "OWNER": "US", "PERIGEE": "0", "APOGEE": "0"},
                {"NORAD_CAT_ID": "11", "OBJECT_TYPE": "PAYLOAD", "OPS_STATUS_CODE": "+", "OWNER": "US", "PERIGEE": "450", "APOGEE": "550"},
                {"NORAD_CAT_ID": "12", "OBJECT_TYPE": "PAYLOAD", "OPS_STATUS_CODE": "+", "OWNER": "US", "PERIGEE": "35000", "APOGEE": "36000"},
            ],
            decay_rows=[],
            generated_at=datetime(2026, 4, 19, 12, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(insights["highlights"]["lowest_active_orbit"]["norad_cat_id"], 11)
        self.assertEqual(insights["highlights"]["highest_orbit"]["norad_cat_id"], 12)


if __name__ == "__main__":
    unittest.main()
