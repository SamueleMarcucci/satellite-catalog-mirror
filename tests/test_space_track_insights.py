from __future__ import annotations

from datetime import datetime, timezone
import json
import tempfile
import unittest
from pathlib import Path

from scripts.build_insights import normalize_insights_base_url
from scripts.space_track_insights import build_space_track_insights, write_insights_output


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
        self.assertEqual(len(insights["today"]["launches"]), 1)
        self.assertEqual(insights["today"]["launches"][0]["norad_cat_id"], 44713)
        self.assertEqual(len(insights["today"]["reentries"]), 1)
        self.assertEqual(insights["breakdowns"]["by_orbit"][0]["key"], "LEO")
        self.assertIn("active_vs_debris", insights["trends"])

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
            normalize_insights_base_url("https://satellite-catalog-mirror.example.workers.dev/insights"),
            "https://satellite-catalog-mirror.example.workers.dev/insights",
        )


if __name__ == "__main__":
    unittest.main()
