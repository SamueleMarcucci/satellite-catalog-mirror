from __future__ import annotations

from datetime import datetime, timezone
import gzip
import json
import tempfile
import unittest
from pathlib import Path

from satellite_snapshot import build_snapshot, build_snapshot_manifest, write_snapshot_outputs


ISS_TLE = """ISS (ZARYA)
1 25544U 98067A   26072.50000000  .00016717  00000+0  30585-3 0  9993
2 25544  51.6416 113.3381 0004037 120.0078 325.1456 15.50000000450000
"""


class SatelliteSnapshotTests(unittest.TestCase):
    def test_build_snapshot_matches_app_json_shape(self) -> None:
        snapshot = build_snapshot(
            ISS_TLE,
            generated_at=datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc),
        )

        self.assertEqual(snapshot["schema_version"], 1)
        self.assertEqual(snapshot["generated_at"], "2026-03-13T12:00:00Z")
        self.assertEqual(len(snapshot["satellites"]), 1)
        row = snapshot["satellites"][0]
        self.assertEqual(row["id"], "25544")
        self.assertEqual(row["name"], "ISS (ZARYA)")
        self.assertTrue(-90 <= row["lat"] <= 90)
        self.assertTrue(-180 <= row["lon"] <= 180)
        self.assertGreater(row["alt_km"], 100)
        self.assertGreater(row["speed_kms"], 1)
        self.assertTrue(0 <= row["heading_deg"] < 360)

    def test_manifest_and_outputs_are_stable(self) -> None:
        generated_at = datetime(2026, 3, 13, 12, 0, 0, tzinfo=timezone.utc)
        snapshot = build_snapshot(ISS_TLE, generated_at=generated_at)
        raw = json.dumps(snapshot, separators=(",", ":"), sort_keys=True).encode("utf-8")
        manifest = build_snapshot_manifest(
            snapshot_json=raw,
            generated_at=generated_at,
            public_base_url="https://example.test/snapshots",
        )

        self.assertEqual(manifest["object_count"], 1)
        self.assertEqual(manifest["snapshot"]["path"], "current.json")
        self.assertEqual(manifest["snapshot_gzip"]["path"], "current.json.gz")
        self.assertTrue(manifest["snapshot"]["url"].endswith("/current.json"))
        self.assertTrue(manifest["snapshot_gzip"]["url"].endswith("/current.json.gz"))

        with tempfile.TemporaryDirectory() as temp:
            output_dir = Path(temp)
            write_snapshot_outputs(output_dir, snapshot, manifest)
            self.assertTrue((output_dir / "current.json").exists())
            self.assertTrue((output_dir / "current.json.gz").exists())
            self.assertTrue((output_dir / "manifest.json").exists())
            decompressed = gzip.decompress((output_dir / "current.json.gz").read_bytes())
            self.assertEqual(decompressed, (output_dir / "current.json").read_bytes())


if __name__ == "__main__":
    unittest.main()
