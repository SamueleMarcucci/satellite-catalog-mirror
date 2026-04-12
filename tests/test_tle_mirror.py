import json
from datetime import datetime, timezone
import tempfile
import unittest
from pathlib import Path

from tle_mirror import (
    build_manifest,
    decode_alpha5_catalog_id,
    format_catalog,
    merge_catalogs,
    parse_tle_blocks,
    should_run_full_refresh,
    write_catalog_outputs,
)


OLDER_ISS = """ISS (OLDER)
1 25544U 98067A   26046.56961129  .00012237  00000+0  23255-3 0  9991
2 25544  51.6318 181.6982 0010992 101.7612 258.4610 15.48625638552905
"""

NEWER_ISS = """ISS (NEWER)
1 25544U 98067A   26047.56961129  .00012237  00000+0  23255-3 0  9992
2 25544  51.6318 181.6982 0010992 101.7612 258.4610 15.48625638552906
"""

HST = """HST
1 20580U 90037B   26046.42870873  .00008875  00000+0  29710-3 0  9990
2 20580  28.4670 191.8156 0001369 214.6582 145.3924 15.29118613769908
"""


class TLEMirrorTests(unittest.TestCase):
    def test_alpha5_decoding_matches_app_mapping(self):
        self.assertEqual(decode_alpha5_catalog_id("25544"), 25544)
        self.assertEqual(decode_alpha5_catalog_id("A0000"), 100000)
        self.assertEqual(decode_alpha5_catalog_id("H9999"), 179999)
        self.assertEqual(decode_alpha5_catalog_id("J0000"), 180000)
        self.assertEqual(decode_alpha5_catalog_id("Z9999"), 339999)

    def test_merge_full_and_delta_without_duplicate_norads(self):
        merged = merge_catalogs(OLDER_ISS, NEWER_ISS + "\n" + HST)
        self.assertEqual(sorted(merged), [20580, 25544])
        output = format_catalog(merged)
        self.assertEqual(output.count("25544"), 2)
        self.assertIn("ISS (NEWER)", output)
        self.assertNotIn("ISS (OLDER)", output)

    def test_preserves_newer_existing_block_when_update_is_older(self):
        merged = merge_catalogs(NEWER_ISS, OLDER_ISS)
        self.assertEqual(len(merged), 1)
        self.assertIn("ISS (NEWER)", format_catalog(merged))

    def test_manifest_and_outputs_are_stable(self):
        text = format_catalog(parse_tle_blocks(NEWER_ISS + HST))
        manifest = build_manifest(
            catalog_text=text,
            source_retrieved_at=datetime(2026, 4, 12, 18, 17, tzinfo=timezone.utc),
            query_kind="delta",
            public_base_url="https://example.test/catalog",
        )
        self.assertEqual(manifest["schema_version"], 1)
        self.assertEqual(manifest["object_count"], 2)
        self.assertEqual(manifest["catalog"]["path"], "current.3le")
        self.assertEqual(len(manifest["catalog"]["sha256"]), 64)
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp)
            write_catalog_outputs(out, text, manifest)
            self.assertTrue((out / "current.3le").exists())
            self.assertTrue((out / "current.3le.gz").exists())
            decoded = json.loads((out / "manifest.json").read_text())
            self.assertEqual(decoded["catalog"]["sha256"], manifest["catalog"]["sha256"])

    def test_full_refresh_when_existing_catalog_missing_or_too_small(self):
        now = datetime(2026, 4, 12, 18, 17, tzinfo=timezone.utc)
        self.assertTrue(should_run_full_refresh(None, force_full=False, now=now))
        self.assertTrue(should_run_full_refresh(NEWER_ISS, force_full=False, now=now))
        self.assertTrue(should_run_full_refresh(NEWER_ISS, force_full=True, now=now))


if __name__ == "__main__":
    unittest.main()
