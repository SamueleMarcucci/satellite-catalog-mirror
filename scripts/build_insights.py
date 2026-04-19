#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import gzip
import json
import os
from pathlib import Path
import sys
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts.mirror_spacetrack import (  # noqa: E402
    MirrorConfig,
    SPACE_TRACK_BASE_URL,
    env_value,
    r2_client,
    space_track_login,
)
from scripts.space_track_insights import (  # noqa: E402
    build_insights_manifest,
    build_space_track_insights,
    write_insights_output,
)
from tle_mirror import utc_now  # noqa: E402


SPACE_TRACK_JSON_QUERIES = {
    "gp": "/basicspacedata/query/class/gp/decay_date/null-val/epoch/%3Enow-10/orderby/NORAD_CAT_ID/format/json/emptyresult/show",
    "satcat": "/basicspacedata/query/class/satcat/orderby/NORAD_CAT_ID/format/json/emptyresult/show",
    "decay": "/basicspacedata/query/class/decay/orderby/DECAY_EPOCH%20desc/format/json/emptyresult/show",
    "satcat_debut": "/basicspacedata/query/class/satcat_debut/orderby/NORAD_CAT_ID%20desc/format/json/emptyresult/show",
}


def normalize_insights_base_url(value: str) -> str:
    base = value.strip().rstrip("/")
    if not base:
        return ""
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    for suffix in ("/catalog", "/snapshots", "/insights"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    if not base.endswith("/insights"):
        base = f"{base}/insights"
    return base


def load_insights_config(require_r2: bool) -> MirrorConfig:
    required = ["SPACE_TRACK_IDENTITY", "SPACE_TRACK_PASSWORD"]
    if require_r2:
        required += [
            "R2_ACCOUNT_ID",
            "R2_ACCESS_KEY_ID",
            "R2_SECRET_ACCESS_KEY",
            "R2_BUCKET",
        ]
    missing = [name for name in required if not env_value(name)]
    if missing:
        raise RuntimeError(f"Missing required environment variables: {', '.join(missing)}")
    return MirrorConfig(
        identity=env_value("SPACE_TRACK_IDENTITY"),
        password=os.environ["SPACE_TRACK_PASSWORD"],
        r2_account_id=env_value("R2_ACCOUNT_ID"),
        r2_access_key_id=env_value("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY", ""),
        r2_bucket=env_value("R2_BUCKET"),
        public_catalog_base_url=normalize_insights_base_url(
            env_value("PUBLIC_INSIGHTS_BASE_URL") or env_value("PUBLIC_CATALOG_BASE_URL")
        ),
        catalog_prefix=env_value("INSIGHTS_PREFIX", "insights/"),
    )


def cached_json_path(cache_dir: Path, class_name: str) -> Path:
    return cache_dir / f"{class_name}.json"


def read_cached_rows(cache_dir: Path, class_name: str, max_age_hours: int, now: datetime) -> list[dict[str, Any]] | None:
    path = cached_json_path(cache_dir, class_name)
    if not path.exists():
        return None
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    if now - modified > timedelta(hours=max_age_hours):
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_cached_rows(cache_dir: Path, class_name: str, rows: list[dict[str, Any]]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cached_json_path(cache_dir, class_name).write_text(
        json.dumps(rows, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )


def fetch_space_track_json(session: requests.Session, class_name: str, timeout: int) -> list[dict[str, Any]]:
    response = session.get(
        f"{SPACE_TRACK_BASE_URL}{SPACE_TRACK_JSON_QUERIES[class_name]}",
        timeout=timeout,
        headers={
            "Accept": "application/json, */*; q=0.8",
            "User-Agent": "SatelliteTracerInsightsBuilder/1.0",
        },
    )
    if response.status_code in (401, 403):
        raise RuntimeError(f"Space-Track rejected credentials for {class_name}")
    if response.status_code == 429:
        raise RuntimeError(f"Space-Track returned HTTP 429 for {class_name}; cadence is too aggressive")
    if response.status_code != 200:
        raise RuntimeError(f"Space-Track {class_name} query failed with HTTP {response.status_code}")
    text = response.text.strip()
    if text == "NO RESULTS RETURNED" or not text:
        return []
    parsed = response.json()
    if not isinstance(parsed, list):
        raise RuntimeError(f"Space-Track {class_name} returned unexpected JSON shape")
    return [row for row in parsed if isinstance(row, dict)]


def load_or_fetch_rows(
    *,
    session: requests.Session,
    class_name: str,
    cache_dir: Path,
    max_age_hours: int,
    now: datetime,
    timeout: int,
    force_refresh: bool,
) -> list[dict[str, Any]]:
    if not force_refresh:
        cached = read_cached_rows(cache_dir, class_name, max_age_hours, now)
        if cached is not None:
            print(f"Using cached Space-Track {class_name}: {len(cached)} rows")
            return cached

    rows = fetch_space_track_json(session, class_name, timeout)
    write_cached_rows(cache_dir, class_name, rows)
    print(f"Fetched Space-Track {class_name}: {len(rows)} rows")
    return rows


def upload_outputs_to_r2(config: MirrorConfig, output_dir: Path, manifest: dict[str, Any]) -> None:
    client = r2_client(config)
    raw = (output_dir / "current.json").read_bytes()
    (output_dir / "current.json.gz").write_bytes(gzip.compress(raw, compresslevel=9, mtime=0))
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    uploads = [
        ("manifest.json", "application/json; charset=utf-8", "public, max-age=300"),
        ("current.json", "application/json; charset=utf-8", "public, max-age=300"),
        ("current.json.gz", "application/gzip", "public, max-age=300"),
    ]
    for filename, content_type, cache_control in uploads:
        client.upload_file(
            str(output_dir / filename),
            config.r2_bucket,
            config.r2_key(filename),
            ExtraArgs={"ContentType": content_type, "CacheControl": cache_control},
        )
        print(f"Uploaded {config.r2_key(filename)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build app-ready Space-Track insights JSON.")
    parser.add_argument("--output-dir", default="public/insights", help="Directory for current.json.")
    parser.add_argument("--cache-dir", default="build/insights/cache", help="Directory for raw Space-Track response cache.")
    parser.add_argument("--cache-max-age-hours", type=int, default=23, help="Reuse raw Space-Track JSON while younger than this.")
    parser.add_argument("--dry-run", action="store_true", help="Write current.json locally, but do not upload to R2.")
    parser.add_argument("--force-refresh", action="store_true", help="Ignore cached Space-Track JSON.")
    parser.add_argument("--timeout", type=int, default=90, help="HTTP timeout per Space-Track request.")
    args = parser.parse_args()

    config = load_insights_config(require_r2=not args.dry_run)
    now = utc_now()
    session = requests.Session()
    space_track_login(session, config, timeout=args.timeout)

    fetched: dict[str, list[dict[str, Any]]] = {}
    for class_name in ("gp", "satcat", "decay", "satcat_debut"):
        try:
            fetched[class_name] = load_or_fetch_rows(
                session=session,
                class_name=class_name,
                cache_dir=Path(args.cache_dir),
                max_age_hours=args.cache_max_age_hours,
                now=now,
                timeout=args.timeout,
                force_refresh=args.force_refresh,
            )
        except Exception:
            if class_name != "satcat_debut":
                raise
            print("Space-Track satcat_debut unavailable; continuing without optional debut enrichment.")
            fetched[class_name] = []

    insights = build_space_track_insights(
        gp_rows=fetched["gp"],
        satcat_rows=fetched["satcat"],
        decay_rows=fetched["decay"],
        satcat_debut_rows=fetched["satcat_debut"],
        generated_at=now,
    )
    if insights["counts"]["gp"] < 15 or insights["counts"]["satcat"] < 15:
        raise RuntimeError(f"Refusing to publish suspiciously small insights input counts: {insights['counts']}")

    output_dir = Path(args.output_dir)
    raw = write_insights_output(output_dir, insights)
    manifest = build_insights_manifest(raw, now, config.public_catalog_base_url)

    print(f"Wrote Space-Track insights to {output_dir / 'current.json'}")
    print(
        "Summary: "
        f"gp={insights['counts']['gp']} "
        f"satcat={insights['counts']['satcat']} "
        f"decay={insights['counts']['decay']} "
        f"merged={insights['counts']['merged']}"
    )
    print("Fields used: NORAD_CAT_ID, OBJECT_NAME, OBJECT_ID, OBJECT_TYPE, OPS_STATUS_CODE, OWNER/COUNTRY, LAUNCH_DATE, DECAY_DATE/DECAY_EPOCH, PERIGEE/APOGEE, INCLINATION, MEAN_MOTION, ECCENTRICITY.")

    if args.dry_run:
        print("Dry run complete; R2 upload skipped.")
    else:
        upload_outputs_to_r2(config, output_dir, manifest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
