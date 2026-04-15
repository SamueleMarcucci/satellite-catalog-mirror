#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import os
from pathlib import Path
import sys

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from satellite_snapshot import build_snapshot, build_snapshot_manifest, write_snapshot_outputs  # noqa: E402
from tle_mirror import utc_now  # noqa: E402
from scripts.mirror_spacetrack import MirrorConfig, env_value, r2_client  # noqa: E402


def load_snapshot_config(require_r2: bool) -> MirrorConfig:
    required = []
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
        identity="",
        password="",
        r2_account_id=env_value("R2_ACCOUNT_ID"),
        r2_access_key_id=env_value("R2_ACCESS_KEY_ID"),
        r2_secret_access_key=os.environ.get("R2_SECRET_ACCESS_KEY", ""),
        r2_bucket=env_value("R2_BUCKET"),
        public_catalog_base_url=normalize_snapshot_base_url(env_value("PUBLIC_SNAPSHOT_BASE_URL")),
        catalog_prefix=env_value("SNAPSHOT_PREFIX", "snapshots/"),
    )


def normalize_snapshot_base_url(value: str) -> str:
    base = value.strip().rstrip("/")
    if not base:
        return ""
    if not base.startswith(("http://", "https://")):
        base = f"https://{base}"
    if not base.endswith("/snapshots"):
        base = f"{base}/snapshots"
    return base


def catalog_url_from_env() -> str:
    explicit = env_value("MIRRORED_CATALOG_URL")
    if explicit:
        return explicit
    base = env_value("PUBLIC_CATALOG_BASE_URL")
    if not base:
        raise RuntimeError("Set MIRRORED_CATALOG_URL or PUBLIC_CATALOG_BASE_URL.")
    return f"{base.rstrip('/')}/current.3le.gz"


def read_catalog_from_url(url: str, timeout: int) -> str:
    response = requests.get(
        url,
        timeout=timeout,
        headers={
            "Accept": "application/gzip, text/plain, */*; q=0.8",
            "User-Agent": "SatelliteTracerSnapshotBuilder/1.0",
        },
    )
    if response.status_code != 200:
        raise RuntimeError(f"Catalog download failed with HTTP {response.status_code}: {url}")
    data = response.content
    if url.endswith(".gz"):
        data = gzip.decompress(data)
    return data.decode("utf-8")


def upload_outputs_to_r2(config: MirrorConfig, output_dir: Path) -> None:
    client = r2_client(config)
    uploads = [
        ("manifest.json", "application/json; charset=utf-8", None, "public, max-age=30"),
        ("current.json", "application/json; charset=utf-8", None, "public, max-age=60"),
        ("current.json.gz", "application/gzip", None, "public, max-age=60"),
    ]
    for filename, content_type, content_encoding, cache_control in uploads:
        extra_args = {
            "ContentType": content_type,
            "CacheControl": cache_control,
        }
        if content_encoding:
            extra_args["ContentEncoding"] = content_encoding
        client.upload_file(
            str(output_dir / filename),
            config.r2_bucket,
            config.r2_key(filename),
            ExtraArgs=extra_args,
        )
        print(f"Uploaded {config.r2_key(filename)}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Build server-side satellite position snapshots from mirrored TLEs.")
    parser.add_argument("--catalog-file", help="Local current.3le or current.3le.gz input. If omitted, downloads MIRRORED_CATALOG_URL.")
    parser.add_argument("--output-dir", default="build/snapshots", help="Directory for generated snapshot files.")
    parser.add_argument("--dry-run", action="store_true", help="Write outputs locally, but do not upload to R2.")
    parser.add_argument("--timeout", type=int, default=60, help="HTTP timeout for downloading the mirrored catalog.")
    args = parser.parse_args()

    config = load_snapshot_config(require_r2=not args.dry_run)
    output_dir = Path(args.output_dir)
    generated_at = utc_now()

    if args.catalog_file:
        path = Path(args.catalog_file)
        data = path.read_bytes()
        if path.suffix == ".gz":
            data = gzip.decompress(data)
        catalog_text = data.decode("utf-8")
    else:
        url = catalog_url_from_env()
        print(f"Downloading mirrored catalog from {url}")
        catalog_text = read_catalog_from_url(url, timeout=args.timeout)

    snapshot = build_snapshot(catalog_text, generated_at=generated_at)
    raw = __import__("json").dumps(snapshot, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if len(snapshot["satellites"]) < 15:
        raise RuntimeError(f"Refusing to publish suspiciously small snapshot: {len(snapshot['satellites'])} objects.")
    manifest = build_snapshot_manifest(
        snapshot_json=raw,
        generated_at=generated_at,
        public_base_url=config.public_catalog_base_url,
    )
    write_snapshot_outputs(output_dir, snapshot, manifest)
    print(f"Wrote {manifest['object_count']} snapshot objects to {output_dir}")

    if args.dry_run:
        print("Dry run complete; R2 upload skipped.")
    else:
        upload_outputs_to_r2(config, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
