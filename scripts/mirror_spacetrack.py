#!/usr/bin/env python3
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import timezone
import os
from pathlib import Path
import sys
from typing import Optional

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from tle_mirror import (  # noqa: E402
    build_manifest,
    format_catalog,
    merge_catalogs,
    parse_tle_blocks,
    should_run_full_refresh,
    utc_now,
    write_catalog_outputs,
)


SPACE_TRACK_BASE_URL = "https://www.space-track.org"
FULL_GP_PATH = "/basicspacedata/query/class/gp/decay_date/null-val/epoch/%3Enow-10/orderby/NORAD_CAT_ID/format/3le/emptyresult/show"
DELTA_GP_PATH = "/basicspacedata/query/class/gp/decay_date/null-val/CREATION_DATE/%3Enow-0.042/orderby/NORAD_CAT_ID/format/3le/emptyresult/show"


@dataclass(frozen=True)
class MirrorConfig:
    identity: str
    password: str
    r2_account_id: str
    r2_access_key_id: str
    r2_secret_access_key: str
    r2_bucket: str
    public_catalog_base_url: str
    catalog_prefix: str = "catalog/"

    @property
    def r2_endpoint_url(self) -> str:
        return f"https://{self.r2_account_id}.r2.cloudflarestorage.com"

    def r2_key(self, filename: str) -> str:
        return f"{self.catalog_prefix.rstrip('/')}/{filename}"


def env_value(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def load_config(require_r2: bool) -> MirrorConfig:
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
        public_catalog_base_url=env_value("PUBLIC_CATALOG_BASE_URL"),
        catalog_prefix=env_value("CATALOG_PREFIX", "catalog/"),
    )


def space_track_login(session: requests.Session, config: MirrorConfig, timeout: int) -> None:
    response = session.post(
        f"{SPACE_TRACK_BASE_URL}/ajaxauth/login",
        data={"identity": config.identity, "password": config.password},
        timeout=timeout,
        headers={"User-Agent": "SatelliteTracerCatalogMirror/1.0"},
    )
    if response.status_code != 200 or not session.cookies:
        raise RuntimeError(f"Space-Track login failed with HTTP {response.status_code}")


def fetch_space_track_gp(session: requests.Session, query_kind: str, timeout: int) -> str:
    path = FULL_GP_PATH if query_kind == "full" else DELTA_GP_PATH
    response = session.get(
        f"{SPACE_TRACK_BASE_URL}{path}",
        timeout=timeout,
        headers={
            "Accept": "text/plain, */*; q=0.8",
            "User-Agent": "SatelliteTracerCatalogMirror/1.0",
        },
    )
    if response.status_code in (401, 403):
        raise RuntimeError("Space-Track rejected the credentials for the GP query")
    if response.status_code == 429:
        raise RuntimeError("Space-Track returned HTTP 429; cadence is too aggressive")
    if response.status_code != 200:
        raise RuntimeError(f"Space-Track GP query failed with HTTP {response.status_code}")
    text = response.text.strip()
    if text == "NO RESULTS RETURNED":
        return ""
    return response.text


def r2_client(config: MirrorConfig):
    import boto3

    return boto3.client(
        "s3",
        endpoint_url=config.r2_endpoint_url,
        aws_access_key_id=config.r2_access_key_id,
        aws_secret_access_key=config.r2_secret_access_key,
        region_name="auto",
    )


def read_existing_catalog_from_r2(config: MirrorConfig) -> Optional[str]:
    client = r2_client(config)
    try:
        obj = client.get_object(Bucket=config.r2_bucket, Key=config.r2_key("current.3le"))
    except Exception as error:
        print(f"No existing R2 catalog loaded ({error.__class__.__name__}). A full refresh may be needed.")
        return None
    body = obj["Body"].read()
    return body.decode("utf-8") if body else None


def upload_outputs_to_r2(config: MirrorConfig, output_dir: Path) -> None:
    client = r2_client(config)
    uploads = [
        ("manifest.json", "application/json; charset=utf-8", None),
        ("current.3le", "text/plain; charset=utf-8", None),
        ("current.3le.gz", "application/gzip", None),
    ]
    for filename, content_type, content_encoding in uploads:
        extra_args = {
            "ContentType": content_type,
            "CacheControl": "public, max-age=300" if filename == "manifest.json" else "public, max-age=3600",
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


def env_force_full(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "y"}


def main() -> int:
    parser = argparse.ArgumentParser(description="Mirror Space-Track GP TLE data to Cloudflare R2.")
    parser.add_argument("--output-dir", default="build/catalog", help="Directory for generated manifest/catalog files.")
    parser.add_argument("--dry-run", action="store_true", help="Fetch and write outputs locally, but do not upload to R2.")
    parser.add_argument("--force-full", action="store_true", help="Force a full Space-Track GP query.")
    parser.add_argument("--timeout", type=int, default=90, help="HTTP timeout per Space-Track request.")
    args = parser.parse_args()

    config = load_config(require_r2=not args.dry_run)
    output_dir = Path(args.output_dir)
    now = utc_now()
    existing_text: Optional[str] = None

    if not args.dry_run:
        existing_text = read_existing_catalog_from_r2(config)
    elif (output_dir / "current.3le").exists():
        existing_text = (output_dir / "current.3le").read_text(encoding="utf-8")

    force_full = args.force_full or env_force_full(env_value("FORCE_FULL"))
    query_kind = "full" if should_run_full_refresh(existing_text, force_full=force_full, now=now) else "delta"
    print(f"Running Space-Track {query_kind} GP query")

    session = requests.Session()
    space_track_login(session, config, timeout=args.timeout)
    fetched_text = fetch_space_track_gp(session, query_kind=query_kind, timeout=args.timeout)

    if not fetched_text and not existing_text:
        raise RuntimeError("Space-Track returned no catalog data and no previous R2 catalog exists.")

    merged = merge_catalogs(existing_text or "", fetched_text)
    if len(merged) < 15:
        raise RuntimeError(f"Refusing to publish suspiciously small catalog: {len(merged)} objects.")
    catalog_text = format_catalog(merged)
    manifest = build_manifest(
        catalog_text=catalog_text,
        source_retrieved_at=now.astimezone(timezone.utc),
        query_kind=query_kind,
        public_base_url=config.public_catalog_base_url,
    )
    write_catalog_outputs(output_dir, catalog_text, manifest)
    print(f"Wrote {manifest['object_count']} catalog objects to {output_dir}")

    if args.dry_run:
        print("Dry run complete; R2 upload skipped.")
    else:
        upload_outputs_to_r2(config, output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
