from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import requests


LAUNCH_LIBRARY_BASE_URL = "https://ll.thespacedevs.com/2.3.0"


def clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def nested_value(row: dict[str, Any], path: list[str]) -> Any:
    value: Any = row
    for key in path:
        if not isinstance(value, dict):
            return None
        value = value.get(key)
    return value


def normalize_launch(row: dict[str, Any]) -> dict[str, Any]:
    status = row.get("status")
    mission = row.get("mission")
    image = row.get("image")
    image_url = None
    if isinstance(image, dict):
        image_url = clean_string(image.get("image_url") or image.get("thumbnail_url"))
    else:
        image_url = clean_string(image)

    return {
        "id": str(row.get("id") or row.get("slug") or ""),
        "name": clean_string(row.get("name")) or "Unnamed launch",
        "window_start": clean_string(row.get("window_start")),
        "window_end": clean_string(row.get("window_end")),
        "status": clean_string(status.get("name") if isinstance(status, dict) else status),
        "provider": clean_string(nested_value(row, ["launch_service_provider", "name"])),
        "vehicle": clean_string(
            nested_value(row, ["rocket", "configuration", "full_name"])
            or nested_value(row, ["rocket", "configuration", "name"])
        ),
        "pad_name": clean_string(nested_value(row, ["pad", "name"])),
        "location_name": clean_string(nested_value(row, ["pad", "location", "name"])),
        "mission_name": clean_string(mission.get("name") if isinstance(mission, dict) else None),
        "mission_type": clean_string(mission.get("type") if isinstance(mission, dict) else None),
        "image_url": image_url,
    }


def parse_iso_datetime(value: Any) -> Optional[datetime]:
    text = clean_string(value)
    if not text:
        return None
    try:
        normalized = text.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def split_launch_sections(
    rows: list[dict[str, Any]],
    *,
    today: date,
    upcoming_limit: int = 25,
) -> dict[str, list[dict[str, Any]]]:
    normalized = [launch for launch in (normalize_launch(row) for row in rows) if launch["id"] and launch["window_start"]]
    normalized.sort(key=lambda item: (item["window_start"], item["id"]))

    today_launches: list[dict[str, Any]] = []
    upcoming_launches: list[dict[str, Any]] = []
    today_ids: set[str] = set()

    for launch in normalized:
        start = parse_iso_datetime(launch.get("window_start"))
        if start is None:
            continue
        if start.date() == today:
            today_launches.append(launch)
            today_ids.add(launch["id"])
        elif start.date() > today and launch["id"] not in today_ids:
            upcoming_launches.append(launch)

    return {
        "today": today_launches,
        "upcoming": upcoming_launches[:upcoming_limit],
    }


def cache_path(cache_dir: Path, start: date, end: date) -> Path:
    return cache_dir / f"launches_{start.isoformat()}_{end.isoformat()}.json"


def read_cached_launch_rows(cache_dir: Path, start: date, end: date, max_age_hours: int, now: datetime) -> list[dict[str, Any]] | None:
    path = cache_path(cache_dir, start, end)
    if not path.exists():
        return None
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    if now - modified > timedelta(hours=max_age_hours):
        return None
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, list) else None


def write_cached_launch_rows(cache_dir: Path, start: date, end: date, rows: list[dict[str, Any]]) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path(cache_dir, start, end).write_text(
        json.dumps(rows, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )


def launch_library_url(start: date, end: date, *, limit: int) -> str:
    query = urlencode(
        {
            "format": "json",
            "limit": str(limit),
            "ordering": "window_start",
            "window_start__gte": f"{start.isoformat()}T00:00:00Z",
            "window_start__lt": f"{end.isoformat()}T00:00:00Z",
        }
    )
    return f"{LAUNCH_LIBRARY_BASE_URL}/launches/upcoming/?{query}"


def fetch_launch_library_rows(start: date, end: date, *, timeout: int, limit: int = 50) -> list[dict[str, Any]]:
    response = requests.get(
        launch_library_url(start, end, limit=limit),
        timeout=timeout,
        headers={
            "Accept": "application/json, */*; q=0.8",
            "User-Agent": "SatelliteTracerInsightsBuilder/1.0",
        },
    )
    if response.status_code == 429:
        raise RuntimeError("Launch Library 2 returned HTTP 429; cadence is too aggressive")
    if response.status_code != 200:
        raise RuntimeError(f"Launch Library 2 query failed with HTTP {response.status_code}")
    parsed = response.json()
    results = parsed.get("results") if isinstance(parsed, dict) else None
    if not isinstance(results, list):
        raise RuntimeError("Launch Library 2 returned unexpected JSON shape")
    return [row for row in results if isinstance(row, dict)]


def load_or_fetch_launch_rows(
    *,
    cache_dir: Path,
    start: date,
    end: date,
    now: datetime,
    timeout: int,
    force_refresh: bool,
    cache_max_age_hours: int,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not force_refresh:
        cached = read_cached_launch_rows(cache_dir, start, end, cache_max_age_hours, now)
        if cached is not None:
            print(f"Using cached Launch Library 2 launches: {len(cached)} rows")
            return cached

    rows = fetch_launch_library_rows(start, end, timeout=timeout, limit=limit)
    write_cached_launch_rows(cache_dir, start, end, rows)
    print(f"Fetched Launch Library 2 launches: {len(rows)} rows")
    return rows
