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
        # Launch Library often has both; some rows omit `window_start` but still carry `net`.
        "window_start": clean_string(row.get("window_start")) or clean_string(row.get("net")),
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
    now: datetime,
    recent_hours: int = 24,
    upcoming_limit: int = 25,
) -> dict[str, list[dict[str, Any]]]:
    """Split normalized launches into *recent* vs *future*.

    ``today`` in the insights JSON historically meant UTC calendar day, which missed any
    window that started **yesterday UTC** but still falls in the last 24 hours, and did not
    match how clients treat a rolling window. We now put ``window_start ∈ [now−24h, now]``
    (UTC) into ``today`` and everything strictly after ``now`` into ``upcoming``.
    """
    now_utc = now.astimezone(timezone.utc)
    cutoff = now_utc - timedelta(hours=recent_hours)

    normalized = [launch for launch in (normalize_launch(row) for row in rows) if launch["id"] and launch["window_start"]]
    normalized.sort(key=lambda item: (item["window_start"], item["id"]))

    today_launches: list[dict[str, Any]] = []
    upcoming_launches: list[dict[str, Any]] = []

    for launch in normalized:
        start = parse_iso_datetime(launch.get("window_start"))
        if start is None:
            continue
        if cutoff <= start <= now_utc:
            today_launches.append(launch)
        elif start > now_utc:
            upcoming_launches.append(launch)

    return {
        "today": today_launches,
        "upcoming": upcoming_launches[:upcoming_limit],
    }


def cache_path(cache_dir: Path, window_start_gte: datetime, window_start_lt: date, limit: int) -> Path:
    g = window_start_gte.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return cache_dir / f"launches_gte_{g}_lt_{window_start_lt.isoformat()}_{limit}.json"


def read_cached_launch_rows(
    cache_dir: Path,
    window_start_gte: datetime,
    window_start_lt: date,
    limit: int,
    max_age_hours: int,
    now: datetime,
) -> list[dict[str, Any]] | None:
    path = cache_path(cache_dir, window_start_gte, window_start_lt, limit)
    if not path.exists():
        return None
    modified = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    if now - modified > timedelta(hours=max_age_hours):
        return None
    parsed = json.loads(path.read_text(encoding="utf-8"))
    return parsed if isinstance(parsed, list) else None


def write_cached_launch_rows(
    cache_dir: Path,
    window_start_gte: datetime,
    window_start_lt: date,
    limit: int,
    rows: list[dict[str, Any]],
) -> None:
    cache_dir.mkdir(parents=True, exist_ok=True)
    cache_path(cache_dir, window_start_gte, window_start_lt, limit).write_text(
        json.dumps(rows, separators=(",", ":"), sort_keys=True),
        encoding="utf-8",
    )


def _format_query_instant_utc(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def launch_library_url(window_start_gte: datetime, window_start_lt: date, *, limit: int) -> str:
    query = urlencode(
        {
            "format": "json",
            "limit": str(limit),
            "ordering": "window_start",
            "window_start__gte": _format_query_instant_utc(window_start_gte),
            "window_start__lt": f"{window_start_lt.isoformat()}T00:00:00Z",
        }
    )
    return f"{LAUNCH_LIBRARY_BASE_URL}/launches/upcoming/?{query}"


def fetch_launch_library_rows(
    window_start_gte: datetime,
    window_start_lt: date,
    *,
    timeout: int,
    limit: int = 50,
) -> list[dict[str, Any]]:
    response = requests.get(
        launch_library_url(window_start_gte, window_start_lt, limit=limit),
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
    window_start_gte: datetime,
    window_start_lt: date,
    now: datetime,
    timeout: int,
    force_refresh: bool,
    cache_max_age_hours: int,
    limit: int = 50,
) -> list[dict[str, Any]]:
    if not force_refresh:
        cached = read_cached_launch_rows(
            cache_dir,
            window_start_gte,
            window_start_lt,
            limit,
            cache_max_age_hours,
            now,
        )
        if cached is not None:
            print(f"Using cached Launch Library 2 launches: {len(cached)} rows")
            return cached

    rows = fetch_launch_library_rows(window_start_gte, window_start_lt, timeout=timeout, limit=limit)
    write_cached_launch_rows(cache_dir, window_start_gte, window_start_lt, limit, rows)
    print(f"Fetched Launch Library 2 launches: {len(rows)} rows")
    return rows
