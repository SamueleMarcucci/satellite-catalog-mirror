from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
import gzip
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Optional

from tle_mirror import isoformat_z, utc_now


EARTH_RADIUS_KM = 6378.137
EARTH_MU_KM3_S2 = 398600.4418
SECONDS_PER_DAY = 86400.0

INSIGHTS_HISTORY_SCHEMA_VERSION = 1
INSIGHTS_HISTORY_MAX_SNAPSHOTS_DEFAULT = 2500

CATEGORY_LABELS = {
    "payload": "Payload",
    "rocket_body": "Rocket Body",
    "debris": "Debris",
    "unknown": "Unknown",
}

COUNTRY_LABELS = {
    "AB": "Saudi Arabia",
    "ALG": "Algeria",
    "ARGN": "Argentina",
    "ASRA": "Austria",
    "AUS": "Australia",
    "AZER": "Azerbaijan",
    "BEL": "Belgium",
    "BRAZ": "Brazil",
    "US": "United States",
    "URY": "Uruguay",
    "PRC": "China",
    "CHN": "China",
    "CHBZ": "China/Brazil",
    "CIS": "Commonwealth of Independent States",
    "TBD": "Unassigned",
    "UNKNOWN": "Unknown",
    "UNK": "Unknown",
    "CA": "Canada",
    "CAN": "Canada",
    "CHLE": "Chile",
    "COL": "Colombia",
    "CZCH": "Czech Republic",
    "DEN": "Denmark",
    "ECU": "Ecuador",
    "EGYP": "Egypt",
    "FR": "France",
    "FRA": "France",
    "GER": "Germany",
    "DEU": "Germany",
    "GREC": "Greece",
    "IND": "India",
    "IDSA": "Indonesia",
    "IRAN": "Iran",
    "IRAQ": "Iraq",
    "ISRA": "Israel",
    "IT": "Italy",
    "ITLY": "Italy",
    "JPN": "Japan",
    "KAZ": "Kazakhstan",
    "LAOS": "Laos",
    "LTU": "Lithuania",
    "LUXE": "Luxembourg",
    "MALA": "Malaysia",
    "MEX": "Mexico",
    "NETH": "Netherlands",
    "NICO": "North Korea",
    "NIG": "Nigeria",
    "NOR": "Norway",
    "NZ": "New Zealand",
    "PAKI": "Pakistan",
    "PER": "Peru",
    "POL": "Poland",
    "POR": "Portugal",
    "UK": "United Kingdom",
    "GB": "United Kingdom",
    "SAFR": "South Africa",
    "SAUD": "Saudi Arabia",
    "SEAL": "Sea Launch",
    "SING": "Singapore",
    "SKOR": "South Korea",
    "SPN": "Spain",
    "STCT": "Singapore/Taiwan",
    "SWED": "Sweden",
    "SWTZ": "Switzerland",
    "THAI": "Thailand",
    "TURK": "Turkey",
    "UAE": "United Arab Emirates",
    "UKR": "Ukraine",
    "RUS": "Russia",
    "VENZ": "Venezuela",
    "VTNM": "Vietnam",
}


def parse_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except ValueError:
        return None


def parse_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def parse_date(value: Any) -> Optional[date]:
    if value is None:
        return None
    token = str(value).strip()[:10]
    if not token:
        return None
    try:
        return date.fromisoformat(token)
    except ValueError:
        return None


def clean_string(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def titleize_key(value: str) -> str:
    return " ".join(part.capitalize() for part in value.replace("_", " ").replace("-", " ").split())


def category_label(value: str) -> str:
    return CATEGORY_LABELS.get(value, titleize_key(value))


def country_label(value: Optional[str]) -> str:
    raw = clean_string(value)
    if raw is None:
        return "Unknown"
    return COUNTRY_LABELS.get(raw.upper(), "Unknown")


def is_known_country_key(value: Optional[str]) -> bool:
    raw = clean_string(value)
    return raw is not None and raw.upper() in COUNTRY_LABELS


def norad_id(row: dict[str, Any]) -> Optional[int]:
    return parse_int(row.get("NORAD_CAT_ID") or row.get("NORAD_CATID") or row.get("CATNR"))


def row_name(
    gp: Optional[dict[str, Any]],
    satcat: Optional[dict[str, Any]],
    decay: Optional[dict[str, Any]] = None,
) -> str:
    for row in (gp, satcat, decay):
        if not row:
            continue
        value = clean_string(row.get("OBJECT_NAME") or row.get("SATNAME") or row.get("OBJECT"))
        if value:
            return value
    return "Unknown object"


def semi_major_axis_km(mean_motion_rev_day: Optional[float]) -> Optional[float]:
    if not mean_motion_rev_day or mean_motion_rev_day <= 0:
        return None
    n_rad_s = mean_motion_rev_day * 2.0 * math.pi / SECONDS_PER_DAY
    return (EARTH_MU_KM3_S2 / (n_rad_s * n_rad_s)) ** (1.0 / 3.0)


def orbital_altitudes_km(
    gp: Optional[dict[str, Any]],
    satcat: Optional[dict[str, Any]],
) -> tuple[Optional[float], Optional[float], Optional[float]]:
    perigee = parse_float(satcat.get("PERIGEE") if satcat else None)
    apogee = parse_float(satcat.get("APOGEE") if satcat else None)
    if perigee is not None and apogee is not None:
        return perigee, apogee, (perigee + apogee) / 2.0

    mean_motion = parse_float(gp.get("MEAN_MOTION") if gp else None)
    eccentricity = parse_float(gp.get("ECCENTRICITY") if gp else None) or 0.0
    semi_major = semi_major_axis_km(mean_motion)
    if semi_major is None:
        return None, None, None
    perigee = max(0.0, semi_major * (1.0 - eccentricity) - EARTH_RADIUS_KM)
    apogee = max(0.0, semi_major * (1.0 + eccentricity) - EARTH_RADIUS_KM)
    return perigee, apogee, (perigee + apogee) / 2.0


def orbit_band(perigee_km: Optional[float], apogee_km: Optional[float], mean_altitude_km: Optional[float]) -> str:
    altitude = mean_altitude_km
    if altitude is None and perigee_km is not None and apogee_km is not None:
        altitude = (perigee_km + apogee_km) / 2.0
    if altitude is None:
        return "Unknown"
    if apogee_km is not None and perigee_km is not None and (apogee_km - perigee_km) > 12000:
        return "HEO"
    if altitude < 2000:
        return "LEO"
    if altitude < 30000:
        return "MEO"
    if 33000 <= altitude <= 39000:
        return "GEO"
    return "High Earth"


def normalized_category(gp: Optional[dict[str, Any]], satcat: Optional[dict[str, Any]]) -> str:
    object_type = clean_string(satcat.get("OBJECT_TYPE") if satcat else None)
    name = row_name(gp, satcat).upper()
    if object_type:
        lowered = object_type.lower()
        if "payload" in lowered:
            return "payload"
        if "rocket" in lowered:
            return "rocket_body"
        if "debris" in lowered:
            return "debris"
    if " DEB" in name or name.endswith("DEB") or "DEBRIS" in name:
        return "debris"
    if " R/B" in name or "ROCKET BODY" in name:
        return "rocket_body"
    return "payload"


def is_active_payload(gp: Optional[dict[str, Any]], satcat: Optional[dict[str, Any]]) -> bool:
    if normalized_category(gp, satcat) != "payload":
        return False
    status = clean_string(satcat.get("OPS_STATUS_CODE") if satcat else None)
    if status is None:
        return True
    return status in {"+", "P", "B", "S", "X"}


def family_name(name: str) -> str:
    words = [word for word in name.upper().replace("-", " ").replace("_", " ").split() if word]
    if not words:
        return "Unknown"
    return words[0]


def normalize_object(
    norad: int,
    gp: Optional[dict[str, Any]],
    satcat: Optional[dict[str, Any]],
    decay: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    perigee, apogee, mean_altitude = orbital_altitudes_km(gp, satcat)
    launch_date = parse_date((satcat or {}).get("LAUNCH_DATE") or (gp or {}).get("LAUNCH_DATE"))
    decay_date = parse_date((decay or {}).get("DECAY_EPOCH") or (decay or {}).get("DECAY_DATE") or (satcat or {}).get("DECAY_DATE"))
    inclination = parse_float((gp or {}).get("INCLINATION") or (satcat or {}).get("INCLINATION"))
    mean_motion = parse_float((gp or {}).get("MEAN_MOTION"))
    category_key = normalized_category(gp, satcat)
    country_key = clean_string((satcat or {}).get("COUNTRY") or (satcat or {}).get("OWNER")) or "Unknown"
    operator_key = clean_string((satcat or {}).get("OWNER") or (satcat or {}).get("COUNTRY")) or "Unknown"
    return {
        "norad_cat_id": norad,
        "name": row_name(gp, satcat, decay),
        "object_id": clean_string((satcat or {}).get("OBJECT_ID") or (gp or {}).get("OBJECT_ID") or (decay or {}).get("OBJECT_ID")),
        "category_key": category_key,
        "category": category_label(category_key),
        "country_key": country_key,
        "country": country_label(country_key),
        "operator_key": operator_key,
        "operator": country_label(operator_key),
        "launch_date": launch_date.isoformat() if launch_date else None,
        "decay_date": decay_date.isoformat() if decay_date else None,
        "orbit": {
            "band": orbit_band(perigee, apogee, mean_altitude),
            "perigee_km": round(perigee, 1) if perigee is not None else None,
            "apogee_km": round(apogee, 1) if apogee is not None else None,
            "mean_altitude_km": round(mean_altitude, 1) if mean_altitude is not None else None,
            "inclination_deg": round(inclination, 3) if inclination is not None else None,
            "mean_motion_rev_day": round(mean_motion, 8) if mean_motion is not None else None,
        },
    }


def top_counts(counter: Counter[str], *, limit: int = 12) -> list[dict[str, Any]]:
    return [{"key": key, "count": count} for key, count in counter.most_common(limit) if key and count > 0]


def labeled_top_counts(counter: Counter[str], *, labeler, limit: int = 12) -> list[dict[str, Any]]:
    return [
        {"key": key, "label": labeler(key), "count": count}
        for key, count in counter.most_common(limit)
        if key and count > 0
    ]


def country_top_counts(values: list[str], *, limit: int = 20) -> list[dict[str, Any]]:
    counter = Counter(value for value in values if is_known_country_key(value))
    return labeled_top_counts(counter, labeler=country_label, limit=limit)


def build_history_snapshot(
    *,
    generated_at: datetime,
    launches_today_count: int,
    reentries_today_count: int,
    active_payloads: int,
    debris: int,
    rocket_bodies: int,
    orbit_counter: Counter[str],
    biggest_constellation_name: str,
    biggest_constellation_count: int,
    family_counts: Counter[str],
) -> dict[str, Any]:
    by_orbit = {
        "leo": int(orbit_counter.get("LEO", 0)),
        "meo": int(orbit_counter.get("MEO", 0)),
        "geo": int(orbit_counter.get("GEO", 0)),
        "heo": int(orbit_counter.get("HEO", 0)),
        "high_earth": int(orbit_counter.get("High Earth", 0)),
        "unknown": int(orbit_counter.get("Unknown", 0)),
    }
    top5 = [
        {"name": name, "count": int(count)}
        for name, count in family_counts.most_common(5)
        if name and count > 0
    ]
    return {
        "timestamp": isoformat_z(generated_at),
        "launches_today_count": int(launches_today_count),
        "reentries_today_count": int(reentries_today_count),
        "active_payloads": int(active_payloads),
        "debris": int(debris),
        "rocket_bodies": int(rocket_bodies),
        "by_orbit": by_orbit,
        "biggest_constellation": {
            "name": biggest_constellation_name,
            "count": int(biggest_constellation_count),
        },
        "top_constellations": top5,
    }


def normalize_insights_history_document(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        return {"schema_version": INSIGHTS_HISTORY_SCHEMA_VERSION, "snapshots": []}
    raw_snaps = data.get("snapshots")
    snaps: list[dict[str, Any]] = []
    if isinstance(raw_snaps, list):
        for item in raw_snaps:
            if isinstance(item, dict) and item.get("timestamp"):
                snaps.append(item)
    snaps.sort(key=lambda s: str(s.get("timestamp") or ""))
    return {
        "schema_version": int(data.get("schema_version", INSIGHTS_HISTORY_SCHEMA_VERSION)),
        "snapshots": snaps,
    }


def merge_insights_history(
    existing: Optional[dict[str, Any]],
    snapshot: dict[str, Any],
    *,
    max_snapshots: int = INSIGHTS_HISTORY_MAX_SNAPSHOTS_DEFAULT,
) -> dict[str, Any]:
    base = normalize_insights_history_document(existing) if existing is not None else normalize_insights_history_document({})
    snapshots = list(base["snapshots"])
    snapshots.append(snapshot)
    snapshots.sort(key=lambda s: str(s.get("timestamp") or ""))
    if len(snapshots) > max_snapshots:
        snapshots = snapshots[-max_snapshots:]
    return {
        "schema_version": INSIGHTS_HISTORY_SCHEMA_VERSION,
        "snapshots": snapshots,
    }


def write_insights_history(output_dir: Path, history: dict[str, Any]) -> bytes:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(history, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    (output_dir / "history.json").write_bytes(raw)
    return raw


def has_valid_orbit_for_highlight(obj: dict[str, Any]) -> bool:
    orbit = obj.get("orbit") or {}
    perigee = orbit.get("perigee_km")
    apogee = orbit.get("apogee_km")
    mean_altitude = orbit.get("mean_altitude_km")
    if not isinstance(perigee, (int, float)) or not isinstance(apogee, (int, float)):
        return False
    if perigee <= 0 or apogee <= 0:
        return False
    if apogee < perigee:
        return False
    if mean_altitude is not None and (not isinstance(mean_altitude, (int, float)) or mean_altitude <= 0):
        return False
    return True


def newest_satellite(active_objects: list[dict[str, Any]], debut_rows: list[dict[str, Any]]) -> Optional[dict[str, Any]]:
    debut_by_norad = {value: row for row in debut_rows if (value := norad_id(row)) is not None}
    candidates: list[tuple[date, int, dict[str, Any]]] = []
    for obj in active_objects:
        debut = debut_by_norad.get(obj["norad_cat_id"])
        debut_date = parse_date((debut or {}).get("DEBUT") or (debut or {}).get("EPOCH") or (debut or {}).get("LAUNCH_DATE"))
        launch_date = parse_date(obj.get("launch_date"))
        sort_date = debut_date or launch_date
        if sort_date:
            enriched = dict(obj)
            enriched["debut_date"] = debut_date.isoformat() if debut_date else None
            candidates.append((sort_date, enriched["norad_cat_id"], enriched))
    if candidates:
        return max(candidates, key=lambda item: (item[0], item[1]))[2]
    return max(active_objects, key=lambda item: item["norad_cat_id"], default=None)


# MARK: - Deep dives (featured topics)

def _u(name: Optional[str]) -> str:
    return (name or "").upper()


def is_starlink_object(obj: dict[str, Any]) -> bool:
    """Conservative identification for Starlink: Space-Track names beginning with STARLINK."""
    name = _u(obj.get("name"))
    return name.startswith("STARLINK")


def is_gps_object(obj: dict[str, Any]) -> bool:
    """GPS / NAVSTAR payloads only (do not include other GNSS like Galileo/GLONASS)."""
    name = _u(obj.get("name"))
    return "NAVSTAR" in name or name.startswith("GPS") or " GPS " in f" {name} "


def is_weather_object(obj: dict[str, Any]) -> bool:
    """Meteorological satellites using strict name tokens for known weather families."""
    name = _u(obj.get("name"))
    tokens = (
        "NOAA",
        "GOES",
        "METOP",
        "METEOR",
        "HIMAWARI",
        "FENGYUN",
        "FY-",
        "FY ",
        "JPSS",
        "SUOMI NPP",
        "TERRA",
        "AQUA",
        "SENTINEL-3",
        "CLOUDSAT",
        "CALIPSO",
    )
    return any(t in name for t in tokens)


def is_station_object(obj: dict[str, Any]) -> bool:
    """Human spaceflight / station complexes and core modules by name."""
    name = _u(obj.get("name"))
    tokens = (
        "ISS",
        "ZARYA",
        "ZVEZDA",
        "NAUKA",
        "PRICHAL",
        "TIANGONG",
        "TIANHE",
        "WENTIAN",
        "MENGTIAN",
        "MIR",
        "SKYLAB",
        "SALYUT",
    )
    return any(t in name for t in tokens)


def _topic_objects(objects: list[dict[str, Any]], predicate) -> list[dict[str, Any]]:
    return [obj for obj in objects if obj.get("category_key") == "payload" and predicate(obj)]


def _pct(n: int, d: int) -> int:
    if d <= 0:
        return 0
    return int(round(100.0 * n / d))


def growth_line_from_launch_dates(
    objects: list[dict[str, Any]],
    *,
    max_points: int = 36,
) -> list[dict[str, Any]]:
    """Cumulative growth line grounded in Space-Track `LAUNCH_DATE` (no fabricated history)."""
    dates = [parse_date(obj.get("launch_date")) for obj in objects]
    dates = [d for d in dates if d is not None]
    if not dates:
        return []
    start = min(dates)
    end = max(dates)
    span_days = (end - start).days

    if span_days <= 90:
        bucket = "day"
    elif span_days <= 365 * 3:
        bucket = "month"
    else:
        bucket = "year"

    def bucket_key(d: date) -> date:
        if bucket == "day":
            return d
        if bucket == "month":
            return date(d.year, d.month, 1)
        return date(d.year, 1, 1)

    counts: Counter[date] = Counter(bucket_key(d) for d in dates)
    keys = sorted(counts)
    # Downsample deterministically if too many buckets: keep the most recent N buckets.
    if len(keys) > max_points:
        keys = keys[-max_points:]

    cumulative = 0
    out: list[dict[str, Any]] = []
    for k in keys:
        cumulative += counts[k]
        out.append({"date": k.isoformat(), "count": cumulative})
    return out


def deep_dives_section(
    *,
    objects: list[dict[str, Any]],
    active_objects: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    total_objects = len(objects)
    total_payloads = sum(1 for o in objects if o.get("category_key") == "payload")

    starlink = _topic_objects(objects, is_starlink_object)
    gps = _topic_objects(objects, is_gps_object)
    weather = _topic_objects(objects, is_weather_object)
    stations = _topic_objects(objects, is_station_object)
    debris = [o for o in objects if o.get("category_key") == "debris"]

    active_by_norad = {o["norad_cat_id"] for o in active_objects}

    def orbit_band_share(objs: list[dict[str, Any]], band: str) -> int:
        return _pct(sum(1 for o in objs if (o.get("orbit") or {}).get("band") == band), len(objs))

    def newest_launch(objs: list[dict[str, Any]]) -> Optional[str]:
        d = [parse_date(o.get("launch_date")) for o in objs]
        d = [x for x in d if x is not None]
        return max(d).isoformat() if d else None

    def active_share(objs: list[dict[str, Any]]) -> int:
        return _pct(sum(1 for o in objs if o.get("norad_cat_id") in active_by_norad), len(objs))

    out: list[dict[str, Any]] = []

    out.append(
        {
            "id": "starlink",
            "title": "Starlink",
            "short_description": f"Starlink accounts for {len(starlink):,} tracked payloads in this catalog snapshot.",
            "total_count": len(starlink),
            "growth_line": growth_line_from_launch_dates(starlink),
            "key_facts": list(
                filter(
                    None,
                    [
                        f"{orbit_band_share(starlink, 'LEO')}% are in LEO." if starlink else None,
                        f"{active_share(starlink)}% are currently active (Space-Track gp)." if starlink else None,
                    ],
                )
            )[:2],
        }
    )

    out.append(
        {
            "id": "gps",
            "title": "GPS",
            "short_description": f"GPS/NAVSTAR satellites total {len(gps):,} payloads in this snapshot.",
            "total_count": len(gps),
            "growth_line": growth_line_from_launch_dates(gps),
            "key_facts": list(
                filter(
                    None,
                    [
                        f"Newest GPS launch: {newest_launch(gps)}." if newest_launch(gps) else None,
                        f"{orbit_band_share(gps, 'MEO')}% are in MEO." if gps else None,
                    ],
                )
            )[:2],
        }
    )

    out.append(
        {
            "id": "weather",
            "title": "Weather satellites",
            "short_description": f"Weather and meteorology missions total {len(weather):,} payloads in this snapshot.",
            "total_count": len(weather),
            "growth_line": growth_line_from_launch_dates(weather),
            "key_facts": list(
                filter(
                    None,
                    [
                        f"Newest weather launch: {newest_launch(weather)}." if newest_launch(weather) else None,
                        f"{orbit_band_share(weather, 'LEO')}% are in LEO." if weather else None,
                    ],
                )
            )[:2],
        }
    )

    out.append(
        {
            "id": "stations",
            "title": "Stations",
            "short_description": f"Station-class objects total {len(stations):,} payloads in this snapshot.",
            "total_count": len(stations),
            "growth_line": growth_line_from_launch_dates(stations),
            "key_facts": list(
                filter(
                    None,
                    [
                        f"{orbit_band_share(stations, 'LEO')}% are in LEO." if stations else None,
                        f"Newest station launch: {newest_launch(stations)}." if newest_launch(stations) else None,
                    ],
                )
            )[:2],
        }
    )

    debris_pct = _pct(len(debris), total_objects)
    out.append(
        {
            "id": "debris",
            "title": "Debris problem",
            "short_description": f"Debris totals {len(debris):,} objects ({debris_pct}% of {total_objects:,} tracked).",
            "total_count": len(debris),
            "growth_line": growth_line_from_launch_dates(debris),
            "key_facts": list(
                filter(
                    None,
                    [
                        f"{orbit_band_share(debris, 'LEO')}% of debris is in LEO." if debris else None,
                        f"Payloads vs debris: {total_payloads:,} payloads, {len(debris):,} debris objects.",
                    ],
                )
            )[:2],
        }
    )

    return out

def build_space_track_insights(
    *,
    gp_rows: list[dict[str, Any]],
    satcat_rows: list[dict[str, Any]],
    decay_rows: list[dict[str, Any]],
    satcat_debut_rows: Optional[list[dict[str, Any]]] = None,
    today_launches: Optional[list[dict[str, Any]]] = None,
    upcoming_launches: Optional[list[dict[str, Any]]] = None,
    generated_at: Optional[datetime] = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    generated_at = generated_at or utc_now()
    today = generated_at.astimezone(timezone.utc).date()

    gp_by_norad = {value: row for row in gp_rows if (value := norad_id(row)) is not None}
    satcat_by_norad = {value: row for row in satcat_rows if (value := norad_id(row)) is not None}
    decay_by_norad = {value: row for row in decay_rows if (value := norad_id(row)) is not None}

    all_ids = sorted(set(gp_by_norad) | set(satcat_by_norad))
    objects = [
        normalize_object(norad, gp_by_norad.get(norad), satcat_by_norad.get(norad), decay_by_norad.get(norad))
        for norad in all_ids
    ]
    active_objects = [
        obj for obj in objects
        if obj["norad_cat_id"] in gp_by_norad
        and is_active_payload(gp_by_norad.get(obj["norad_cat_id"]), satcat_by_norad.get(obj["norad_cat_id"]))
    ]

    reentries_today = [
        normalize_object(norad, gp_by_norad.get(norad), satcat_by_norad.get(norad), row)
        for norad, row in decay_by_norad.items()
        if parse_date(row.get("DECAY_EPOCH") or row.get("DECAY_DATE")) == today
    ]
    upcoming_reentries = [
        normalize_object(norad, gp_by_norad.get(norad), satcat_by_norad.get(norad), row)
        for norad, row in decay_by_norad.items()
        if (decay_date := parse_date(row.get("DECAY_EPOCH") or row.get("DECAY_DATE"))) is not None and decay_date >= today
    ]
    upcoming_reentries.sort(key=lambda item: (item.get("decay_date") or "9999-12-31", item["norad_cat_id"]))

    family_counts = Counter(family_name(obj["name"]) for obj in active_objects)
    biggest_family, biggest_count = family_counts.most_common(1)[0] if family_counts else ("Unknown", 0)
    by_orbit = top_counts(Counter(obj["orbit"]["band"] for obj in objects), limit=8)

    launches_by_year: Counter[str] = Counter()
    for obj in objects:
        if obj.get("launch_date"):
            launches_by_year[obj["launch_date"][:4]] += 1
    recent_years = sorted(launches_by_year)[-12:]

    highest = max(
        (obj for obj in objects if has_valid_orbit_for_highlight(obj)),
        key=lambda item: item["orbit"]["apogee_km"],
        default=None,
    )
    lowest_active = min(
        (obj for obj in active_objects if has_valid_orbit_for_highlight(obj)),
        key=lambda item: item["orbit"]["perigee_km"],
        default=None,
    )

    debris_count = sum(1 for obj in objects if obj["category_key"] == "debris")
    rocket_bodies_count = sum(1 for obj in objects if obj["category_key"] == "rocket_body")
    orbit_counter = Counter(obj["orbit"]["band"] for obj in objects)
    launches_today_count = len(today_launches or [])

    history_snapshot = build_history_snapshot(
        generated_at=generated_at,
        launches_today_count=launches_today_count,
        reentries_today_count=len(reentries_today),
        active_payloads=len(active_objects),
        debris=debris_count,
        rocket_bodies=rocket_bodies_count,
        orbit_counter=orbit_counter,
        biggest_constellation_name=biggest_family,
        biggest_constellation_count=int(biggest_count),
        family_counts=family_counts,
    )

    insights: dict[str, Any] = {
        "schema_version": 1,
        "last_updated": isoformat_z(generated_at),
        "source": {
            "name": "Space-Track.org",
            "classes": ["gp", "satcat", "decay", "satcat_debut"],
            "join_key": "NORAD_CAT_ID",
        },
        "today": {
            "launches": today_launches or [],
            "reentries": sorted(reentries_today, key=lambda item: item["norad_cat_id"]),
        },
        "upcoming": {
            "launches": upcoming_launches or [],
            "reentries": upcoming_reentries[:25],
        },
        "highlights": {
            "biggest_constellation": {
                "name": biggest_family,
                "count": biggest_count,
                "basis": "active payload family inferred from Space-Track object names",
            },
            "newest_satellite": newest_satellite(active_objects, satcat_debut_rows or []),
            "highest_orbit": highest,
            "lowest_active_orbit": lowest_active,
        },
        "deep_dives": deep_dives_section(objects=objects, active_objects=active_objects),
        "breakdowns": {
            "by_orbit": by_orbit,
            "by_category": labeled_top_counts(Counter(obj["category_key"] for obj in objects), labeler=category_label, limit=8),
            "by_country": country_top_counts([obj.get("country_key") or "Unknown" for obj in objects], limit=20),
            "by_operator": [],
        },
        "trends": {
            "launches_over_time": [{"year": year, "count": launches_by_year[year]} for year in recent_years],
            "active_vs_debris": [
                {"key": "active_payloads", "label": "Active Payloads", "count": len(active_objects)},
                {"key": "debris", "label": "Debris", "count": debris_count},
                {"key": "rocket_bodies", "label": "Rocket Bodies", "count": rocket_bodies_count},
            ],
            "busiest_orbit_band": by_orbit[:5],
        },
        "counts": {
            "gp": len(gp_by_norad),
            "satcat": len(satcat_by_norad),
            "decay": len(decay_by_norad),
            "merged": len(objects),
        },
    }
    return insights, history_snapshot


def write_insights_output(output_dir: Path, insights: dict[str, Any]) -> bytes:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(insights, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    (output_dir / "current.json").write_bytes(raw)
    return raw


def build_insights_manifest(
    raw: bytes,
    generated_at: datetime,
    public_base_url: str,
    *,
    history_raw: Optional[bytes] = None,
) -> dict[str, Any]:
    gz = gzip.compress(raw, compresslevel=9, mtime=0)
    base = public_base_url.rstrip("/")
    manifest: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": isoformat_z(generated_at),
        "manifest": {
            "path": "manifest.json",
            "url": f"{base}/manifest.json" if base else None,
            "content_type": "application/json; charset=utf-8",
        },
        "insights": {
            "path": "current.json",
            "url": f"{base}/current.json" if base else None,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
            "content_type": "application/json; charset=utf-8",
        },
        "insights_gzip": {
            "path": "current.json.gz",
            "url": f"{base}/current.json.gz" if base else None,
            "sha256": hashlib.sha256(gz).hexdigest(),
            "bytes": len(gz),
            "content_type": "application/gzip",
        },
    }
    if history_raw is not None:
        hgz = gzip.compress(history_raw, compresslevel=9, mtime=0)
        manifest["insights_history"] = {
            "path": "history.json",
            "url": f"{base}/history.json" if base else None,
            "sha256": hashlib.sha256(history_raw).hexdigest(),
            "bytes": len(history_raw),
            "content_type": "application/json; charset=utf-8",
        }
        manifest["insights_history_gzip"] = {
            "path": "history.json.gz",
            "url": f"{base}/history.json.gz" if base else None,
            "sha256": hashlib.sha256(hgz).hexdigest(),
            "bytes": len(hgz),
            "content_type": "application/gzip",
        }
    return manifest
