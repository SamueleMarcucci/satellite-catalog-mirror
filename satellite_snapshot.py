from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import gzip
import json
import math
from pathlib import Path
from typing import Iterable, Optional

from sgp4.api import Satrec, jday

from tle_mirror import TLEBlock, isoformat_z, parse_tle_blocks, sha256_hex, utc_now


EARTH_EQUATORIAL_RADIUS_KM = 6378.137
EARTH_FLATTENING = 1.0 / 298.257223563
EARTH_ROTATION_RAD_PER_SEC = 7.2921150e-5


@dataclass(frozen=True)
class SnapshotRow:
    id: str
    name: str
    lat: float
    lon: float
    alt_km: float
    speed_kms: float
    heading_deg: float

    def as_json(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "alt_km": round(self.alt_km, 3),
            "speed_kms": round(self.speed_kms, 5),
            "heading_deg": round(self.heading_deg, 2),
        }


def tle_name(block: TLEBlock) -> str:
    if len(block.lines) == 3:
        return block.lines[0].strip().lstrip("0 ").strip() or f"SAT {block.norad_id}"
    return f"SAT {block.norad_id}"


def gmst_rad(value: datetime) -> float:
    jd, fr = jday(
        value.year,
        value.month,
        value.day,
        value.hour,
        value.minute,
        value.second + value.microsecond / 1_000_000,
    )
    t_ut1 = ((jd - 2451545.0) + fr) / 36525.0
    seconds = (
        67310.54841
        + (876600.0 * 3600 + 8640184.812866) * t_ut1
        + 0.093104 * t_ut1 * t_ut1
        - 6.2e-6 * t_ut1 * t_ut1 * t_ut1
    )
    return math.radians((seconds / 240.0) % 360.0)


def rotate_z(vector: tuple[float, float, float], angle_rad: float) -> tuple[float, float, float]:
    x, y, z = vector
    c = math.cos(angle_rad)
    s = math.sin(angle_rad)
    return (c * x + s * y, -s * x + c * y, z)


def ecef_to_geodetic_km(ecef: tuple[float, float, float]) -> tuple[float, float, float]:
    x, y, z = ecef
    a = EARTH_EQUATORIAL_RADIUS_KM
    f = EARTH_FLATTENING
    b = a * (1.0 - f)
    e2 = 1.0 - (b * b) / (a * a)
    ep2 = (a * a - b * b) / (b * b)
    p = math.hypot(x, y)
    lon = math.atan2(y, x)
    theta = math.atan2(z * a, p * b)
    sin_theta = math.sin(theta)
    cos_theta = math.cos(theta)
    lat = math.atan2(
        z + ep2 * b * sin_theta * sin_theta * sin_theta,
        p - e2 * a * cos_theta * cos_theta * cos_theta,
    )
    sin_lat = math.sin(lat)
    n = a / math.sqrt(1.0 - e2 * sin_lat * sin_lat)
    alt = p / math.cos(lat) - n
    return (math.degrees(lat), normalize_lon_deg(math.degrees(lon)), alt)


def normalize_lon_deg(value: float) -> float:
    normalized = ((value + 180.0) % 360.0) - 180.0
    return 180.0 if normalized == -180.0 else normalized


def heading_from_velocity(
    lat_rad: float,
    lon_rad: float,
    ecef_velocity: tuple[float, float, float],
) -> float:
    vx, vy, vz = ecef_velocity
    east = (-math.sin(lon_rad), math.cos(lon_rad), 0.0)
    north = (
        -math.sin(lat_rad) * math.cos(lon_rad),
        -math.sin(lat_rad) * math.sin(lon_rad),
        math.cos(lat_rad),
    )
    east_v = vx * east[0] + vy * east[1] + vz * east[2]
    north_v = vx * north[0] + vy * north[1] + vz * north[2]
    return (math.degrees(math.atan2(east_v, north_v)) + 360.0) % 360.0


def propagate_block(block: TLEBlock, at: datetime) -> Optional[SnapshotRow]:
    if len(block.lines) == 3:
        line1, line2 = block.lines[1], block.lines[2]
    elif len(block.lines) == 2:
        line1, line2 = block.lines[0], block.lines[1]
    else:
        return None

    sat = Satrec.twoline2rv(line1, line2)
    jd, fr = jday(
        at.year,
        at.month,
        at.day,
        at.hour,
        at.minute,
        at.second + at.microsecond / 1_000_000,
    )
    error, teme_position, teme_velocity = sat.sgp4(jd, fr)
    if error != 0:
        return None

    theta = gmst_rad(at)
    ecef_position = rotate_z(teme_position, theta)
    rotated_velocity = rotate_z(teme_velocity, theta)
    earth_rotation_cross_r = (
        -EARTH_ROTATION_RAD_PER_SEC * ecef_position[1],
        EARTH_ROTATION_RAD_PER_SEC * ecef_position[0],
        0.0,
    )
    ecef_velocity = (
        rotated_velocity[0] - earth_rotation_cross_r[0],
        rotated_velocity[1] - earth_rotation_cross_r[1],
        rotated_velocity[2] - earth_rotation_cross_r[2],
    )
    lat, lon, alt = ecef_to_geodetic_km(ecef_position)
    speed = math.sqrt(sum(v * v for v in ecef_velocity))
    heading = heading_from_velocity(math.radians(lat), math.radians(lon), ecef_velocity)
    if not all(math.isfinite(v) for v in (lat, lon, alt, speed, heading)):
        return None
    return SnapshotRow(
        id=str(block.norad_id),
        name=tle_name(block),
        lat=lat,
        lon=lon,
        alt_km=alt,
        speed_kms=speed,
        heading_deg=heading,
    )


def build_snapshot(catalog_text: str, *, generated_at: Optional[datetime] = None) -> dict:
    generated = (generated_at or utc_now()).astimezone(timezone.utc).replace(microsecond=0)
    rows = []
    for block in parse_tle_blocks(catalog_text).values():
        row = propagate_block(block, generated)
        if row is not None:
            rows.append(row)
    rows.sort(key=lambda row: int(row.id))
    return {
        "schema_version": 1,
        "generated_at": isoformat_z(generated),
        "source": "Satellite TLE mirror server snapshot",
        "propagator": "SGP4",
        "satellites": [row.as_json() for row in rows],
    }


def build_snapshot_manifest(
    *,
    snapshot_json: bytes,
    generated_at: datetime,
    public_base_url: str,
) -> dict:
    gz = gzip.compress(snapshot_json, compresslevel=9, mtime=0)
    parsed = json.loads(snapshot_json.decode("utf-8"))
    base = public_base_url.rstrip("/")
    return {
        "schema_version": 1,
        "source": "Satellite TLE mirror server snapshot",
        "generated_at": isoformat_z(generated_at),
        "object_count": len(parsed.get("satellites", [])),
        "snapshot": {
            "path": "current.json",
            "url": f"{base}/current.json" if base else None,
            "sha256": sha256_hex(snapshot_json),
            "bytes": len(snapshot_json),
            "content_type": "application/json; charset=utf-8",
        },
        "snapshot_gzip": {
            "path": "current.json.gz",
            "url": f"{base}/current.json.gz" if base else None,
            "sha256": sha256_hex(gz),
            "bytes": len(gz),
            "content_type": "application/gzip",
        },
    }


def write_snapshot_outputs(output_dir: Path, snapshot: dict, manifest: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw = json.dumps(snapshot, separators=(",", ":"), sort_keys=True).encode("utf-8")
    (output_dir / "current.json").write_bytes(raw)
    (output_dir / "current.json.gz").write_bytes(gzip.compress(raw, compresslevel=9, mtime=0))
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
