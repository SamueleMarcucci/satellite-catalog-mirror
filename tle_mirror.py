from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import json
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple


@dataclass(frozen=True)
class TLEBlock:
    norad_id: int
    lines: Tuple[str, ...]
    epoch: Optional[datetime]

    @property
    def text(self) -> str:
        return "\n".join(self.lines)


def utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def isoformat_z(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def decode_alpha5_catalog_id(field: str) -> Optional[int]:
    last_five = ("00000" + field.strip().upper())[-5:]
    if len(last_five) != 5:
        return None
    first = last_five[0]
    suffix = last_five[1:]
    if not suffix.isdigit():
        return None
    seq_no = int(suffix)
    if first.isdigit():
        value = int(first) * 10000 + seq_no
    elif "A" <= first <= "H":
        value = (ord(first) - 55) * 10000 + seq_no
    elif "J" <= first <= "N":
        value = (ord(first) - 56) * 10000 + seq_no
    elif "P" <= first <= "Z":
        value = (ord(first) - 57) * 10000 + seq_no
    else:
        return None
    return value if value > 0 else None


def catalog_id_from_tle_line(line: str) -> Optional[int]:
    if len(line) < 7:
        return None
    return decode_alpha5_catalog_id(line[2:7])


def tle_epoch_from_line1(line1: str) -> Optional[datetime]:
    if len(line1) < 32:
        return None
    token = line1[18:32].strip()
    if len(token) < 5:
        return None
    try:
        yy = int(token[:2])
        day_of_year = float(token[2:])
    except ValueError:
        return None
    year = 2000 + yy if yy < 57 else 1900 + yy
    jan1 = datetime(year, 1, 1, tzinfo=timezone.utc)
    return jan1 + timedelta(days=day_of_year - 1)


def _select_newer(current: Optional[TLEBlock], candidate: TLEBlock) -> TLEBlock:
    if current is None:
        return candidate
    if current.epoch is None:
        return candidate
    if candidate.epoch is None:
        return current
    return candidate if candidate.epoch >= current.epoch else current


def parse_tle_blocks(text: str) -> Dict[int, TLEBlock]:
    lines = [line.strip() for line in text.replace("\r", "").splitlines() if line.strip()]
    blocks: Dict[int, TLEBlock] = {}
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("1 ") and i + 1 < len(lines) and lines[i + 1].startswith("2 "):
            line1 = line
            line2 = lines[i + 1]
            norad1 = catalog_id_from_tle_line(line1)
            norad2 = catalog_id_from_tle_line(line2)
            if norad1 is not None and norad1 == norad2:
                candidate = TLEBlock(norad_id=norad2, lines=(line1, line2), epoch=tle_epoch_from_line1(line1))
                blocks[norad2] = _select_newer(blocks.get(norad2), candidate)
            i += 2
            continue
        if i + 2 < len(lines) and lines[i + 1].startswith("1 ") and lines[i + 2].startswith("2 "):
            name = lines[i]
            line1 = lines[i + 1]
            line2 = lines[i + 2]
            norad1 = catalog_id_from_tle_line(line1)
            norad2 = catalog_id_from_tle_line(line2)
            if norad1 is not None and norad1 == norad2:
                candidate = TLEBlock(norad_id=norad2, lines=(name, line1, line2), epoch=tle_epoch_from_line1(line1))
                blocks[norad2] = _select_newer(blocks.get(norad2), candidate)
            i += 3
            continue
        i += 1
    return blocks


def merge_catalogs(existing_text: str, update_text: str) -> Dict[int, TLEBlock]:
    merged = parse_tle_blocks(existing_text)
    for norad_id, block in parse_tle_blocks(update_text).items():
        merged[norad_id] = _select_newer(merged.get(norad_id), block)
    return merged


def format_catalog(blocks: Dict[int, TLEBlock]) -> str:
    return "\n".join(blocks[norad].text for norad in sorted(blocks)) + ("\n" if blocks else "")


def newest_epoch(blocks: Dict[int, TLEBlock]) -> Optional[datetime]:
    epochs = [block.epoch for block in blocks.values() if block.epoch is not None]
    return max(epochs) if epochs else None


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build_manifest(
    *,
    catalog_text: str,
    source_retrieved_at: datetime,
    query_kind: str,
    public_base_url: str,
) -> dict:
    blocks = parse_tle_blocks(catalog_text)
    raw = catalog_text.encode("utf-8")
    gz = gzip.compress(raw, compresslevel=9, mtime=0)
    base = public_base_url.rstrip("/")
    return {
        "schema_version": 1,
        "source": "Space-Track GP mirror",
        "attribution": "Data from Space-Track.org GP class. Propagation performed on-device by the app.",
        "generated_at": isoformat_z(utc_now()),
        "source_retrieved_at": isoformat_z(source_retrieved_at),
        "query_kind": query_kind,
        "object_count": len(blocks),
        "newest_tle_epoch": isoformat_z(newest_epoch(blocks)),
        "catalog": {
            "path": "current.3le",
            "url": f"{base}/current.3le" if base else None,
            "sha256": sha256_hex(raw),
            "bytes": len(raw),
            "content_type": "text/plain; charset=utf-8",
        },
        "catalog_gzip": {
            "path": "current.3le.gz",
            "url": f"{base}/current.3le.gz" if base else None,
            "sha256": sha256_hex(gz),
            "bytes": len(gz),
            "content_type": "application/gzip",
        },
    }


def write_catalog_outputs(output_dir: Path, catalog_text: str, manifest: dict) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    raw = catalog_text.encode("utf-8")
    (output_dir / "current.3le").write_bytes(raw)
    (output_dir / "current.3le.gz").write_bytes(gzip.compress(raw, compresslevel=9, mtime=0))
    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def should_run_full_refresh(existing_text: Optional[str], *, force_full: bool, now: datetime) -> bool:
    if force_full or not existing_text:
        return True
    blocks = parse_tle_blocks(existing_text)
    if len(blocks) < 1000:
        return True
    newest = newest_epoch(blocks)
    if newest is None:
        return True
    return (now - newest).total_seconds() > 7 * 24 * 3600
