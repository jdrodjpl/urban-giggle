#!/usr/bin/env python3
"""Load full-resolution ECMWF wind point GeoJSONs into MMGIS geodatasets,
as a level-of-detail pyramid.

Runs on the MMGIS server after the nightly S3 sync (see the 10:30 UTC crontab
entry). Scans the synced `*_full.geojson` files (one per day, ~850k points on
the canonical EPSG:3413 10 km grid, written by urban-giggle's wind_arrows
worker with a per-feature `time` property) and loads each date into FOUR
geodatasets:

    frozonwindfull    every grid point        (10 km)   for zoom >= 6
    frozonwindlod4    every 4th row+col       (40 km)   for zoom 4-6
    frozonwindlod8    every 8th row+col       (80 km)   for zoom 3-4
    frozonwindlod16   every 16th row+col      (160 km)  for zoom 0-3

The thinned tiers are regular-grid subsets (not random samples), so each zoom
band renders as a proper coarser grid with bounded feature counts per
viewport (~40-80 px arrow spacing per band). Grid position is derived from
feature ORDER: build_wind_arrows ravels the 1120x760 grid row-major with
stride 1, so index i -> row i//760, col i%760. That invariant is guarded —
if a file's feature count isn't exactly 760*1120 the LOD tiers are skipped
for that date (the full dataset still loads).

Endpoint semantics that matter (see MMGIS plugins/core/backend/Geodatasets):
  * POST /recreate/:name/:start_end_prop bootstraps (or truncates!) a dataset.
  * POST /append/:name?start_prop=&end_prop= — start/end props MUST be resent
    on every call or appended rows get NULL times.
  * DELETE /prune/:name?end_before= — rolling retention.
  * Auth: long-term token in the Authorization header.

Each daily snapshot covers its whole day: start_time = the worker-stamped
`time` (00:00Z), end_time = injected `time_end` (next midnight), so any
intraday time-slider window matches (the get endpoint filters on overlap).

State (per dataset+date chunk progress) is kept in STATE_PATH so an
interrupted run resumes at the failed chunk instead of duplicating rows.

Stdlib only — no pip dependencies.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

# NOTE: MMGIS sanitizes geodataset names on creation (hyphens stripped), so
# use sanitized forms directly — hyphenated names would resolve on
# append/recreate but NOT on the frontend's /get lookup.
DATASETS = [
    # (name, stride) — smallest tiers first so coarse zoom levels come up
    # quickly on a fresh load; the big full-res load runs last.
    ("frozonwindlod16", 16),
    ("frozonwindlod8", 8),
    ("frozonwindlod4", 4),
    ("frozonwindfull", 1),
]
GRID_W = 760
GRID_H = 1120

SRC_DIR = Path(os.environ.get(
    "WIND_FULL_DIR",
    "/home/mmgis/frozon-efs/Layers/frozon-ecmwf-wind-arrows-daily"))
MMGIS_HOST = os.environ.get("MMGIS_HOST", "http://localhost/frozon")
TOKEN = os.environ.get("MMGIS_TOKEN", "")
STATE_PATH = Path(os.environ.get(
    "WIND_LOADER_STATE", str(Path(__file__).parent / "wind-loader-state.json")))
CHUNK = int(os.environ.get("WIND_LOADER_CHUNK", "25000"))
RETAIN_DAYS = int(os.environ.get("RETAIN_DAYS", "7"))
START_PROP = "time"
END_PROP = "time_end"


def log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')} {msg}",
          flush=True)


def api(method: str, path: str, body=None, timeout=600):
    url = f"{MMGIS_HOST}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers={
        "Authorization": f"Bearer {TOKEN}",
        "Content-Type": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        out = json.loads(r.read().decode())
    if out.get("status") != "success":
        raise RuntimeError(f"{method} {path} -> {out.get('message')}")
    return out


def existing_datasets() -> set:
    try:
        out = api("POST", "/api/geodatasets/entries", body={})
        entries = out.get("body", {}).get("entries", []) or []
        return {e.get("name") for e in entries if isinstance(e, dict)}
    except Exception as e:  # noqa: BLE001 — unknown: assume all exist (no truncate)
        log(f"entries check failed ({e}); assuming datasets exist")
        return {name for name, _ in DATASETS}


def load_state() -> dict:
    if STATE_PATH.exists():
        state = json.loads(STATE_PATH.read_text())
        if "dates" in state and "loads" not in state:
            # Migrate pre-pyramid state: old entries tracked only the full
            # dataset, keyed by bare date.
            state["loads"] = {
                f"frozonwindfull|{d}": v for d, v in state.pop("dates").items()
            }
        state.setdefault("loads", {})
        return state
    return {"loads": {}}


def save_state(state: dict) -> None:
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=1))
    tmp.replace(STATE_PATH)


def discover_files() -> dict:
    """date_key (YYYYMMDD) -> path of the *_full.geojson"""
    out = {}
    for f in sorted(SRC_DIR.rglob("*_full.geojson")):
        date_key = f.stem.replace("_full", "").split("_")[-1]
        if len(date_key) == 8 and date_key.isdigit():
            out[date_key] = f
    return out


def parse_day(path: Path, date_key: str) -> list:
    log(f"[{date_key}] parsing {path.name} "
        f"({path.stat().st_size / 1e6:.0f} MB)")
    features = json.loads(path.read_text())["features"]
    day = datetime.strptime(date_key, "%Y%m%d").replace(tzinfo=timezone.utc)
    time_end = (day + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    for f in features:
        f["properties"][END_PROP] = time_end
    return features


def thin(features: list, stride: int) -> list:
    if stride == 1:
        return features
    if len(features) != GRID_W * GRID_H:
        return None  # grid-order invariant broken; caller skips LOD tiers
    return [f for i, f in enumerate(features)
            if (i // GRID_W) % stride == 0 and (i % GRID_W) % stride == 0]


def load_into(dataset: str, features: list, date_key: str, state: dict,
              bootstrap: bool) -> None:
    key = f"{dataset}|{date_key}"
    total = len(features)
    n_chunks = (total + CHUNK - 1) // CHUNK
    entry = state["loads"].setdefault(
        key, {"chunks_done": 0, "n_chunks": n_chunks, "done": False})
    if entry.get("n_chunks") != n_chunks:
        entry.update({"chunks_done": 0, "n_chunks": n_chunks, "done": False})

    for i in range(entry["chunks_done"], n_chunks):
        chunk = features[i * CHUNK:(i + 1) * CHUNK]
        fc = {"type": "FeatureCollection", "features": chunk}
        if bootstrap and i == 0 and entry["chunks_done"] == 0:
            api("POST",
                f"/api/geodatasets/recreate/{dataset}/{START_PROP},{END_PROP}",
                body=fc)
        else:
            api("POST",
                f"/api/geodatasets/append/{dataset}"
                f"?start_prop={START_PROP}&end_prop={END_PROP}",
                body=fc)
        entry["chunks_done"] = i + 1
        save_state(state)
        if n_chunks > 1:
            log(f"[{date_key}] {dataset}: chunk {i + 1}/{n_chunks} loaded")

    entry["done"] = True
    save_state(state)
    log(f"[{date_key}] {dataset}: complete ({total} features)")


def main() -> int:
    if not TOKEN:
        log("ERROR: MMGIS_TOKEN not set")
        return 1

    state = load_state()
    files = discover_files()
    if not files:
        log(f"no *_full.geojson files under {SRC_DIR}; nothing to do")
        return 0

    pending = {
        d: p for d, p in files.items()
        if any(not state["loads"].get(f"{name}|{d}", {}).get("done")
               for name, _ in DATASETS)
    }
    log(f"{len(files)} full-res file(s) on disk, {len(pending)} date(s) with "
        f"work to do")
    if not pending:
        existing = None
    else:
        existing = existing_datasets()

    failed = False
    for date_key in sorted(pending):
        t0 = time.time()
        features = parse_day(pending[date_key], date_key)
        for name, stride in DATASETS:
            if state["loads"].get(f"{name}|{date_key}", {}).get("done"):
                continue
            subset = thin(features, stride)
            if subset is None:
                log(f"[{date_key}] {name}: SKIPPED — {len(features)} features "
                    f"!= {GRID_W}x{GRID_H}; grid-order invariant broken")
                continue
            try:
                load_into(name, subset, date_key, state, name not in existing)
                existing.add(name)
            except (urllib.error.URLError, RuntimeError) as e:
                done = state["loads"].get(f"{name}|{date_key}", {})
                log(f"[{date_key}] {name}: FAILED ({e}); will resume at chunk "
                    f"{done.get('chunks_done', 0)} next run")
                failed = True
                break
        if failed:
            break
        log(f"[{date_key}] took {time.time() - t0:.0f}s")

    # Rolling retention across all tiers; forget state for pruned dates.
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETAIN_DAYS)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    for name, _ in DATASETS:
        try:
            out = api("DELETE",
                      f"/api/geodatasets/prune/{name}?end_before={cutoff_iso}")
            deleted = out.get("body", {}).get("deleted")
            if deleted:
                log(f"prune {name} < {cutoff_iso}: {deleted} row(s)")
        except (urllib.error.URLError, RuntimeError) as e:
            log(f"prune {name} failed ({e}) — non-fatal, will retry next run")
    state["loads"] = {
        k: v for k, v in state["loads"].items()
        if k.split("|")[1] >= cutoff.strftime("%Y%m%d")
        or k.split("|")[1] in files
    }
    save_state(state)
    return 2 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
