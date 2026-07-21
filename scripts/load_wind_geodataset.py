#!/usr/bin/env python3
"""Load full-resolution ECMWF wind point GeoJSONs into an MMGIS geodataset.

Runs on the MMGIS server after the nightly S3 sync (see the 10:30 UTC crontab
entry). Scans the synced `*_full.geojson` files (one per day, ~850k points,
written by urban-giggle's wind_arrows worker with a per-feature `time`
property), chunk-appends any unloaded dates into the `frozon-wind-full`
geodataset via the MMGIS Geodatasets API, then prunes rows older than
RETAIN_DAYS via DELETE /api/geodatasets/prune/:name (added to MMGIS alongside
this script — there is no other delete-by-date mechanism in the API).

Endpoint semantics that matter (see MMGIS plugins/core/backend/Geodatasets):
  * POST /api/geodatasets/recreate/:name/:start_end_prop — body is the
    FeatureCollection; creates (or truncates!) the dataset. Used only on
    first bootstrap.
  * POST /api/geodatasets/append/:name?start_prop=&end_prop= — body is the
    FeatureCollection. start_prop/end_prop MUST be resent on every call or
    appended rows get NULL times and never match temporal queries.
  * Auth: long-term token in the Authorization header.

State (loaded dates + per-date chunk progress) is kept in STATE_PATH so an
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

DATASET = os.environ.get("WIND_GEODATASET", "frozon-wind-full")
SRC_DIR = Path(os.environ.get(
    "WIND_FULL_DIR",
    "/home/mmgis/frozon-efs/Layers/frozon-ecmwf-wind-arrows-daily"))
MMGIS_HOST = os.environ.get("MMGIS_HOST", "http://localhost:18881")
TOKEN = os.environ.get("MMGIS_TOKEN", "")
STATE_PATH = Path(os.environ.get(
    "WIND_LOADER_STATE", str(Path(__file__).parent / "wind-loader-state.json")))
CHUNK = int(os.environ.get("WIND_LOADER_CHUNK", "25000"))
RETAIN_DAYS = int(os.environ.get("RETAIN_DAYS", "7"))
TIME_PROP = "time"


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


def dataset_exists() -> bool:
    try:
        out = api("POST", "/api/geodatasets/entries", body={})
        entries = out.get("body", out.get("entries", [])) or []
        names = [e.get("name") for e in entries if isinstance(e, dict)]
        return DATASET in names
    except Exception as e:  # noqa: BLE001 — treat as unknown, bootstrap safely
        log(f"entries check failed ({e}); assuming dataset exists")
        return True


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {"dates": {}}


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


def load_date(date_key: str, path: Path, state: dict, bootstrap: bool) -> None:
    log(f"[{date_key}] parsing {path.name} "
        f"({path.stat().st_size / 1e6:.0f} MB)")
    features = json.loads(path.read_text())["features"]
    total = len(features)
    n_chunks = (total + CHUNK - 1) // CHUNK
    entry = state["dates"].setdefault(
        date_key, {"chunks_done": 0, "n_chunks": n_chunks, "done": False})
    if entry.get("n_chunks") != n_chunks:
        entry.update({"chunks_done": 0, "n_chunks": n_chunks, "done": False})

    for i in range(entry["chunks_done"], n_chunks):
        chunk = features[i * CHUNK:(i + 1) * CHUNK]
        fc = {"type": "FeatureCollection", "features": chunk}
        if bootstrap and i == 0 and entry["chunks_done"] == 0:
            api("POST",
                f"/api/geodatasets/recreate/{DATASET}/{TIME_PROP},{TIME_PROP}",
                body=fc)
        else:
            api("POST",
                f"/api/geodatasets/append/{DATASET}"
                f"?start_prop={TIME_PROP}&end_prop={TIME_PROP}",
                body=fc)
        entry["chunks_done"] = i + 1
        save_state(state)
        log(f"[{date_key}] chunk {i + 1}/{n_chunks} "
            f"({len(chunk)} features) loaded")

    entry["done"] = True
    save_state(state)
    log(f"[{date_key}] complete: {total} features")


def main() -> int:
    if not TOKEN:
        log("ERROR: MMGIS_TOKEN not set")
        return 1

    state = load_state()
    files = discover_files()
    if not files:
        log(f"no *_full.geojson files under {SRC_DIR}; nothing to do")
        return 0

    pending = {d: p for d, p in files.items()
               if not state["dates"].get(d, {}).get("done")}
    log(f"{len(files)} full-res file(s) on disk, {len(pending)} to load")

    exists = dataset_exists() if pending else True
    bootstrap = not exists
    for date_key in sorted(pending):
        t0 = time.time()
        try:
            load_date(date_key, pending[date_key], state, bootstrap)
        except (urllib.error.URLError, RuntimeError) as e:
            log(f"[{date_key}] FAILED ({e}); will resume at chunk "
                f"{state['dates'].get(date_key, {}).get('chunks_done', 0)} "
                f"next run")
            return 2
        bootstrap = False
        log(f"[{date_key}] took {time.time() - t0:.0f}s")

    # Rolling retention: prune rows older than RETAIN_DAYS, and forget state
    # for dates whose source files the S3 sync has already pruned from disk.
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETAIN_DAYS)
    cutoff_iso = cutoff.strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        out = api("DELETE",
                  f"/api/geodatasets/prune/{DATASET}?end_before={cutoff_iso}")
        log(f"prune < {cutoff_iso}: {out.get('body', {}).get('deleted')} "
            f"row(s) deleted")
    except (urllib.error.URLError, RuntimeError) as e:
        log(f"prune failed ({e}) — non-fatal, will retry next run")
    state["dates"] = {d: v for d, v in state["dates"].items()
                      if d >= cutoff.strftime("%Y%m%d") or d in files}
    save_state(state)
    return 0


if __name__ == "__main__":
    sys.exit(main())
