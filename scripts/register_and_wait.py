#!/usr/bin/env python3
"""Register one or more MAAP algorithms and block until each build
finishes (or fails). Prevents the recurring "I re-registered and
submitted but the job ran old code" problem by surfacing the actual
build state instead of guessing from the registration response.

Usage (from a MAAP notebook or shell with maap-py installed):

    # Register all four algos:
    python scripts/register_and_wait.py \\
        .maap/sample-algo-configs/frozon-iss-cog-pipeline.yml \\
        .maap/sample-algo-configs/frozon-iss-ingest-cog.yml \\
        .maap/sample-algo-configs/frozon-iss-zarr-pipeline.yml \\
        .maap/sample-algo-configs/frozon-iss-ingest-zarr.yml

    # Or just the two COG algos after a code change:
    python scripts/register_and_wait.py \\
        .maap/sample-algo-configs/frozon-iss-cog-pipeline.yml \\
        .maap/sample-algo-configs/frozon-iss-ingest-cog.yml

Exits non-zero if any build fails. Use with `&&` to chain a submit:

    python scripts/register_and_wait.py <yamls...> && \\
        python scripts/submit_cog_pipeline.py
"""

import argparse
import json
import sys
import time
import urllib.request
from pathlib import Path

POLL_INTERVAL_S = 30
SUCCESS_MARKER = "Build complete!"
FAILURE_MARKERS = ("did not complete successfully", "ERROR: Job failed")


def register_and_wait(maap, yml_path: str, timeout_s: int = 1800) -> bool:
    """Register the algo at `yml_path` and poll its CI build until it
    succeeds, fails, or times out. Returns True on success."""
    name = Path(yml_path).name
    print(f"\n=== Registering {name} ===", flush=True)

    result = maap.register_algorithm_from_yaml_file(yml_path)
    try:
        parsed = json.loads(result.text)
    except Exception:
        print(f"  ✗ Registration returned non-JSON: {result.text[:300]}",
              file=sys.stderr)
        return False

    last_pipeline = parsed.get("message", {}).get("last_pipeline", {})
    pipeline_id = last_pipeline.get("id")
    sha = (last_pipeline.get("sha") or "")[:8]
    job_log_url = parsed.get("message", {}).get("job_log_url")

    if not job_log_url:
        print(f"  ✗ No job_log_url in response: {parsed}", file=sys.stderr)
        return False

    print(f"  pipeline {pipeline_id} (registry SHA {sha})")
    print(f"  log: {job_log_url}")

    start = time.time()
    notified_queued = False
    while True:
        elapsed = int(time.time() - start)
        if elapsed > timeout_s:
            print(f"  ✗ Timed out after {timeout_s}s waiting for build", file=sys.stderr)
            return False

        try:
            log = urllib.request.urlopen(job_log_url, timeout=30).read().decode()
        except Exception as e:
            log = ""
            print(f"  ...log fetch error ({e}); retrying", flush=True)
            time.sleep(POLL_INTERVAL_S)
            continue

        # GitLab returns an HTML page when the job hasn't started yet (or
        # when raw-log auth is gated). Detect and back off quietly.
        is_html = log.lstrip().startswith("<!DOCTYPE") or "<html" in log[:500].lower()
        if is_html or not log.strip():
            if not notified_queued:
                print(f"  ...build queued, no log yet (will keep polling)", flush=True)
                notified_queued = True
            elif elapsed % 120 < POLL_INTERVAL_S:
                print(f"  ...still queued ({elapsed}s elapsed)", flush=True)
            time.sleep(POLL_INTERVAL_S)
            continue

        # Once we have real log content, the build has at least started.
        if notified_queued:
            print(f"  ...build started", flush=True)
            notified_queued = False

        tail = log[-4000:]
        if SUCCESS_MARKER in log:
            print(f"  ✓ Build complete (took {elapsed}s)", flush=True)
            return True
        if any(m in tail for m in FAILURE_MARKERS):
            print(f"  ✗ Build failed. Last 20 lines:", file=sys.stderr)
            for line in log.splitlines()[-20:]:
                print(f"    {line}", file=sys.stderr)
            return False

        # Print a brief tail so progress is visible.
        last_line = log.strip().splitlines()[-1] if log.strip() else "(empty)"
        print(f"  ...{last_line[:100]}", flush=True)
        time.sleep(POLL_INTERVAL_S)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("yamls", nargs="+", help="Algorithm config YAML paths")
    parser.add_argument("--maap-host", default="api.maap-project.org")
    parser.add_argument("--timeout", type=int, default=1800,
                        help="Per-algo build timeout in seconds (default 1800 = 30 min)")
    args = parser.parse_args()

    from maap.maap import MAAP
    maap = MAAP(maap_host=args.maap_host)

    all_passed = True
    for yml in args.yamls:
        if not Path(yml).exists():
            print(f"✗ {yml} not found", file=sys.stderr)
            all_passed = False
            continue
        if not register_and_wait(maap, yml, timeout_s=args.timeout):
            all_passed = False

    print("\n" + ("=" * 40))
    print("ALL BUILDS PASSED" if all_passed else "AT LEAST ONE BUILD FAILED")
    return 0 if all_passed else 1


if __name__ == "__main__":
    sys.exit(main())
