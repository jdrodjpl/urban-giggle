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
import re
import sys
import time
import urllib.request
from pathlib import Path

POLL_INTERVAL_S = 30
TERMINAL_STATES = {"success", "failed", "canceled", "skipped", "manual"}


def _pipeline_status(pipeline_web_url: str) -> str:
    """Scrape the pipeline page HTML for its current status.

    GitLab's pipeline page is publicly accessible (no auth gate, unlike
    the raw job log) and includes the pipeline status in the
    `data-qa-selector` and badge classes. We grep for `ci_status_badge`
    style markers; if multiple matches, pick the most-recent one. Returns
    one of: pending, running, success, failed, canceled, skipped, manual,
    or 'unknown'.
    """
    try:
        html = urllib.request.urlopen(pipeline_web_url, timeout=30).read().decode()
    except Exception as e:
        return f"fetch-error:{e}"

    # Look for the pipeline-level status. GitLab embeds it in several places;
    # the most reliable I've found is `data-status="<state>"` on the status
    # badge link near the top of the pipeline page.
    candidates = re.findall(r'data-status="([a-z]+)"', html)
    for c in candidates:
        if c in TERMINAL_STATES or c in ("pending", "running", "preparing", "scheduled"):
            return c
    # Fallback — look for status icons via title attribute.
    for state in ("success", "failed", "running", "pending", "canceled", "skipped"):
        if f'title="{state.capitalize()}"' in html or f'aria-label="Status: {state.capitalize()}"' in html:
            return state
    return "unknown"


def register_and_wait(maap, yml_path: str, timeout_s: int = 1800) -> bool:
    """Register the algo at `yml_path` and poll its CI pipeline status
    until success/failure/timeout. Returns True on success."""
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
    pipeline_web_url = last_pipeline.get("web_url")
    sha = (last_pipeline.get("sha") or "")[:8]
    job_log_url = parsed.get("message", {}).get("job_log_url")

    if not pipeline_web_url:
        print(f"  ✗ No last_pipeline.web_url in response: {parsed}", file=sys.stderr)
        return False

    print(f"  pipeline {pipeline_id} (registry SHA {sha})")
    print(f"  page: {pipeline_web_url}")
    if job_log_url:
        print(f"  log:  {job_log_url}")

    start = time.time()
    last_status = None
    while True:
        elapsed = int(time.time() - start)
        if elapsed > timeout_s:
            print(f"  ✗ Timed out after {timeout_s}s waiting for build", file=sys.stderr)
            return False

        status = _pipeline_status(pipeline_web_url)
        if status != last_status:
            print(f"  status: {status} (t+{elapsed}s)", flush=True)
            last_status = status

        if status == "success":
            print(f"  ✓ Build complete (took {elapsed}s)", flush=True)
            return True
        if status in ("failed", "canceled"):
            print(f"  ✗ Build {status}. Open the page URL above for details.",
                  file=sys.stderr)
            return False
        if status == "skipped":
            # 'skipped' means MAAP didn't actually rebuild — usually because
            # nothing it cares about changed. Treat as success since the
            # existing image is what'll be used.
            print(f"  ✓ Build skipped (image reused)", flush=True)
            return True

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
