#!/usr/bin/env python3
"""Emit _job.json params as shell-eval-able assignments.

Replaces the `jq -r '.params.X // empty' _job.json` calls in our run
scripts. jq is a system binary that wasn't reliably installed in our
orchestrator images on MAAP's CI; Python is always available.

Usage from a run script:
    eval "$(python3 /path/to/load_job_params.py _job.json)"

After eval'ing, every key from `_job.json`'s `.params` is set as a
shell variable, with `None`/null values normalized to empty string.
Values are shell-quoted to survive special characters.
"""

import json
import shlex
import sys


def main() -> int:
    path = sys.argv[1] if len(sys.argv) > 1 else "_job.json"
    with open(path) as f:
        data = json.load(f)
    params = data.get("params", {}) or {}
    for key, value in params.items():
        if value is None:
            value = ""
        # MAAP fills unset positional inputs with the literal string "none";
        # treat that as empty so the run scripts' `[[ -n "$var" ]]` checks
        # behave the same as for genuinely unset values.
        if isinstance(value, str) and value.strip().lower() in ("none", "null"):
            value = ""
        # JSON-encode lists/dicts so downstream python code can json.loads()
        # them. Without this, str(list) gives single-quoted Python repr like
        # ['a','b'] which isn't valid JSON.
        if isinstance(value, (list, dict)):
            value = json.dumps(value)
        elif isinstance(value, bool):
            # Lowercase so shell comparisons like [[ "$x" == "true" ]] work,
            # and argparse choices like {true,false} accept the value.
            value = "true" if value else "false"
        else:
            value = str(value)
        print(f"{key}={shlex.quote(value)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
