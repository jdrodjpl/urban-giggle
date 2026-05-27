#!/usr/bin/env python3
"""Standalone SWOT NetCDF → TIFF conversion script.

Use this to test and develop the converter implementation locally
before wiring it into the MAAP pipeline. Downloads a SWOT granule
from CMR (or reads a local .nc file), runs the converter, and writes
GeoTIFFs to an output directory.

Once the converter implementation is solid, the pipeline integration
point is:
  - COG pipeline: orchestrator downloads .nc, converts, uploads TIFFs
    to staging S3, then submits one COG worker per TIFF.
  - Zarr pipeline: worker downloads .nc files, converts inline, feeds
    TIFFs to the streaming Zarr builder.

Usage:
    # From a local NetCDF file:
    python scripts/convert_swot.py --input /path/to/swot_file.nc \\
        --dataset swot-ssh-expert --output-dir ./converted/

    # From CMR (downloads first):
    python scripts/convert_swot.py --cmr-short-name SWOT_L2_LR_SSH_Expert_2.0 \\
        --cmr-temporal-start 2024-04-10 --cmr-temporal-end 2024-04-11 \\
        --dataset swot-ssh-expert --output-dir ./converted/ --limit 1
"""

import argparse
import os
import sys
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert SWOT NetCDF granules to GeoTIFF."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--input", help="Path to a local NetCDF file")
    src.add_argument("--cmr-short-name",
                     help="CMR short_name to search and download from")

    parser.add_argument("--dataset", required=True,
                        choices=["swot-ssh-expert", "swot-ssh-unsmoothed"],
                        help="Which converter to use")
    parser.add_argument("--output-dir", required=True,
                        help="Directory to write output TIFFs")
    parser.add_argument("--variables", nargs="*", default=None,
                        help="Variables to extract (default: converter's default set)")

    parser.add_argument("--cmr-temporal-start", default=None)
    parser.add_argument("--cmr-temporal-end", default=None)
    parser.add_argument("--cmr-bbox", default=None,
                        help="west,south,east,north")
    parser.add_argument("--limit", type=int, default=1)

    args = parser.parse_args()

    sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))
    from converters import get_converter

    converter = get_converter(args.dataset)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.input:
        source_path = Path(args.input)
        if not source_path.exists():
            print(f"ERROR: {source_path} not found", file=sys.stderr)
            return 1
        sources = [source_path]
    else:
        import earthaccess
        earthaccess.login(strategy="environment")
        results = earthaccess.search_data(
            short_name=args.cmr_short_name,
            temporal=(args.cmr_temporal_start, args.cmr_temporal_end)
                if args.cmr_temporal_start else None,
            bounding_box=tuple(float(x) for x in args.cmr_bbox.split(','))
                if args.cmr_bbox else None,
        )
        if not results:
            print("No granules found.", file=sys.stderr)
            return 1
        results = results[:args.limit]
        print(f"Downloading {len(results)} granule(s)...")
        dl_dir = output_dir / "_downloads"
        dl_dir.mkdir(exist_ok=True)
        sources = earthaccess.download(results, str(dl_dir))
        sources = [Path(s) for s in sources]

    total_tiffs = []
    for src_file in sources:
        print(f"\nConverting {src_file.name} with {converter.dataset_name}...")
        try:
            results = converter.convert(src_file, output_dir, args.variables)
            for r in results:
                print(f"  → {r.path.name} (var={r.variable})")
            total_tiffs.extend(results)
        except NotImplementedError as e:
            print(f"\n  STUB: {e}", file=sys.stderr)
            print("  Implement the convert() method in "
                  "src/converters/swot_nc.py to proceed.", file=sys.stderr)
            return 3

    print(f"\nDone — {len(total_tiffs)} TIFF(s) in {output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
