#!/usr/bin/env python3
"""
Build a sparse Zarr time series from GeoTIFFs using streaming writes.
Processes all files upfront, pre-allocates zarr array, writes in batches.
No bounds expansion or complex append logic needed.
"""

import os
import sys
import argparse
import logging
from pathlib import Path
from typing import List, Optional, Tuple
import numpy as np
from datetime import datetime
import re
import zarr
from fnmatch import fnmatch
import gc
import rasterio

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def extract_datetime_from_filename(filename: str, time_regex: Optional[str] = None) -> Optional[datetime]:
    """Extract datetime from filename using regex pattern.

    Args:
        filename: The filename to extract from
        time_regex: Regex pattern with named group 'start_date' or first capture group.
                   Example: SWOT_L2_LR_NPFRB_\\d+_\\d+_(?P<start_date>\\d{8}T\\d{6})

    If no regex provided, tries common patterns (SWOT, Sentinel-1, etc)
    """
    # If custom regex provided, use it
    if time_regex:
        try:
            match = re.search(time_regex, filename)
            if match:
                # Try named group 'start_date' first
                try:
                    date_str = match.group('start_date')
                except IndexError:
                    # Fall back to first capture group
                    date_str = match.group(1)

                # Parse the date string
                if len(date_str) == 8:
                    # YYYYMMDD format
                    return datetime.strptime(date_str, '%Y%m%d')
                elif len(date_str) == 15 and 'T' in date_str:
                    # YYYYMMDDTHHMMSS format
                    return datetime.strptime(date_str, '%Y%m%dT%H%M%S')
                else:
                    return datetime.fromisoformat(date_str)
        except Exception as e:
            logger.warning(f"Could not parse date with provided regex from {filename}: {e}")
            return None

    # Default patterns if no regex provided
    # Try SWOT L2 pattern first
    match = re.search(r'SWOT_L2_LR_NPFRB_\d+_\d+_(\d{8}T\d{6})', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%dT%H%M%S')

    # Try SWOT binned pattern - try YYYYMMDD first, then YYYYMM
    match = re.search(r'swot_bin_(\d{8})', filename, re.IGNORECASE)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d')

    match = re.search(r'swot_bin_(\d{6})', filename, re.IGNORECASE)
    if match:
        return datetime.strptime(match.group(1) + '01', '%Y%m%d')

    # Try Sentinel-1 pattern
    match = re.search(r'S1[AB]_\w+_\w+_\w+_(\d{8}T\d{6})_(\d{8}T\d{6})', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%dT%H%M%S')

    # Try ICESat-2 pattern - try YYYYMMDD first, then YYYYMM
    match = re.search(r'icesat2_bin_(\d{8})', filename)
    if match:
        return datetime.strptime(match.group(1), '%Y%m%d')

    match = re.search(r'icesat2_bin_(\d{6})', filename)
    if match:
        # YYYYMM format - default to first day of month
        return datetime.strptime(match.group(1) + '01', '%Y%m%d')

    return None


def get_global_bounds_and_resolution(tiff_files: List[Path]) -> Tuple[dict, float, str]:
    """
    Calculate the union of all bounds from all files and detect resolution.
    Returns: (bounds_dict, resolution, crs)
    """
    logger.info("Calculating global extent from all files...")

    all_bounds = []
    resolutions = []
    crs = None

    for i, f in enumerate(tiff_files):
        if i % 20 == 0:
            logger.info(f"  Checking bounds: {i}/{len(tiff_files)}")

        try:
            with rasterio.open(str(f)) as src:
                bounds = src.bounds  # (left, bottom, right, top)
                all_bounds.append(bounds)

                # Get resolution from transform
                res_x = abs(src.transform.a)
                res_y = abs(src.transform.e)
                resolutions.append((res_x, res_y))

                if crs is None:
                    crs = src.crs
                    source_nodata = src.nodata
        except Exception as e:
            logger.warning(f"  Skipping corrupted file: {f.name} ({type(e).__name__})")
            continue

    if not all_bounds:
        raise ValueError("Could not read bounds from any TIFF files")

    # Calculate union of all bounds
    min_x = min(b[0] for b in all_bounds)
    min_y = min(b[1] for b in all_bounds)
    max_x = max(b[2] for b in all_bounds)
    max_y = max(b[3] for b in all_bounds)

    # Use median resolution (handle slight variations)
    res_x_values = [r[0] for r in resolutions]
    res_y_values = [r[1] for r in resolutions]
    resolution = np.median(res_x_values)

    global_bounds = {
        'west': min_x,
        'south': min_y,
        'east': max_x,
        'north': max_y,
        'width': max_x - min_x,
        'height': max_y - min_y
    }

    logger.info(f"Global extent: {global_bounds['width']:.0f} x {global_bounds['height']:.0f} units")
    logger.info(f"Resolution: {resolution:.2f} units")
    logger.info(f"CRS: {crs}")

    return global_bounds, resolution, str(crs) if crs else 'unknown'


def build_zarr_streaming(
    tiff_files: List[Path],
    output_path: Path,
    chunk_size: int = 1024,
    limit: Optional[int] = None,
    time_regex: Optional[str] = None
) -> bool:
    """
    Build sparse Zarr by streaming file data one at a time.
    Calculates bounds that encompass all input TIFFs, pre-allocates zarr array, writes files sequentially.
    No reprojection - preserves native resolution. Files placed at correct positions in expanded grid.
    """
    try:
        if limit:
            tiff_files = tiff_files[:limit]
            logger.info(f"Limited to {len(tiff_files)} files")

        # Validate that we can extract dates from filenames BEFORE doing anything else
        logger.info("Validating date extraction from filenames...")
        files_without_dates = []
        for tiff_file in tiff_files:
            dt = extract_datetime_from_filename(tiff_file.name, time_regex=time_regex)
            if not dt:
                files_without_dates.append(tiff_file.name)

        if files_without_dates:
            logger.error(f"\n❌ FATAL: Cannot extract dates from {len(files_without_dates)} file(s):")
            for fname in files_without_dates[:10]:  # Show first 10
                logger.error(f"  - {fname}")
            if len(files_without_dates) > 10:
                logger.error(f"  ... and {len(files_without_dates) - 10} more")

            logger.error("\nPlease fix filename format or provide --time_regex parameter.")
            logger.error("Supported formats:")
            logger.error("  - SWOT L2: SWOT_L2_LR_NPFRB_024_529_20241130T233441_..._freeboard.tif")
            logger.error("  - SWOT binned: swot_bin_20231130_ssha_COG.tif")
            logger.error("  - Sentinel-1: S1A_EW_GRDM_1SDH_20241201T163328_..._sigma0.tif")
            logger.error("  - ICESat-2 daily: icesat2_bin_20250321_freeboard.tif")
            logger.error("  - ICESat-2 monthly: icesat2_bin_202412_lead_ssha.tif")
            return False

        logger.info(f"✓ Successfully extracted dates from all {len(tiff_files)} files")

        # Get global bounds and resolution from all files
        global_bounds, resolution, crs = get_global_bounds_and_resolution(tiff_files)

        # Calculate grid dimensions based on global bounds
        grid_width = int(np.ceil(global_bounds['width'] / resolution))
        grid_height = int(np.ceil(global_bounds['height'] / resolution))

        logger.info(f"Creating global grid: {grid_height} x {grid_width} pixels at {resolution:.2f} unit resolution")

        # Create coordinate arrays for the global grid
        x_coords = np.arange(global_bounds['west'], global_bounds['west'] + grid_width * resolution, resolution)[:grid_width]
        y_coords = np.arange(global_bounds['north'], global_bounds['north'] - grid_height * resolution, -resolution)[:grid_height]

        # Delete existing output if it exists
        if output_path.exists():
            import shutil
            shutil.rmtree(output_path)

        # Initialize Zarr store with direct zarr API
        logger.info(f"Initializing Zarr store at {output_path}...")
        store = zarr.open(str(output_path), mode='w')

        # Detect zarr version to use correct dimension metadata
        zarr_major = int(zarr.__version__.split('.')[0])
        dim_kwargs = {}
        if zarr_major >= 3:
            # zarr v3: dimension_names is a first-class array metadata field
            dim_kwargs_data = {'dimension_names': ['time', 'y', 'x']}
            dim_kwargs_time = {'dimension_names': ['time']}
            dim_kwargs_y = {'dimension_names': ['y']}
            dim_kwargs_x = {'dimension_names': ['x']}
        else:
            dim_kwargs_data = {}
            dim_kwargs_time = {}
            dim_kwargs_y = {}
            dim_kwargs_x = {}

        # Pre-allocate data array with global grid dimensions
        data_array = store.create_array(
            'data',
            shape=(len(tiff_files), grid_height, grid_width),
            chunks=(1, chunk_size, chunk_size),
            dtype=np.float32,
            fill_value=np.nan,
            **dim_kwargs_data
        )

        # Create coordinate arrays
        # Create time array with datetime64[ns] dtype (placeholder values, updated after processing)
        store.create_array('time', data=np.zeros(len(tiff_files), dtype='datetime64[ns]'), **dim_kwargs_time)
        store.create_array('y', data=y_coords, **dim_kwargs_y)
        store.create_array('x', data=x_coords, **dim_kwargs_x)

        # Store metadata
        store.attrs['created'] = datetime.now().isoformat()
        store.attrs['source_files_count'] = len(tiff_files)
        store.attrs['crs'] = crs
        store.attrs['resolution'] = float(resolution)
        store.attrs['bounds_west'] = float(global_bounds['west'])
        store.attrs['bounds_south'] = float(global_bounds['south'])
        store.attrs['bounds_east'] = float(global_bounds['east'])
        store.attrs['bounds_north'] = float(global_bounds['north'])
        if source_nodata is not None:
            store.attrs['source_nodata'] = float(source_nodata)

        # Process each file one at a time (most memory efficient)
        all_timestamps = []
        total_processed = 0
        total_skipped = 0

        logger.info(f"Processing {len(tiff_files)} files one at a time...")

        for t_idx, tiff_file in enumerate(tiff_files):
            logger.info(f"File {t_idx + 1}/{len(tiff_files)}: {tiff_file.name}")

            try:
                # Extract timestamp
                dt = extract_datetime_from_filename(tiff_file.name, time_regex=time_regex)
                if not dt:
                    dt = datetime.fromtimestamp(tiff_file.stat().st_mtime)

                # Read TIFF in spatial chunks to maintain low memory usage
                with rasterio.open(str(tiff_file)) as src:
                    # Get file bounds and dimensions
                    file_bounds = src.bounds  # (left, bottom, right, top)
                    file_height, file_width = src.height, src.width

                    # Calculate position in global grid
                    # Grid y increases downward from north
                    file_grid_y_start = int(np.round((global_bounds['north'] - file_bounds.top) / resolution))
                    file_grid_x_start = int(np.round((file_bounds.left - global_bounds['west']) / resolution))

                    # Process file in spatial chunks (e.g., 512×512) to keep memory low
                    spatial_chunk = 512
                    total_non_zero = 0

                    for file_y in range(0, file_height, spatial_chunk):
                        for file_x in range(0, file_width, spatial_chunk):
                            # Current chunk bounds in source file
                            chunk_y_end = min(file_y + spatial_chunk, file_height)
                            chunk_x_end = min(file_x + spatial_chunk, file_width)
                            chunk_height = chunk_y_end - file_y
                            chunk_width = chunk_x_end - file_x

                            # Corresponding position in global grid
                            grid_y_start = file_grid_y_start + file_y
                            grid_x_start = file_grid_x_start + file_x
                            grid_y_end = grid_y_start + chunk_height
                            grid_x_end = grid_x_start + chunk_width

                            # Clip to grid boundaries
                            grid_y_clipped_start = max(0, grid_y_start)
                            grid_x_clipped_start = max(0, grid_x_start)
                            grid_y_clipped_end = min(grid_height, grid_y_end)
                            grid_x_clipped_end = min(grid_width, grid_x_end)

                            # Skip chunk if entirely outside grid
                            if grid_y_clipped_start >= grid_y_clipped_end or grid_x_clipped_start >= grid_x_clipped_end:
                                continue

                            # Calculate source slice accounting for clipping
                            src_y_offset = max(0, -grid_y_start)
                            src_x_offset = max(0, -grid_x_start)
                            src_y_end = src_y_offset + (grid_y_clipped_end - grid_y_clipped_start)
                            src_x_end = src_x_offset + (grid_x_clipped_end - grid_x_clipped_start)

                            # Read chunk from file
                            chunk_data = src.read(1, window=((file_y, chunk_y_end), (file_x, chunk_x_end)))
                            chunk_array = chunk_data[src_y_offset:src_y_end, src_x_offset:src_x_end].astype(np.float32)

                            # Write chunk to zarr at correct position
                            data_array[t_idx, grid_y_clipped_start:grid_y_clipped_end, grid_x_clipped_start:grid_x_clipped_end] = chunk_array

                            chunk_non_zero = np.sum(chunk_array != 0)
                            total_non_zero += chunk_non_zero

                logger.info(f"  ✓ {total_non_zero:,} non-zero pixels")

                all_timestamps.append(dt)
                total_processed += 1

            except Exception as e:
                logger.error(f"  ✗ Error: {e}")
                total_skipped += 1

            # Clear memory after each file
            gc.collect()

        # Update time coordinate with actual timestamps
        logger.info("Updating time coordinates...")
        time_array = store['time']
        time_values = np.array(all_timestamps, dtype='datetime64[ns]')[:total_processed]
        time_array[:total_processed] = time_values

        # Add dimension metadata
        # Set dimension metadata for both zarr v2 and v3 compatibility
        for arr, dims in [(data_array, ['time', 'y', 'x']),
                          (time_array, ['time']),
                          (store['y'], ['y']),
                          (store['x'], ['x'])]:
            arr.attrs['_ARRAY_DIMENSIONS'] = dims
            arr.attrs['dimension_names'] = dims

        # Consolidate metadata
        logger.info("Consolidating metadata...")
        zarr.consolidate_metadata(str(output_path))

        # Final verification (sample small regions to avoid memory issues)
        logger.info("\n=== Final Verification ===")
        store_final = zarr.open(str(output_path), mode='r')
        logger.info(f"Shape: {store_final['data'].shape}")
        if len(time_values) > 0:
            logger.info(f"Time range: {time_values[0]} to {time_values[-1]}")

        # Check a few time slices for data (sample 1000x1000 region to avoid memory issues)
        sample_size = min(1000, grid_height, grid_width)
        for t in range(min(5, total_processed)):
            # Sample center region
            y_center = grid_height // 2
            x_center = grid_width // 2
            y_start = max(0, y_center - sample_size // 2)
            x_start = max(0, x_center - sample_size // 2)
            y_end = min(grid_height, y_start + sample_size)
            x_end = min(grid_width, x_start + sample_size)

            data_sample = store_final['data'][t, y_start:y_end, x_start:x_end]
            non_zero = np.sum(data_sample != 0)
            if non_zero > 0:
                logger.info(f"  Time {t}: Sample has {non_zero:,} non-zero pixels (real data ✓)")
            else:
                logger.info(f"  Time {t}: Sample shows no data (may be sparse)")

        # Zarr groups don't need to be closed (no close() method)
        del store_final
        gc.collect()

        logger.info(f"\n=== Processing Summary ===")
        logger.info(f"Successfully processed: {total_processed} files")
        logger.info(f"Skipped: {total_skipped} files")
        logger.info(f"Total attempted: {total_processed + total_skipped} files")

        logger.info(f"\n✓ Successfully created sparse Zarr at {output_path}")
        return True

    except Exception as e:
        logger.error(f"Failed to create sparse Zarr: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    parser = argparse.ArgumentParser(
        description='Build sparse Zarr time series from GeoTIFFs using streaming writes.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This creates a sparse array where:
- Each pixel has data ONLY when the satellite passed over it
- No data interpolation or filling
- Preserves actual values from all TIFFs
- Accepts that most pixels will be NaN/0 at any given time
- Pre-allocates full array, writes in streaming batches

Example:
    %(prog)s /path/to/tiffs -o sparse_timeseries.zarr --batch-size 5 --limit 20
        """
    )

    parser.add_argument('input_dir', help='Directory containing TIFF files')
    parser.add_argument('-o', '--output', required=True, help='Output Zarr path')
    parser.add_argument('--chunk-size', type=int, default=1024,
                       help='Zarr spatial chunk size (default: 1024)')
    parser.add_argument('--limit', type=int,
                       help='Limit number of files to process (for testing)')
    parser.add_argument('--filter', type=str,
                       help='Filter files using glob pattern (e.g., "*ssha.tif" or "S1*_VV.tif")')
    parser.add_argument('--exclude', type=str,
                       help='Exclude files matching glob pattern (e.g., "*lead_ssha*")')
    parser.add_argument('--time_regex', type=str, default=None,
                       help='Regex pattern to extract timestamp from filename. '
                            'Use named group "start_date" or first capture group. '
                            'Example: r"SWOT_L2_LR_NPFRB_\\d+_\\d+_(?P<start_date>\\d{8}T\\d{6})"')

    args = parser.parse_args()

    # Find TIFF files
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        logger.error(f"Input directory not found: {input_dir}")
        sys.exit(1)

    patterns = ['*.tif', '*.tiff', '*.TIF', '*.TIFF']
    tiff_files = []
    for pattern in patterns:
        tiff_files.extend(input_dir.glob(pattern))
    tiff_files = sorted(tiff_files)

    if not tiff_files:
        logger.error("No TIFF files found")
        sys.exit(1)

    logger.info(f"Found {len(tiff_files)} TIFF files")

    # Apply filter if specified
    if args.filter:
        filtered_files = [f for f in tiff_files if fnmatch(f.name, args.filter)]
        logger.info(f"Applied filter '{args.filter}': {len(filtered_files)} files match")
        if not filtered_files:
            logger.error(f"No files match the filter '{args.filter}'")
            sys.exit(1)
        tiff_files = filtered_files

    # Apply exclude if specified
    if args.exclude:
        before_count = len(tiff_files)
        tiff_files = [f for f in tiff_files if not fnmatch(f.name, args.exclude)]
        logger.info(f"Applied exclude '{args.exclude}': removed {before_count - len(tiff_files)} files, {len(tiff_files)} remaining")
        if not tiff_files:
            logger.error(f"All files were excluded by '{args.exclude}'")
            sys.exit(1)

    # Create sparse zarr
    success = build_zarr_streaming(
        tiff_files=tiff_files,
        output_path=Path(args.output),
        chunk_size=args.chunk_size,
        limit=args.limit,
        time_regex=args.time_regex
    )

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
