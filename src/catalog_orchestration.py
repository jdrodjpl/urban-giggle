"""
Shared STAC cataloging + post-STAC webhook orchestration.

Used by both pipeline_cog.py (per-TIFF COG outputs) and pipeline_zarr.py
(single-Zarr output). Each worker emits a `catalog.json` to its DPS output;
the orchestrator pulls every catalog, upserts collections/items into MMGIS,
and fires the per-item webhook if configured.
"""

import argparse
import json
import logging
from typing import List, Optional

import fsspec
import pystac
from maap.dps.dps_job import DPSJob

import create_stac_items
from common_utils import AWSUtils, LoggingUtils, MaapUtils

logger = logging.getLogger(__name__)


def load_remote_catalog(maap, stac_cat_file: str) -> pystac.Catalog:
    """Resolve a remote catalog.json (S3) into a pystac.Catalog with
    absolute asset hrefs."""
    bucket_name, catalog_path = AWSUtils.parse_s3_path(stac_cat_file)
    presigned_url = maap.aws.s3_signed_url(bucket_name, catalog_path)['url']
    with fsspec.open(presigned_url, "r") as f:
        data = json.load(f)
    catalog = pystac.Catalog.from_dict(data)
    catalog.set_self_href(presigned_url)
    catalog.make_all_asset_hrefs_absolute()
    return catalog


def upsert_one_catalog(
    catalog: pystac.Catalog,
    args: argparse.Namespace,
    mmgis_token: str,
    webhook_token: Optional[str],
    ogc_uris: List[str],
    asset_uris: List[str],
    primary_asset_keys: tuple = ("asset", "data"),
) -> None:
    """Walk a single pystac.Catalog and upsert each collection/items
    into MMGIS. Updates ogc_uris/asset_uris in place. Fires the
    post-STAC webhook (if configured) after each successful upsert.

    `primary_asset_keys` controls which asset on each item is treated as
    the "main" output for webhook + product-notification purposes — COGs
    use "asset", Zarr items use "data"."""
    for _root, collections, _items in catalog.walk():
        if not collections:
            continue
        for coll in collections:
            collection_id = coll.to_dict()['id']
            collection_items = list(coll.get_items())

            for item in collection_items:
                for asset_key, asset in item.assets.items():
                    if asset.href.startswith("https://") and ".s3." in asset.href:
                        asset.href = AWSUtils.convert_s3_http_to_s3_uri(asset.href)
                    ogc_uris.append(
                        f"{args.mmgis_host}/stac/collections/{collection_id}/items/{item.id}"
                    )
                    if asset_key in primary_asset_keys and asset.href not in asset_uris:
                        asset_uris.append(asset.href)

            upserted = create_stac_items.upsert_collection(
                mmgis_url=args.mmgis_host,
                mmgis_token=mmgis_token,
                collection_id=collection_id,
                collection=coll,
                collection_items=collection_items,
                upsert_items=args.upsert,
            )
            if upserted:
                m = f"STAC catalog update complete for collection {collection_id}."
                logger.info(m)
                LoggingUtils.cmss_logger(m, args.cmss_logger_host)

                if args.post_stac_webhook_url:
                    for item in collection_items:
                        primary = next(
                            (item.assets[k] for k in primary_asset_keys
                             if k in item.assets),
                            None,
                        )
                        if primary is None:
                            continue
                        LoggingUtils.post_stac_webhook(
                            webhook_url=args.post_stac_webhook_url,
                            collection_id=collection_id,
                            item_id=item.id,
                            asset_uri=primary.href,
                            token=webhook_token,
                        )


def catalog_products(
    args: argparse.Namespace,
    maap,
    worker_jobs: List[DPSJob],
    primary_asset_keys: tuple = ("asset", "data"),
) -> None:
    """Pull every `catalog.json` from completed worker jobs and upsert into
    MMGIS. Collection upsert is idempotent (PUT-on-exist, POST-on-new), so
    multiple workers writing to the same collection accrete items. Item
    conflicts are governed by `args.upsert` (default on)."""
    if not (args.mmgis_host and args.titiler_token_secret_name):
        logger.info("MMGIS host or token secret not provided — skipping STAC catalog step")
        return

    stac_cat_files = MaapUtils.get_dps_output(worker_jobs, "catalog.json")
    if not stac_cat_files:
        raise RuntimeError("No STAC catalog files found from worker jobs")

    logger.info(
        f"Cataloging {len(stac_cat_files)} worker catalog(s) "
        f"(upsert_items={args.upsert})"
    )
    mmgis_token = maap.secrets.get_secret(args.titiler_token_secret_name)

    webhook_token: Optional[str] = None
    if args.post_stac_webhook_url and args.post_stac_webhook_token_secret_name:
        webhook_token = maap.secrets.get_secret(args.post_stac_webhook_token_secret_name)

    ogc_uris: List[str] = []
    asset_uris: List[str] = []

    for stac_cat_file in stac_cat_files:
        logger.info(f"Cataloging from {stac_cat_file}")
        catalog = load_remote_catalog(maap, stac_cat_file)
        upsert_one_catalog(
            catalog, args, mmgis_token, webhook_token,
            ogc_uris, asset_uris, primary_asset_keys,
        )

    product_details = {
        "concept_id": args.collection_id,
        "ogc": ogc_uris,
        "uris": asset_uris,
        "job_id": MaapUtils.get_job_id(),
    }
    LoggingUtils.cmss_product_available(product_details, args.cmss_logger_host)
    LoggingUtils.cmss_logger(
        f"Products available for collection {args.collection_id}",
        args.cmss_logger_host,
    )
