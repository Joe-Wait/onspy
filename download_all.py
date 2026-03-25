#!/usr/bin/env python3
"""
Download all ONS datasets as Parquet files.

Creates an ons_datasets/ folder containing one .parquet file per dataset.
Skips datasets that fail to download and logs progress throughout.
Handles 429 rate-limit responses with exponential backoff.

Usage:
    python download_all.py
    python download_all.py --output-dir my_data
    python download_all.py --resume  # skip already-downloaded files

Query later with DuckDB:
    SELECT * FROM read_parquet('ons_datasets/cpih01.parquet');
    SELECT * FROM read_parquet('ons_datasets/*.parquet', filename=true);
"""

import argparse
import json
import logging
import random
import sys
import time
from datetime import datetime
from pathlib import Path

import onspy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Retry configuration
MAX_RETRIES = 5
INITIAL_BACKOFF = 5.0  # seconds
MAX_BACKOFF = 120.0  # seconds
BACKOFF_MULTIPLIER = 2.0


def _is_rate_limit_error(exc: Exception) -> bool:
    """Check if an exception is caused by a 429 rate limit response."""
    msg = str(exc).lower()
    # The requests library includes the status code in HTTPError messages
    if "429" in msg:
        return True
    if "too many requests" in msg:
        return True
    if "rate limit" in msg:
        return True
    return False


def download_with_retry(dataset_id: str, attempt_label: str):
    """Download a dataset with exponential backoff on 429 errors.

    Args:
        dataset_id: ONS dataset ID
        attempt_label: Label for log messages like "[3/337]"

    Returns:
        pandas DataFrame

    Raises:
        Exception: If all retries are exhausted or a non-retryable error occurs
    """
    backoff = INITIAL_BACKOFF

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            return onspy.download_dataset(dataset_id)
        except Exception as e:
            if _is_rate_limit_error(e) and attempt < MAX_RETRIES:
                # Add jitter: backoff +/- 25%
                jitter = backoff * random.uniform(-0.25, 0.25)
                wait = min(backoff + jitter, MAX_BACKOFF)
                log.warning(
                    f"{attempt_label} {dataset_id} -- 429 rate limited, "
                    f"retry {attempt}/{MAX_RETRIES} after {wait:.1f}s"
                )
                time.sleep(wait)
                backoff = min(backoff * BACKOFF_MULTIPLIER, MAX_BACKOFF)
            else:
                raise

    # Should never reach here, but just in case
    raise RuntimeError(f"Failed to download {dataset_id} after {MAX_RETRIES} retries")


def download_all(
    output_dir: str = "ons_datasets", resume: bool = False, delay: float = 2.0
):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # Fetch all dataset IDs
    log.info("Fetching dataset IDs from ONS API...")
    ids = onspy.get_dataset_ids()
    total = len(ids)
    log.info(f"Found {total} datasets")

    succeeded = []
    failed = []
    skipped = []
    consecutive_429s = 0

    for i, dataset_id in enumerate(ids, 1):
        parquet_path = out / f"{dataset_id}.parquet"

        # Resume mode: skip files that already exist
        if resume and parquet_path.exists():
            log.info(f"[{i}/{total}] {dataset_id} -- already exists, skipping")
            skipped.append(dataset_id)
            continue

        label = f"[{i}/{total}]"
        log.info(f"{label} Downloading {dataset_id}...")
        try:
            df = download_with_retry(dataset_id, label)
            consecutive_429s = 0  # reset on success

            if df.empty:
                log.warning(f"{label} {dataset_id} -- empty DataFrame, skipping")
                failed.append((dataset_id, "empty DataFrame"))
                continue

            df.to_parquet(parquet_path, index=False)
            log.info(
                f"{label} {dataset_id} -- saved {len(df):,} rows x {len(df.columns)} cols "
                f"-> {parquet_path}"
            )
            succeeded.append(dataset_id)

        except Exception as e:
            if _is_rate_limit_error(e):
                consecutive_429s += 1
                log.error(
                    f"{label} {dataset_id} -- rate limited after {MAX_RETRIES} retries"
                )
                # If we keep getting 429s, slow down significantly
                if consecutive_429s >= 3:
                    cooldown = 60.0 * consecutive_429s
                    log.warning(
                        f"  {consecutive_429s} consecutive 429s -- cooling down for {cooldown:.0f}s"
                    )
                    time.sleep(cooldown)
            else:
                consecutive_429s = 0
                log.error(f"{label} {dataset_id} -- FAILED: {e}")

            failed.append((dataset_id, str(e)))

        # Adaptive delay: slow down if we've been hitting rate limits recently
        if i < total:
            effective_delay = delay * (1 + consecutive_429s)
            time.sleep(effective_delay)

    # Write summary
    log.info("=" * 60)
    log.info(
        f"Done. {len(succeeded)} succeeded, {len(failed)} failed, {len(skipped)} skipped"
    )
    log.info(f"Parquet files in: {out.resolve()}")

    if failed:
        log.info(f"\nFailed datasets ({len(failed)}):")
        for dataset_id, reason in failed:
            log.info(f"  - {dataset_id}: {reason}")

    # Save manifest
    manifest = {
        "timestamp": datetime.utcnow().isoformat(),
        "total_available": total,
        "succeeded": succeeded,
        "failed": [{"id": d, "reason": r} for d, r in failed],
        "skipped": skipped,
    }
    manifest_path = out / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))
    log.info(f"Manifest written to {manifest_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Download all ONS datasets as Parquet files"
    )
    parser.add_argument(
        "--output-dir",
        default="ons_datasets",
        help="Output directory for parquet files (default: ons_datasets)",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip datasets that already have a .parquet file",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=2.0,
        help="Seconds to wait between downloads (default: 2.0)",
    )
    args = parser.parse_args()
    download_all(output_dir=args.output_dir, resume=args.resume, delay=args.delay)


if __name__ == "__main__":
    main()
