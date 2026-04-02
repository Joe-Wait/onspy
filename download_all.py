#!/usr/bin/env python3
"""Backward-compatible script wrapper for parquet sync helpers.

Usage:
    python download_all.py
    python download_all.py --output-dir my_data --resume
    python download_all.py --dataset-id cpih01 --dataset-id weekly-deaths-region
"""

from __future__ import annotations

import argparse
import json
import logging

import onspy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Download ONS datasets as parquet files"
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
    parser.add_argument(
        "--dataset-id",
        action="append",
        dest="dataset_ids",
        help="Dataset ID to download (repeat flag for multiple IDs)",
    )
    args = parser.parse_args()

    if args.dataset_ids:
        summary = onspy.download_datasets_parquet(
            dataset_ids=args.dataset_ids,
            output_dir=args.output_dir,
            resume=args.resume,
            delay=args.delay,
        )
    else:
        summary = onspy.download_all_parquet(
            output_dir=args.output_dir,
            resume=args.resume,
            delay=args.delay,
        )

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
