"""Parquet synchronization helpers for onspy.

These functions provide bulk dataset export to local parquet files so data can
be analyzed efficiently with DuckDB or other local analytical engines.
"""

from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pandas.errors import EmptyDataError

from . import core
from .exceptions import ONSConnectionError, ONSRequestError

logger = logging.getLogger(__name__)


MAX_RETRIES = 5
INITIAL_BACKOFF = 5.0
MAX_BACKOFF = 120.0
BACKOFF_MULTIPLIER = 2.0
STREAM_CSV_CHUNK_ROWS = 50_000
STREAM_DATASET_PREFIXES = ("ashe-", "TS", "RM", "ST")
SYNC_LOCK_FILENAME = ".onspy_parquet_sync.lock"


def _is_rate_limit_error(exc: Exception) -> bool:
    """Return True when an exception likely represents a 429 response."""
    if isinstance(exc, ONSRequestError) and exc.status_code == 429:
        return True
    msg = str(exc).lower()
    return "429" in msg or "too many requests" in msg or "rate limit" in msg


def _is_retryable_error(exc: Exception) -> bool:
    """Return True for transient HTTP/network errors that should retry."""
    if isinstance(exc, ONSConnectionError):
        return True
    if isinstance(exc, ONSRequestError):
        return exc.status_code in {429, 500, 502, 503, 504}
    return _is_rate_limit_error(exc)


def _is_retryable_stream_error(exc: Exception) -> bool:
    """Return True for transient stream/parse issues worth retrying."""
    if _is_retryable_error(exc):
        return True
    if isinstance(exc, EmptyDataError):
        return True
    msg = str(exc).lower()
    return "no columns to parse from file" in msg


def _normalize_dataset_ids(dataset_ids: Iterable[str]) -> List[str]:
    """Normalize and deduplicate dataset IDs while preserving order."""
    seen = set()
    normalized: List[str] = []

    for raw in dataset_ids:
        if raw is None:
            continue
        dataset_id = str(raw).strip()
        if not dataset_id or dataset_id in seen:
            continue
        seen.add(dataset_id)
        normalized.append(dataset_id)

    return normalized


def _pid_is_alive(pid: int) -> bool:
    """Return True when a process with pid appears to be running."""
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _acquire_sync_lock(output_dir: Path) -> tuple[Path, str]:
    """Acquire an exclusive lock file for parquet sync in output_dir."""
    lock_path = output_dir / SYNC_LOCK_FILENAME
    token = uuid.uuid4().hex
    payload = {
        "pid": os.getpid(),
        "token": token,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }

    for _ in range(3):
        try:
            fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            with os.fdopen(fd, "w", encoding="utf-8") as lock_file:
                json.dump(payload, lock_file)
            return lock_path, token
        except FileExistsError:
            try:
                existing = json.loads(lock_path.read_text(encoding="utf-8"))
            except Exception:
                existing = {}

            existing_pid = int(existing.get("pid", 0) or 0)
            if existing_pid and not _pid_is_alive(existing_pid):
                try:
                    lock_path.unlink()
                except FileNotFoundError:
                    pass
                continue

            raise RuntimeError(
                "Parquet sync already running for output directory "
                f"'{output_dir}'. Active lock: {lock_path}"
            )

    raise RuntimeError(f"Could not acquire parquet sync lock: {lock_path}")


def _release_sync_lock(lock_path: Path | None, token: str | None) -> None:
    """Release the sync lock only if it belongs to this process token."""
    if lock_path is None or token is None:
        return
    if not lock_path.exists():
        return

    try:
        payload = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        payload = {}

    if payload.get("token") != token:
        return

    try:
        lock_path.unlink()
    except FileNotFoundError:
        pass


def _download_dataset_with_retry(
    dataset_id: str,
    max_retries: int,
    initial_backoff: float,
    max_backoff: float,
) -> pd.DataFrame:
    """Download one dataset with retry/backoff for transient failures."""
    backoff = initial_backoff

    for attempt in range(1, max_retries + 1):
        try:
            df = core.download_dataset(dataset_id)
            if df.empty:
                raise RuntimeError("empty DataFrame")
            return df
        except Exception as exc:
            is_last_attempt = attempt >= max_retries
            is_retryable = _is_retryable_error(exc)

            if is_last_attempt or not is_retryable:
                raise

            jitter = backoff * random.uniform(-0.25, 0.25)
            wait = min(backoff + jitter, max_backoff)
            logger.warning(
                "%s failed (%s). Retry %d/%d in %.1fs",
                dataset_id,
                exc,
                attempt,
                max_retries,
                wait,
            )
            time.sleep(wait)
            backoff = min(backoff * BACKOFF_MULTIPLIER, max_backoff)

    raise RuntimeError(
        f"Failed to download dataset '{dataset_id}' after {max_retries} retries"
    )


def _should_stream_dataset_sync(dataset_id: str) -> bool:
    """Return True when dataset should be written via chunked CSV streaming."""
    lowered = dataset_id.lower()
    uppered = dataset_id.upper()
    if lowered.startswith("ashe-"):
        return True
    return uppered.startswith(STREAM_DATASET_PREFIXES[1:])


def _stream_csv_to_parquet(
    csv_url: str,
    parquet_path: Path,
    chunk_rows: int = STREAM_CSV_CHUNK_ROWS,
) -> tuple[int, int]:
    """Stream a remote CSV into parquet without loading full data into memory."""
    if not csv_url:
        raise RuntimeError("Missing CSV URL for streaming parquet export")
    if chunk_rows < 1:
        raise ValueError("chunk_rows must be >= 1")

    tmp_path = parquet_path.with_name(
        f".{parquet_path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp"
    )
    if tmp_path.exists():
        tmp_path.unlink()

    response = None
    writer = None
    total_rows = 0
    total_columns = 0

    try:
        response = core.make_request(csv_url, stream=True)
        response.raw.decode_content = True

        reader = pd.read_csv(
            response.raw,
            chunksize=chunk_rows,
            dtype="string",
            low_memory=False,
        )
        for chunk in reader:
            if chunk is None:
                continue

            table = pa.Table.from_pandas(chunk, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    str(tmp_path),
                    table.schema,
                    compression="zstd",
                )
                total_columns = int(table.num_columns)
            elif table.schema != writer.schema:
                table = table.cast(writer.schema, safe=False)

            writer.write_table(table)
            total_rows += int(len(chunk))

        if writer is None or total_rows == 0:
            raise RuntimeError("empty DataFrame")

        writer.close()
        writer = None
        tmp_path.replace(parquet_path)
        return total_rows, total_columns
    except Exception:
        if writer is not None:
            writer.close()
        if tmp_path.exists():
            tmp_path.unlink()
        raise
    finally:
        if response is not None:
            try:
                response.close()
            except Exception:
                pass


def _download_large_dataset_to_parquet_with_retry(
    dataset_id: str,
    parquet_path: Path,
    max_retries: int,
    initial_backoff: float,
    max_backoff: float,
) -> Dict[str, Any]:
    """Download large CSV-backed datasets via chunked streaming and retries."""
    backoff = initial_backoff

    for attempt in range(1, max_retries + 1):
        try:
            edition, version = core._resolve_edition_version(dataset_id)
            definition = core._get_dataset_definition(dataset_id, edition, version)
            csv_url = definition.get("downloads", {}).get("csv", {}).get("href", "")
            if not csv_url:
                raise RuntimeError("dataset does not expose CSV download")

            rows, columns = _stream_csv_to_parquet(csv_url, parquet_path)
            return {
                "id": dataset_id,
                "rows": rows,
                "columns": columns,
                "path": str(parquet_path),
                "sync_method": "csv_stream",
                "edition": edition,
                "version": version,
            }
        except Exception as exc:
            is_last_attempt = attempt >= max_retries
            is_retryable = _is_retryable_stream_error(exc)

            if is_last_attempt or not is_retryable:
                raise

            jitter = backoff * random.uniform(-0.25, 0.25)
            wait = min(backoff + jitter, max_backoff)
            logger.warning(
                "%s (stream) failed (%s). Retry %d/%d in %.1fs",
                dataset_id,
                exc,
                attempt,
                max_retries,
                wait,
            )
            time.sleep(wait)
            backoff = min(backoff * BACKOFF_MULTIPLIER, max_backoff)

    raise RuntimeError(
        f"Failed to stream dataset '{dataset_id}' after {max_retries} retries"
    )


def _write_manifest(output_dir: Path, manifest: Dict[str, Any]) -> Path:
    """Persist run metadata to output_dir/manifest.json."""
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest_path


def _build_manifest(
    *,
    mode: str,
    output_dir: Path,
    requested_ids: List[str],
    succeeded: List[Dict[str, Any]],
    failed: List[Dict[str, str]],
    skipped: List[str],
    started_at: float,
    status: str,
    current_dataset_id: str | None,
) -> Dict[str, Any]:
    """Build a manifest snapshot for the current sync progress."""
    elapsed_seconds = round(time.time() - started_at, 2)
    processed_count = len(succeeded) + len(failed) + len(skipped)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": status,
        "mode": mode,
        "output_dir": str(output_dir.resolve()),
        "requested_dataset_ids": requested_ids,
        "requested_count": len(requested_ids),
        "processed_count": processed_count,
        "remaining_count": len(requested_ids) - processed_count,
        "current_dataset_id": current_dataset_id,
        "succeeded_count": len(succeeded),
        "failed_count": len(failed),
        "skipped_count": len(skipped),
        "succeeded": succeeded,
        "failed": failed,
        "skipped": skipped,
        "elapsed_seconds": elapsed_seconds,
    }


def download_datasets_parquet(
    dataset_ids: List[str],
    output_dir: str = "ons_datasets",
    resume: bool = False,
    delay: float = 2.0,
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
    max_backoff: float = MAX_BACKOFF,
    _mode: str = "specific",
) -> Dict[str, Any]:
    """Download specific ONS datasets as parquet files.

    Args:
        dataset_ids: Dataset IDs to download.
        output_dir: Target directory for parquet files.
        resume: Skip datasets that already exist as parquet files.
        delay: Seconds to sleep between dataset downloads.
        max_retries: Maximum retries per dataset.
        initial_backoff: Initial backoff in seconds.
        max_backoff: Maximum backoff in seconds.

    Returns:
        Dictionary with sync summary and manifest path.
    """
    requested_ids = _normalize_dataset_ids(dataset_ids)
    if not requested_ids:
        raise ValueError("You must provide at least one dataset ID")
    if delay < 0:
        raise ValueError("delay must be >= 0")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    lock_path, lock_token = _acquire_sync_lock(out)

    try:
        started_at = time.time()
        succeeded: List[Dict[str, Any]] = []
        failed: List[Dict[str, str]] = []
        skipped: List[str] = []

        total = len(requested_ids)
        logger.info("Starting parquet sync for %d dataset(s)", total)

        manifest = _build_manifest(
            mode=_mode,
            output_dir=out,
            requested_ids=requested_ids,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            started_at=started_at,
            status="in_progress",
            current_dataset_id=None,
        )
        manifest_path = _write_manifest(out, manifest)

        for index, dataset_id in enumerate(requested_ids, start=1):
            label = f"[{index}/{total}]"
            parquet_path = out / f"{dataset_id}.parquet"

            manifest = _build_manifest(
                mode=_mode,
                output_dir=out,
                requested_ids=requested_ids,
                succeeded=succeeded,
                failed=failed,
                skipped=skipped,
                started_at=started_at,
                status="in_progress",
                current_dataset_id=dataset_id,
            )
            _write_manifest(out, manifest)

            if resume and parquet_path.exists():
                logger.info("%s %s -- exists, skipping", label, dataset_id)
                skipped.append(dataset_id)
                manifest = _build_manifest(
                    mode=_mode,
                    output_dir=out,
                    requested_ids=requested_ids,
                    succeeded=succeeded,
                    failed=failed,
                    skipped=skipped,
                    started_at=started_at,
                    status="in_progress",
                    current_dataset_id=dataset_id,
                )
                _write_manifest(out, manifest)
                continue

            try:
                if _should_stream_dataset_sync(dataset_id):
                    record = _download_large_dataset_to_parquet_with_retry(
                        dataset_id,
                        parquet_path=parquet_path,
                        max_retries=max_retries,
                        initial_backoff=initial_backoff,
                        max_backoff=max_backoff,
                    )
                else:
                    df = _download_dataset_with_retry(
                        dataset_id,
                        max_retries=max_retries,
                        initial_backoff=initial_backoff,
                        max_backoff=max_backoff,
                    )
                    try:
                        df.to_parquet(parquet_path, index=False)
                    except ImportError as exc:
                        raise RuntimeError(
                            "Parquet support requires 'pyarrow' or 'fastparquet'"
                        ) from exc

                    record = {
                        "id": dataset_id,
                        "rows": int(len(df)),
                        "columns": int(len(df.columns)),
                        "path": str(parquet_path),
                        "sync_method": "dataframe",
                    }

                succeeded.append(record)
                logger.info(
                    "%s %s -- saved %d rows x %d cols (%s)",
                    label,
                    dataset_id,
                    record.get("rows", 0),
                    record.get("columns", 0),
                    record.get("sync_method", "unknown"),
                )
            except Exception as exc:
                logger.error("%s %s -- FAILED: %s", label, dataset_id, exc)
                failed.append({"id": dataset_id, "reason": str(exc)})

            manifest = _build_manifest(
                mode=_mode,
                output_dir=out,
                requested_ids=requested_ids,
                succeeded=succeeded,
                failed=failed,
                skipped=skipped,
                started_at=started_at,
                status="in_progress",
                current_dataset_id=dataset_id,
            )
            _write_manifest(out, manifest)

            if delay > 0 and index < total:
                time.sleep(delay)

        elapsed_seconds = round(time.time() - started_at, 2)
        manifest = _build_manifest(
            mode=_mode,
            output_dir=out,
            requested_ids=requested_ids,
            succeeded=succeeded,
            failed=failed,
            skipped=skipped,
            started_at=started_at,
            status="completed",
            current_dataset_id=None,
        )
        manifest_path = _write_manifest(out, manifest)

        return {
            "output_dir": str(out.resolve()),
            "requested_count": total,
            "succeeded_count": len(succeeded),
            "failed_count": len(failed),
            "skipped_count": len(skipped),
            "succeeded": succeeded,
            "failed": failed,
            "skipped": skipped,
            "elapsed_seconds": elapsed_seconds,
            "manifest_path": str(manifest_path),
        }
    finally:
        _release_sync_lock(lock_path, lock_token)


def download_all_parquet(
    output_dir: str = "ons_datasets",
    resume: bool = False,
    delay: float = 2.0,
    max_retries: int = MAX_RETRIES,
    initial_backoff: float = INITIAL_BACKOFF,
    max_backoff: float = MAX_BACKOFF,
) -> Dict[str, Any]:
    """Download all available ONS datasets as parquet files.

    Args:
        output_dir: Target directory for parquet files.
        resume: Skip datasets that already exist as parquet files.
        delay: Seconds to sleep between dataset downloads.
        max_retries: Maximum retries per dataset.
        initial_backoff: Initial backoff in seconds.
        max_backoff: Maximum backoff in seconds.

    Returns:
        Dictionary with sync summary and manifest path.
    """
    dataset_ids = core.get_dataset_ids()
    summary = download_datasets_parquet(
        dataset_ids=dataset_ids,
        output_dir=output_dir,
        resume=resume,
        delay=delay,
        max_retries=max_retries,
        initial_backoff=initial_backoff,
        max_backoff=max_backoff,
        _mode="all",
    )
    summary["requested_mode"] = "all"
    summary["available_datasets_count"] = len(dataset_ids)
    return summary
