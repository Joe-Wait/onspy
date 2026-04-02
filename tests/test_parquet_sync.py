"""Tests for parquet synchronization helpers."""

import json
from pathlib import Path
from unittest.mock import Mock

import pandas as pd
import pytest

import onspy.parquet_sync as parquet_sync


def _fake_to_parquet(self, path, index=False):
    Path(path).write_text("parquet-bytes", encoding="utf-8")


def test_download_datasets_parquet_writes_manifest(tmp_path, monkeypatch):
    df = pd.DataFrame({"col": [1, 2, 3]})

    monkeypatch.setattr(parquet_sync.core, "download_dataset", lambda dataset_id: df)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", _fake_to_parquet)

    summary = parquet_sync.download_datasets_parquet(
        dataset_ids=["cpih01", "weekly-deaths-region"],
        output_dir=str(tmp_path),
        delay=0,
    )

    assert summary["requested_count"] == 2
    assert summary["succeeded_count"] == 2
    assert summary["failed_count"] == 0
    assert (tmp_path / "cpih01.parquet").exists()
    assert (tmp_path / "weekly-deaths-region.parquet").exists()

    manifest = json.loads((tmp_path / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["status"] == "completed"
    assert manifest["processed_count"] == 2
    assert manifest["requested_dataset_ids"] == ["cpih01", "weekly-deaths-region"]


def test_download_datasets_parquet_respects_resume(tmp_path, monkeypatch):
    df = pd.DataFrame({"col": [1]})

    (tmp_path / "cpih01.parquet").write_text("existing", encoding="utf-8")
    monkeypatch.setattr(parquet_sync.core, "download_dataset", lambda dataset_id: df)
    monkeypatch.setattr(pd.DataFrame, "to_parquet", _fake_to_parquet)

    summary = parquet_sync.download_datasets_parquet(
        dataset_ids=["cpih01", "lms"],
        output_dir=str(tmp_path),
        resume=True,
        delay=0,
    )

    assert summary["requested_count"] == 2
    assert summary["skipped_count"] == 1
    assert summary["succeeded_count"] == 1
    assert "cpih01" in summary["skipped"]
    assert (tmp_path / "lms.parquet").exists()


def test_download_datasets_parquet_requires_ids(tmp_path):
    with pytest.raises(ValueError, match="at least one"):
        parquet_sync.download_datasets_parquet([], output_dir=str(tmp_path))


def test_download_all_parquet_uses_dataset_catalog(monkeypatch):
    monkeypatch.setattr(parquet_sync.core, "get_dataset_ids", lambda: ["cpih01", "lms"])
    call_mock = Mock(
        return_value={
            "requested_count": 2,
            "succeeded_count": 2,
            "failed_count": 0,
            "skipped_count": 0,
        }
    )
    monkeypatch.setattr(parquet_sync, "download_datasets_parquet", call_mock)

    summary = parquet_sync.download_all_parquet(delay=0)

    assert summary["requested_mode"] == "all"
    assert summary["available_datasets_count"] == 2
    call_mock.assert_called_once()
