# Changelog

All notable changes to the onspy package will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.3] - 2026-04-06

### Changed

- Parquet sync now skips noncompliant datasets (instead of failing the run)
  when source artifacts are missing or unusable (for example no CSV download,
  empty tabular download, or binary/non-text payload at CSV URL).
- Sync summaries/manifests now include `skipped_details` with per-dataset
  reasons and skip type (`resume` or `noncompliant`).

### Added

- Additional parquet sync test coverage for noncompliant-dataset skip behavior.

## [0.2.2] - 2026-04-05

### Fixed

- Fixed intermittent `No such file or directory` rename failures in streaming
  parquet sync by using unique temporary file names and robust cleanup.
- Fixed mixed-type Arrow conversion failures (for example `soc` column in
  `ashe-tables-3`) by normalizing streamed CSV chunks to a stable string dtype
  before parquet writes.
- Suppressed repeated pandas dtype warnings in the large-dataset stream path by
  using deterministic chunk parsing options.

### Added

- Exclusive output-directory lock (`.onspy_parquet_sync.lock`) to prevent
  concurrent parquet sync runs from clobbering each other in the same target
  directory.
- Lock conflict and stale-lock recovery coverage in parquet sync tests.

## [0.2.1] - 2026-04-05

### Added

- Memory-safe streaming parquet export path for very large CSV-backed datasets
  (`ashe-*`, `TS*`, `RM*`, `ST*`) with temp-file writes and atomic replace.
- Sync metadata now records `sync_method` (`csv_stream` or `dataframe`) per
  succeeded dataset in parquet sync summaries/manifests.
- New client regression test to ensure streamed HTTP requests do not eagerly
  read response bodies.

### Changed

- `ONSClient.make_request(..., stream=True)` no longer accesses
  `response.content`, preserving true streaming behavior.
- `utils.read_csv` now reads CSV responses from a stream and raises typed ONS
  HTTP/network exceptions for failed CSV fetches.
- Parquet sync retry classification now handles transient stream parsing
  failures (for example, `No columns to parse from file`) as retryable.

### Fixed

- Fixed process OOM kills during `download_all_parquet` on very large datasets
  by avoiding full in-memory DataFrame materialization in the large-dataset
  sync path.

## [0.2.0] - 2026-04-02

### Added

- MCP server architecture as the runtime source of truth for tools and prompts
- Unified `core.py` function layer used by MCP, CLI, and Python library usage
- New MCP parquet sync tools:
  - `download_all_parquet`
  - `download_datasets_parquet`
- New MCP boundary tools:
  - `list_boundaries`
  - `download_boundary`
- New Python parquet sync APIs:
  - `download_all_parquet(...)`
  - `download_datasets_parquet(...)`
- New detailed dimension options API/tool:
  - `get_dimension_options_detailed(...)`
- Updated `download_all.py` compatibility wrapper with specific dataset support (`--dataset-id`)
- New tests for parquet sync and server tool wrappers

### Changed

- Major client refactor from legacy module split (`datasets.py`, `get.py`, `search.py`, etc.)
  to a consolidated API centered on `core.py`
- Public API naming moved away from legacy `ons_*` function names to explicit names like
  `list_datasets`, `download_dataset`, `get_observations`, and `search_dataset`
- `README.md` rewritten for MCP-first and parquet + DuckDB workflows
- `SKILL.md` updated to include full parquet download and local DuckDB analysis instructions
- CLI now includes parquet sync commands via `onspy call-tool ...`
- `download_datasets_parquet` CLI input simplified to repeatable `--dataset-id` flags
- `get_observations` wildcard behavior made deterministic:
  table-backed datasets support `*`, API-only datasets require explicit values
- HTTP client now raises typed request/connection exceptions instead of returning silent `None`
- Parquet sync manifest now updates incrementally during long-running downloads
- Package metadata and runtime dependencies updated for CLI/MCP/parquet workflow
- Minimum supported Python version raised to 3.10 to match MCP/CLI type syntax
- CI workflow updated to current Python version matrix and coverage target

### Removed

- Legacy unit tests tied to deleted pre-refactor modules and naming conventions
- Tracked compiled Python bytecode artifacts (`__pycache__/*.pyc`) from the repository

### Fixed

- Corrected historical ordering and dates in changelog entries

## [0.1.3] - 2026-02-16

### Fixed

- changed ping url to ons.gov.uk

## [0.1.2] - 2025-04-10

### Fixed

- fix ons_get_latest function name

## [0.1.1] - 2025-03-30

### Added

- ons_get_latest function
- Wellbeing example
- Update readme

## [0.1.0] - 2025-03-25

### Added

- Initial release of onspy package
- Functions to access and interact with ONS datasets
- Functions to search and browse code lists
- Support for downloading CSV data via the ONS API
- Documentation and examples
- Test suite with unit and integration tests
