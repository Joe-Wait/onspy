---
name: "onspy-cli"
description: "CLI for the onspy MCP server. Access UK Office for National Statistics data: search datasets, download data, explore dimensions and code lists."
---

# onspy CLI

## Tool Commands

### list_datasets

List all available ONS datasets with metadata.

Returns datasets with id, title, description, keywords, release
frequency, and publication state.

```bash
onspy call-tool list_datasets --limit <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--limit` | string | no | JSON string |

### get_dataset_ids

Get a simple list of all dataset IDs.

Useful for scripting and discovering what datasets exist.

```bash
onspy call-tool get_dataset_ids
```

### get_dataset_info

Get detailed information about a specific dataset.

Returns title, description, keywords, release frequency, state,
next release date, latest version, and available editions.

Args:
    id: Dataset ID (e.g., 'cpih01', 'weekly-deaths-region')

```bash
onspy call-tool get_dataset_info --id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |

### get_editions

List available editions for a dataset.

Args:
    id: Dataset ID

```bash
onspy call-tool get_editions --id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |

### find_latest_version

Find the latest version across ALL editions of a dataset.

Some datasets have multiple editions (e.g., 'time-series', 'covid-19').
This checks all editions and returns the one with the highest version.

Args:
    id: Dataset ID

```bash
onspy call-tool find_latest_version --id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |

### download_dataset

Download a dataset from ONS.

Automatically finds the latest version if edition/version not specified.
Returns metadata, column names, and a preview of the first N rows.

Args:
    id: Dataset ID
    edition: Edition name (auto-detects if not provided)
    version: Version number (auto-detects if not provided)
    preview_rows: Number of rows to include in preview

```bash
onspy call-tool download_dataset --id <value> --edition <value> --version <value> --preview-rows <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |
| `--edition` | string | no | JSON string |
| `--version` | string | no | JSON string |
| `--preview-rows` | integer | no |  |

### get_dimensions

List available dimensions (filterable columns) for a dataset.

Dimensions are categorical columns you can filter by when querying
observations (e.g., 'geography', 'time', 'aggregate').

Args:
    id: Dataset ID

```bash
onspy call-tool get_dimensions --id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |

### get_dimension_options

Get available values for a specific dimension.

Use this to discover valid filter values before calling get_observations.
For example, get all geography codes or time periods.

Args:
    id: Dataset ID
    dimension: Dimension name (e.g., 'geography', 'time')
    limit: Maximum options to return

```bash
onspy call-tool get_dimension_options --id <value> --dimension <value> --limit <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |
| `--dimension` | string | yes |  |
| `--limit` | string | no | JSON string |

### get_observations

Get filtered observations from a dataset.

All dimensions must be specified. Use '*' as wildcard for any dimension.
Call get_dimensions() first to see required filters, then
get_dimension_options() to see valid values.

Example filters: {"geography": "K02000001", "aggregate": "cpih1dim1A0", "time": "*"}

Args:
    id: Dataset ID
    filters: Dictionary mapping dimension names to filter values
    edition: Edition name (auto-detects if not provided)
    version: Version number (auto-detects if not provided)

```bash
onspy call-tool get_observations --id <value> --filters <value> --edition <value> --version <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |
| `--filters` | object | yes | JSON string |
| `--edition` | string | no | JSON string |
| `--version` | string | no | JSON string |

### get_metadata

Get metadata for a dataset version.

Includes contact details, methodology, dimension descriptions, etc.

Args:
    id: Dataset ID
    edition: Edition name (auto-detects if not provided)
    version: Version number (auto-detects if not provided)

```bash
onspy call-tool get_metadata --id <value> --edition <value> --version <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |
| `--edition` | string | no | JSON string |
| `--version` | string | no | JSON string |

### search_dataset

Search for values within a dataset dimension.

Useful for finding specific codes or categories when you know
the dataset but not the exact dimension value.

Args:
    id: Dataset ID
    dimension: Dimension name to search (e.g., 'aggregate')
    query: Search query string
    edition: Edition name (auto-detects if not provided)
    version: Version number (auto-detects if not provided)

```bash
onspy call-tool search_dataset --id <value> --dimension <value> --query <value> --edition <value> --version <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |
| `--dimension` | string | yes |  |
| `--query` | string | yes |  |
| `--edition` | string | no | JSON string |
| `--version` | string | no | JSON string |

### list_codelists

List all available code lists (standardized categories).

Code lists provide standardized values for dimensions across datasets,
such as geography codes, time periods, or classifications.

```bash
onspy call-tool list_codelists
```

### get_codelist_info

Get detailed information about a code list.

Args:
    code_id: Code list ID (e.g., 'quarter', 'geography')

```bash
onspy call-tool get_codelist_info --code-id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--code-id` | string | yes |  |

### get_codelist_editions

Get available editions for a code list.

Args:
    code_id: Code list ID

```bash
onspy call-tool get_codelist_editions --code-id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--code-id` | string | yes |  |

### get_codes

Get codes for a specific code list edition.

Returns the standardized values and their meanings.

Args:
    code_id: Code list ID
    edition: Edition name

```bash
onspy call-tool get_codes --code-id <value> --edition <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--code-id` | string | yes |  |
| `--edition` | string | yes |  |

### get_code_info

Get details for a specific code.

Args:
    code_id: Code list ID
    edition: Edition name
    code: Code value

```bash
onspy call-tool get_code_info --code-id <value> --edition <value> --code <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--code-id` | string | yes |  |
| `--edition` | string | yes |  |
| `--code` | string | yes |  |

### get_dev_url

Get the ONS developer documentation URL.

```bash
onspy call-tool get_dev_url
```

### get_qmi_url

Get the Quality and Methodology Information (QMI) URL for a dataset.

Args:
    id: Dataset ID

```bash
onspy call-tool get_qmi_url --id <value>
```

| Flag | Type | Required | Description |
|------|------|----------|-------------|
| `--id` | string | yes |  |

## Utility Commands

```bash
onspy list-tools
onspy list-resources
onspy read-resource <uri>
onspy list-prompts
onspy get-prompt <name> [key=value ...]
```
