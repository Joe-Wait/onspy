# onspy <a href='https://github.com/Joe-Wait/onspy'><img src='assets/logo.png' align="right" height="132" /></a>

A Python client for the Office of National Statistics (ONS) API.

[![PyPI version](https://badge.fury.io/py/onspy.svg)](https://badge.fury.io/py/onspy)
[![Python versions](https://img.shields.io/pypi/pyversions/onspy.svg)](https://pypi.org/project/onspy/)
[![License](https://img.shields.io/github/license/Joe-Wait/onspy.svg)](https://github.com/Joe-Wait/onspy/blob/main/LICENSE)

## Overview

`onspy` provides a simple interface to access data from the UK Office of National Statistics API.
It allows you to:

- Retrieve datasets and their metadata
- Search for specific data within datasets
- Browse code lists and dimensions
- Access quality and methodology information

## Installation

```bash
pip install onspy
```

## Quick Start

```python
import onspy

# List all available datasets
datasets = onspy.ons_datasets()
print(datasets.head())

# Get a list of dataset IDs
ids = onspy.ons_ids()
print(ids[:5])

# View dataset information
onspy.ons_desc("cpih01")

# Download data
df = onspy.ons_get("cpih01")
print(df.head())

# Get specific observations
obs = onspy.ons_get_obs("cpih01", geography="K02000001", aggregate="cpih1dim1A0", time="*")
print(obs.head())
```

## Examples

The examples folder contains examples demonstrating basic usage and more advanced usage, such as creating plots from datasets:

![Housing Costs Example](assets/weekly_deaths_by_geography.png)

![Housing Costs Example](assets/housing_costs_annual_change.png)

## Main Features

### Dataset Functions

- `ons_datasets()` - Get information about all available datasets
- `ons_ids()` - Get a list of all available dataset IDs
- `ons_desc(id)` - Print a description of a dataset
- `ons_editions(id)` - Get available editions for a dataset
- `ons_latest_edition(id)` - Get the latest edition name for a dataset
- `ons_latest_version(id)` - Get the latest version number for a dataset

### Data Retrieval Functions

- `ons_get(id, edition=None, version=None)` - Download a dataset
- `ons_get_obs(id, edition=None, version=None, **kwargs)` - Get specific observations
- `ons_dim(id, edition=None, version=None)` - Get dimensions for a dataset
- `ons_dim_opts(id, dimension, edition=None, version=None)` - Get dimension options
- `ons_meta(id, edition=None, version=None)` - Get metadata for a dataset

### Code List Functions

- `ons_codelists()` - Get a list of all available code lists
- `ons_codelist(code_id)` - Get details for a specific code list
- `ons_codelist_editions(code_id)` - Get editions for a code list
- `ons_codes(code_id, edition)` - Get codes for a specific edition of a code list
- `ons_code(code_id, edition, code)` - Get details for a specific code

### Search Functions

- `ons_search(id, name, query, edition=None, version=None)` - Search for a dataset

### Browser Functions

- `ons_browse()` - Open the ONS developer webpage in a browser
- `ons_browse_qmi(id)` - Open the QMI webpage for a dataset in a browser

## Advanced Usage Examples

### Getting Data from a Specific Version

```python
# Get an older version of a dataset
df = onspy.ons_get(id="cpih01", version="5")
```

### Filtering by Dimensions

```python
# Get dimensions for a dataset
dimensions = onspy.ons_dim("cpih01")
print(dimensions)

# Get options for a specific dimension
time_options = onspy.ons_dim_opts("cpih01", dimension="time")
print(time_options[:5])

# Get observations with specific dimension filters
obs = onspy.ons_get_obs(
    "cpih01",
    geography="K02000001",  # UK
    aggregate="cpih1dim1A0", # All items
    time="Oct-11"           # Specific time
)
print(obs)
```

### Working with Code Lists

```python
# Get all code lists
code_lists = onspy.ons_codelists()
print(code_lists[:5])

# Get details for a specific code list
quarter_cl = onspy.ons_codelist("quarter")
print(quarter_cl)

# Get editions for a code list
editions = onspy.ons_codelist_editions("quarter")
print(editions)

# Get codes for an edition
codes = onspy.ons_codes("quarter", "one-off")
print(codes)
```

## Building Bots

If you are building a bot using onspy, it is best practice to update the User-Agent header in `client.py`.

The API is [rate limited](https://developer.ons.gov.uk/bots/). Ensure your bot respects the limits to avoid being blocked.

## API Reference

For detailed API documentation, please refer to the [ONS Developer Hub](https://developer.ons.gov.uk/).

## License

Copyright (C) 2025 Joe Wait

This program is free software: you can redistribute it and/or modify
it under the terms of the GPL-3.0 License - see the LICENSE file for details.

## Contributing

Contributions are welcomed! Please feel free to raise an Issue or submit a Pull Request.

## Acknowledgements

This project was inspired by [onsr](https://github.com/kvasilopoulos/onsr) by [Kostas Vasilopoulos](https://github.com/kvasilopoulos).
