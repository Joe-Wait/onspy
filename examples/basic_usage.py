"""
Basic usage examples.

This script demonstrates how to use the main functionality of the onspy package.
"""

import onspy


def main():
    """Run basic examples of onspy package usage."""
    print("---ONS Datasets---")
    datasets = onspy.list_datasets()
    if not datasets.empty:
        print(f"Found {len(datasets)} datasets")
        print("First 5 datasets:")
        print(datasets[["id", "title"]].head())

    print("\n---ONS Dataset IDs---")
    ids = onspy.get_dataset_ids()
    print(f"Found {len(ids)} dataset IDs")
    print("First 5 dataset IDs:", ids[:5])

    example_id = ids[0] if ids else "cpih01"

    print(f"\n---Dataset Info for {example_id}---")
    info = onspy.get_dataset_info(example_id)
    print(f"  Title: {info['title']}")
    print(f"  State: {info['state']}")
    print(f"  Release Frequency: {info['release_frequency']}")
    print(f"  Description: {info['description'][:100]}...")

    print(f"\n---Dataset Editions for {example_id}---")
    editions = onspy.get_editions(example_id)
    print(f"Editions: {editions}")

    print(f"\n---Dataset Dimensions for {example_id}---")
    dimensions = onspy.get_dimensions(example_id)
    print(f"Dimensions: {dimensions}")

    if dimensions:
        dim_name = dimensions[0]
        print(f"\n---Dimension Options for {dim_name}---")
        options = onspy.get_dimension_options(example_id, dimension=dim_name, limit=5)
        print(f"First 5 options for dimension {dim_name}: {options}")

    print("\n---Code Lists---")
    codelists = onspy.list_codelists()
    print(f"Found {len(codelists)} code lists")
    print("First 5 code lists:", codelists[:5])

    if codelists:
        example_code_id = codelists[0]
        print(f"\n---Code List Editions for {example_code_id}---")
        code_editions = onspy.get_codelist_editions(example_code_id)
        if code_editions:
            print(f"Found {len(code_editions)} editions for {example_code_id}")
            example_edition = code_editions[0].get("edition", "")
            print(f"First edition: {example_edition}")

            print(f"\n---Codes for {example_code_id}/{example_edition}---")
            codes = onspy.get_codes(example_code_id, example_edition)
            print(f"Found {len(codes)} codes")
            if codes:
                print("First code:", codes[0])

    print("\n---URLs---")
    print(f"Developer docs: {onspy.get_dev_url()}")
    qmi = onspy.get_qmi_url(example_id)
    if qmi:
        print(f"QMI for {example_id}: {qmi}")


if __name__ == "__main__":
    main()
