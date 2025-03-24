"""
Basic usage examples.

This script demonstrates how to use the main functionality of the onspy package.
"""

import onspy


def main():
    """Run basic examples of onspy package usage."""
    print("---ONS Datasets---")
    # Get all available datasets
    datasets = onspy.ons_datasets()
    if datasets is not None:
        print(f"Found {len(datasets)} datasets")
        print("First 5 datasets:")
        print(datasets[["id", "title"]].head())

    print("\n---ONS Dataset IDs---")
    # Get dataset IDs
    ids = onspy.ons_ids()
    if ids is not None:
        print(f"Found {len(ids)} dataset IDs")
        print("First 5 dataset IDs:", ids[:5])

    # Example dataset ID (assuming cpih01 exists)
    example_id = ids[0] if ids else "cpih01"

    print(f"\n---Dataset Description for {example_id}---")
    onspy.ons_desc(example_id)

    print(f"\n---Dataset Editions for {example_id}---")
    # Get editions for a dataset
    editions = onspy.ons_editions(example_id)
    if editions is not None:
        print(f"Found {len(editions)} editions for dataset {example_id}")
        print("Editions:", editions)

    print(f"\n---Dataset Dimensions for {example_id}---")
    # Get dimensions for a dataset
    dimensions = onspy.ons_dim(example_id)
    print(f"Dimensions for dataset {example_id}:", dimensions)

    # Only proceed if dimensions are available
    if dimensions:
        dim_name = dimensions[0]
        print(f"\n---Dimension Options for {dim_name}---")
        # Get options for a dimension
        options = onspy.ons_dim_opts(example_id, dimension=dim_name, limit=5)
        print(f"First 5 options for dimension {dim_name}:", options)

    print("\n---Code Lists---")
    # Get code lists
    codelists = onspy.ons_codelists()
    if codelists is not None:
        print(f"Found {len(codelists)} code lists")
        print("First 5 code lists:", codelists[:5])

        if codelists:
            example_code_id = codelists[0]
            print(f"\n---Code List Editions for {example_code_id}---")
            # Get editions for a code list
            code_editions = onspy.ons_codelist_editions(example_code_id)
            if code_editions is not None and len(code_editions) > 0:
                print(
                    f"Found {len(code_editions)} editions for code list {example_code_id}"
                )
                example_edition = code_editions[0].get("edition", "")
                print(f"First edition: {example_edition}")

                print(f"\n---Codes for {example_code_id}/{example_edition}---")
                # Get codes for a code list edition
                codes = onspy.ons_codes(example_code_id, example_edition)
                if codes is not None:
                    print(
                        f"Found {len(codes)} codes for code list {example_code_id}/{example_edition}"
                    )
                    if len(codes) > 0:
                        print("First code:", codes[0])

    print("\n---Browser Functions---")
    print("To browse the ONS developer website: onspy.ons_browse()")
    print(
        f'To browse QMI for dataset {example_id}: onspy.ons_browse_qmi("{example_id}")'
    )


if __name__ == "__main__":
    main()
