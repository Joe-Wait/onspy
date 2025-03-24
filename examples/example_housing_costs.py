"""
Example: Visualizing UK Owner-Occupier Housing Costs

This example demonstrates how to use the onspy package to retrieve housing costs data
from the ONS API and create a visualization showing annual changes in owner-occupier
housing costs over time.
"""

import onspy
import matplotlib.pyplot as plt
from datetime import datetime


def get_housing_costs_data():
    """
    Retrieve CPIH data using the onspy package.

    Returns:
        Pandas DataFrame containing the data
    """
    # Get CPIH01 dataset
    print("Getting CPIH datasets from ONS API...")
    dataset_id = "cpih01"

    # Find the latest version across all editions
    print(f"Finding latest version across all editions for dataset '{dataset_id}'...")
    latest = onspy.ons_find_latest_version_across_editions(dataset_id)

    if latest:
        edition, version = latest
        print(f"Found latest version: {version} in edition '{edition}'")

        # Get dimensions to understand the data structure
        dimensions = onspy.ons_dim(dataset_id, edition, version)
        if dimensions:
            print(f"Available dimensions: {', '.join(dimensions)}")

        # Get the data
        print(
            f"Downloading data for '{dataset_id}' (edition: {edition}, version: {version})..."
        )
        data = onspy.ons_get(id=dataset_id, edition=edition, version=version)

        if data is not None:
            print(f"Successfully retrieved data with {len(data)} rows")
            return data
        else:
            print("Could not retrieve data from the ONS API.")
    else:
        print(f"Could not find latest version across editions for {dataset_id}")

    return None


def prepare_housing_costs_data(data):
    """
    Filter and prepare housing costs data for plotting.

    Args:
        data: DataFrame containing CPIH data

    Returns:
        DataFrame prepared for plotting with dates and annual % changes
    """
    if data is None or data.empty:
        return None

    print("Preparing housing costs data...")

    # Print column names
    print(f"Column names in dataset: {', '.join(data.columns)}")

    # Check if required columns exist
    required_columns = ["v4_0", "mmm-yy", "cpih1dim1aggid"]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        print(f"ERROR: Required columns missing: {', '.join(missing_columns)}")

    # Filter for Owner Occupiers Housing Costs (CP042)
    housing_costs = data[data["cpih1dim1aggid"] == "CP042"].copy()

    if len(housing_costs) == 0:
        print("ERROR: No data found for Owner Occupiers Housing Costs (CP042)")
        # Look for what values exist in the column
        values = data["cpih1dim1aggid"].unique()
        print(
            f"Available values in cpih1dim1aggid: {', '.join(sorted(values)[:10])}{'...' if len(values) > 10 else ''}"
        )
        return None

    print(f"Found {len(housing_costs)} entries for Owner Occupiers Housing Costs")

    # Parse dates and sort chronologically
    def parse_date(date_str):
        try:
            # Format is "mmm-yy" like "Jan-20"
            return datetime.strptime(date_str, "%b-%y")
        except ValueError as e:
            print(f"Error parsing date '{date_str}': {e}")
            return None

    housing_costs["date"] = housing_costs["mmm-yy"].apply(parse_date)
    housing_costs = housing_costs.dropna(subset=["date"])
    housing_costs = housing_costs.sort_values("date")

    # Calculate annual percentage changes
    # First, set the index to date to make it easier to align values from previous years
    housing_costs.set_index("date", inplace=True)

    # Create a new column with the values shifted by 12 months
    housing_costs["v4_0_previous_year"] = housing_costs["v4_0"].shift(12)

    # Calculate the percentage change
    housing_costs["annual_pct_change"] = (
        (housing_costs["v4_0"] - housing_costs["v4_0_previous_year"])
        / housing_costs["v4_0_previous_year"]
    ) * 100

    # Reset index to make date a regular column again
    housing_costs.reset_index(inplace=True)

    # Drop rows with missing percentage change (first 12 months)
    housing_costs = housing_costs.dropna(subset=["annual_pct_change"])

    print(
        f"Prepared data with {len(housing_costs)} rows after calculating annual changes"
    )

    return housing_costs


def plot_housing_costs(
    data, title="Annual Change in Owner Occupiers' Housing Costs", figsize=(12, 6)
):
    """
    Create a plot of annual percentage changes in housing costs.

    Args:
        data: DataFrame with prepared housing costs data
        title: Plot title
        figsize: Figure size

    Returns:
        Figure and axis objects
    """
    if data is None or data.empty:
        print("No data available for plotting.")
        return None, None

    fig, ax = plt.subplots(figsize=figsize)

    # Plot annual percentage change
    ax.plot(data["date"], data["annual_pct_change"], linewidth=2)

    # Add a horizontal line at 0%
    ax.axhline(y=0, color="gray", linestyle="--", alpha=0.7)

    # Format the y-axis as percentage
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}%"))

    # Improve x-axis labels (show year and month)
    from matplotlib.dates import DateFormatter, YearLocator, MonthLocator

    ax.xaxis.set_major_locator(YearLocator())
    ax.xaxis.set_minor_locator(MonthLocator())
    ax.xaxis.set_major_formatter(DateFormatter("%Y"))

    # Add labels and title
    ax.set_xlabel("Date")
    ax.set_ylabel("Annual Percentage Change")
    ax.set_title(title)

    # Add grid for better readability
    ax.grid(True, alpha=0.3)

    # Add a text box with latest value
    latest = data.iloc[-1]
    latest_date = latest["date"]
    latest_value = latest["annual_pct_change"]
    latest_date_str = latest_date.strftime("%b %Y")
    ax.annotate(
        f"Latest ({latest_date_str}): {latest_value:.1f}%",
        xy=(0.02, 0.95),
        xycoords="axes fraction",
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="gray", alpha=0.8),
    )

    plt.xticks(rotation=45)

    plt.tight_layout()

    return fig, ax


def main():
    """Main function to run the example."""
    print("==== ONSpy Housing Costs Visualization Example ====\n")

    # Get the data
    data = get_housing_costs_data()

    if data is not None:
        # Print dataset overview
        print("\nDataset preview:")
        print("--------------")
        print(data.head(3))

        # Process the data
        processed_data = prepare_housing_costs_data(data)

        if processed_data is not None:
            # Create the plot
            fig, ax = plot_housing_costs(processed_data)

            if fig is not None:
                # Save the plot
                output_file = "housing_costs_annual_change.png"
                plt.savefig(output_file, dpi=300)
                print(f"\nPlot saved to: {output_file}")

                # Show the plot
                plt.show()
            else:
                print("ERROR: Could not create plot.")
        else:
            print("ERROR: Could not prepare data for plotting.")
    else:
        print("ERROR: Could not retrieve data from the ONS API.")

    print("\nExample completed.")


if __name__ == "__main__":
    main()
