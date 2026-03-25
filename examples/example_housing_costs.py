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
    dataset_id = "cpih01"

    print(f"Finding latest version for '{dataset_id}'...")
    latest = onspy.find_latest_version_across_editions(dataset_id)
    if latest is None:
        print(f"Could not find latest version for {dataset_id}")
        return None

    edition, version = latest
    print(f"Latest: edition='{edition}', version={version}")

    print("Getting dimensions...")
    dimensions = onspy.get_dimensions(dataset_id, edition, version)
    if dimensions:
        print(f"Dimensions: {', '.join(dimensions)}")

    print(f"Downloading data...")
    data = onspy.download_dataset(dataset_id, edition, version)

    if data.empty:
        print("Could not retrieve data.")
        return None

    print(f"Retrieved {len(data)} rows")
    return data


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
    print(f"Columns: {', '.join(data.columns)}")

    required = ["v4_0", "mmm-yy", "cpih1dim1aggid"]
    missing = [col for col in required if col not in data.columns]
    if missing:
        print(f"ERROR: Missing columns: {', '.join(missing)}")
        return None

    # Filter for Owner Occupiers Housing Costs (CP042)
    housing = data[data["cpih1dim1aggid"] == "CP042"].copy()

    if len(housing) == 0:
        print("ERROR: No data found for Owner Occupiers Housing Costs (CP042)")
        values = data["cpih1dim1aggid"].unique()
        print(f"Available values: {', '.join(sorted(values)[:10])}...")
        return None

    print(f"Found {len(housing)} entries for Owner Occupiers Housing Costs")

    def parse_date(date_str):
        try:
            return datetime.strptime(date_str, "%b-%y")
        except ValueError:
            return None

    housing["date"] = housing["mmm-yy"].apply(parse_date)
    housing = housing.dropna(subset=["date"]).sort_values("date")

    # Calculate annual percentage changes
    housing.set_index("date", inplace=True)
    housing["v4_0_previous_year"] = housing["v4_0"].shift(12)
    housing["annual_pct_change"] = (
        (housing["v4_0"] - housing["v4_0_previous_year"])
        / housing["v4_0_previous_year"]
    ) * 100
    housing.reset_index(inplace=True)
    housing = housing.dropna(subset=["annual_pct_change"])

    print(f"Prepared {len(housing)} rows after calculating annual changes")
    return housing


def plot_housing_costs(
    data, title="Annual Change in Owner Occupiers' Housing Costs", figsize=(12, 6)
):
    if data is None or data.empty:
        print("No data available for plotting.")
        return None, None

    fig, ax = plt.subplots(figsize=figsize)
    ax.plot(data["date"], data["annual_pct_change"], linewidth=2)
    ax.axhline(y=0, color="gray", linestyle="--", alpha=0.7)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:.1f}%"))

    from matplotlib.dates import DateFormatter, YearLocator, MonthLocator

    ax.xaxis.set_major_locator(YearLocator())
    ax.xaxis.set_minor_locator(MonthLocator())
    ax.xaxis.set_major_formatter(DateFormatter("%Y"))

    ax.set_xlabel("Date")
    ax.set_ylabel("Annual Percentage Change")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)

    latest = data.iloc[-1]
    latest_date_str = latest["date"].strftime("%b %Y")
    latest_value = latest["annual_pct_change"]
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
    print("==== onspy Housing Costs Visualization Example ====\n")

    data = get_housing_costs_data()
    if data is None:
        print("ERROR: Could not retrieve data.")
        return

    print("\nDataset preview:")
    print(data.head(3))

    processed = prepare_housing_costs_data(data)
    if processed is None:
        print("ERROR: Could not prepare data.")
        return

    fig, ax = plot_housing_costs(processed)
    if fig is not None:
        output_file = "housing_costs_annual_change.png"
        plt.savefig(output_file, dpi=300)
        print(f"\nPlot saved to: {output_file}")
        plt.show()

    print("\nExample completed.")


if __name__ == "__main__":
    main()
