"""
Example: Visualizing UK Weekly Deaths Data

This example demonstrates how to use the onspy package to retrieve weekly deaths data
from the ONS API and create a visualization using matplotlib.
"""

import onspy
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import numpy as np


def get_weekly_deaths_data():
    """
    Retrieve weekly deaths data using the onspy package.

    Returns:
        Pandas DataFrame containing the data
    """
    print("Getting available datasets from ONS API...")
    datasets = onspy.list_datasets(limit=5)
    if not datasets.empty:
        print(f"Found datasets (showing first 5):")
        for _, row in datasets.iterrows():
            print(f"  {row.get('id', '?')}: {row.get('title', 'Untitled')}")

    dataset_id = "weekly-deaths-region"

    print(f"\nGetting editions for '{dataset_id}'...")
    editions = onspy.get_editions(dataset_id)
    if editions:
        print(f"Available editions: {', '.join(editions)}")

    print("Finding latest version across all editions...")
    latest = onspy.find_latest_version_across_editions(dataset_id)
    if latest is None:
        print("Could not determine latest version.")
        return None

    edition, version = latest
    print(f"Latest: edition='{edition}', version={version}")

    print("Getting dimensions...")
    dimensions = onspy.get_dimensions(dataset_id, edition, version)
    if dimensions:
        print(f"Dimensions: {', '.join(dimensions)}")

    print(f"\nDownloading data...")
    data = onspy.download_dataset(dataset_id, edition, version)

    if data.empty:
        print("Could not retrieve data.")
        return None

    print(f"Retrieved {len(data)} rows")
    return data


def prepare_data_for_plotting(data):
    """
    Prepare the weekly deaths data for plotting.

    Args:
        data: DataFrame containing the data

    Returns:
        DataFrame prepared for plotting
    """
    if data is None or data.empty:
        return None

    print("Preparing data for plotting...")
    df = data.copy()
    print(f"Columns: {', '.join(df.columns)}")

    necessary_columns = ["v4_1", "Time", "Geography", "week-number"]
    for col in necessary_columns:
        if col not in df.columns:
            print(f"ERROR: Required column '{col}' not found")
            similar = [c for c in df.columns if col.lower() in c.lower()]
            if similar:
                print(f"Similar columns: {', '.join(similar)}")
            return None

    df = df.dropna(subset=necessary_columns)

    def create_week_date(row):
        try:
            year = int(row["Time"])
            week_num = int(row["week-number"].replace("week-", ""))
            jan1 = datetime(year, 1, 1)
            jan1_weekday = jan1.weekday()
            first_monday = (
                jan1 if jan1_weekday == 0 else jan1 + timedelta(days=(7 - jan1_weekday))
            )
            return first_monday + timedelta(weeks=(week_num - 1))
        except Exception:
            return None

    df["WeekDate"] = df.apply(create_week_date, axis=1)
    df = df.dropna(subset=["WeekDate"])

    if len(df) == 0:
        print("ERROR: No valid data points after date conversion")
        return None

    aggregated = df.groupby(["WeekDate", "Geography"])["v4_1"].sum().reset_index()
    print(f"Aggregated: {aggregated.shape[0]} rows")
    return aggregated


def plot_weekly_deaths(data, title="Weekly Deaths by Geography", figsize=(14, 7)):
    if data is None or data.empty:
        print("No data available for plotting.")
        return None, None

    fig, ax = plt.subplots(figsize=figsize)
    filtered = data[data["Geography"] != "England and Wales"]
    colors = plt.cm.tab10(np.linspace(0, 1, len(filtered["Geography"].unique())))

    for i, (geo, group) in enumerate(filtered.groupby("Geography")):
        group = group.sort_values("WeekDate")
        ax.plot(
            group["WeekDate"],
            group["v4_1"],
            label=geo,
            color=colors[i],
            alpha=0.6,
            linewidth=1.5,
        )

    from matplotlib.dates import DateFormatter, YearLocator, MonthLocator

    ax.xaxis.set_major_locator(YearLocator())
    ax.xaxis.set_minor_locator(MonthLocator())
    ax.xaxis.set_major_formatter(DateFormatter("%Y"))
    ax.legend(fontsize=8, loc="upper left", bbox_to_anchor=(1, 1))
    ax.set_xlabel("Date")
    ax.set_ylabel("Number of Deaths")
    ax.set_title(title)
    ax.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    return fig, ax


def main():
    print("==== onspy Weekly Deaths Visualization Example ====\n")

    data = get_weekly_deaths_data()
    if data is None:
        print("ERROR: Could not retrieve data.")
        return

    print("\nDataset preview:")
    print(data.head(3))
    print(f"\nColumns: {', '.join(data.columns)}")

    processed = prepare_data_for_plotting(data)
    if processed is None or processed.empty:
        print("ERROR: Could not prepare data for plotting.")
        return

    fig, ax = plot_weekly_deaths(processed)
    if fig is not None:
        output_file = "weekly_deaths_by_geography.png"
        plt.savefig(output_file, dpi=300)
        print(f"\nPlot saved to: {output_file}")
        plt.show()

    print("\nExample completed.")


if __name__ == "__main__":
    main()
