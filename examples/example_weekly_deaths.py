"""
Example: Visualizing UK Weekly Deaths Data

This example demonstrates how to use the onspy package to retrieve weekly deaths data
from the ONS API and create a visualization using matplotlib.
"""

import onspy
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np


def format_date(date_str):
    """Convert ONS date format to datetime object."""
    try:
        # Try different date formats that might be in the data
        formats = ["%Y-%m-%d", "%d-%b-%y", "%d %B %Y", "%Y"]

        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue

        # If none of the formats work, try to extract year
        if date_str.isdigit() and len(date_str) == 4:
            return datetime(int(date_str), 1, 1)

        return None
    except Exception:
        return None


def get_weekly_deaths_data():
    """
    Retrieve weekly deaths data using the onspy package.

    Returns:
        Pandas DataFrame containing the data
    """
    # First, list available datasets to display capabilities of the client
    print("Getting available datasets from ONS API...")
    datasets = onspy.ons_datasets()

    if datasets is not None:
        print(f"Found {len(datasets)} datasets in the ONS API")

        # Display the first few datasets
        print("Sample of available datasets:")
        for i, (idx, row) in enumerate(datasets.head(3).iterrows()):
            print(
                f"  {i+1}. {row.get('id', 'Unknown ID')}: {row.get('title', 'Untitled')}"
            )

    # We're interested in weekly deaths data
    dataset_id = "weekly-deaths-region"

    # Get available editions for this dataset
    print(f"\nGetting available editions for dataset '{dataset_id}'...")
    editions = onspy.ons_editions(dataset_id)
    if editions:
        print(f"Available editions: {', '.join(editions)}")

        # Find the latest version across all editions
        print(f"Finding latest version across all editions...")
        latest = onspy.ons_find_latest_version_across_editions(dataset_id)

        if latest:
            edition, version = latest
            print(f"Found latest version: {version} in edition '{edition}'")

            # Get the dimensions of this dataset
            print(f"Getting dimensions for dataset...")
            dimensions = onspy.ons_dim(dataset_id, edition, version)
            if dimensions:
                print(f"Available dimensions: {', '.join(dimensions)}")

            # Now get the actual dataset using the edition and version we found
            print(
                f"\nGetting data for '{dataset_id}' (edition: {edition}, version: {version})..."
            )
            data = onspy.ons_get(id=dataset_id, edition=edition, version=version)

            if data is not None:
                print(f"Successfully retrieved data with {len(data)} rows")
                return data
            else:
                print("Could not retrieve data from the ONS API.")
        else:
            print("Could not determine latest version across editions.")

    print("Could not retrieve data using onspy client")
    return None


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

    # Make a copy to avoid modifying the original
    df = data.copy()

    print(f"Column names in dataset: {', '.join(df.columns)}")

    # Confirm the relevant columns exist
    necessary_columns = ["v4_1", "Time", "Geography", "week-number"]
    for col in necessary_columns:
        if col not in df.columns:
            print(f"ERROR: Required column '{col}' not found in dataset")
            # Try to find a similar column if exact match not found
            similar_cols = [c for c in df.columns if col.lower() in c.lower()]
            if similar_cols:
                print(f"Found similar columns: {', '.join(similar_cols)}")
            return None

    # Drop rows with missing data
    df = df.dropna(subset=["v4_1", "Time", "Geography", "week-number"])

    # Parse year and week number to create proper dates
    print("Creating weekly date values...")

    def create_week_date(row):
        """Convert year and week number to a datetime object."""
        try:
            year = int(row["Time"])
            # Extract numeric week number from format "week-XX"
            week_str = row["week-number"]
            week_num = int(week_str.replace("week-", ""))

            # Create a date from year and ISO week number
            # Using the first day of the specified week
            from datetime import datetime, timedelta

            # Create a date for Jan 1 of the year
            jan1 = datetime(year, 1, 1)
            # Find the day of the week (0 is Monday in isocalendar)
            jan1_weekday = jan1.weekday()
            # Find the first Monday of the year
            if jan1_weekday == 0:  # If Jan 1 is Monday
                first_monday = jan1
            else:
                first_monday = jan1 + timedelta(days=(7 - jan1_weekday))

            # Add the weeks (minus 1 since we're starting from week 1)
            target_date = first_monday + timedelta(weeks=(week_num - 1))
            return target_date
        except Exception as e:
            print(
                f"Error creating date for row {row['Time']}, {row['week-number']}: {e}"
            )
            return None

    # Apply the function to create a new Date column
    df["WeekDate"] = df.apply(create_week_date, axis=1)

    # Drop rows where date creation failed
    df = df.dropna(subset=["WeekDate"])

    if len(df) == 0:
        print("ERROR: No valid data points after date conversion")
        return None

    # Group by week date and geography to aggregate multiple entries
    print("Aggregating data by week date and geography...")
    aggregated_df = df.groupby(["WeekDate", "Geography"])["v4_1"].sum().reset_index()

    print(f"Data shape after aggregation: {aggregated_df.shape}")
    if len(aggregated_df) > 0:
        print("First 3 rows after aggregation:")
        print(aggregated_df[["WeekDate", "Geography", "v4_1"]].head(3))

    return aggregated_df


def plot_weekly_deaths(data, title="Weekly Deaths by Geography", figsize=(14, 7)):
    if data is None or data.empty:
        print("No data available for plotting.")
        return None, None

    fig, ax = plt.subplots(figsize=figsize)
    filtered_data = data[data["Geography"] != "England and Wales"]
    colors = plt.cm.tab10(np.linspace(0, 1, len(filtered_data["Geography"].unique())))

    # For each geography, plot the weekly time series
    for i, (geography_name, group) in enumerate(filtered_data.groupby("Geography")):
        # Sort by date for proper line plotting
        group = group.sort_values("WeekDate")

        # Plot the weekly data points
        ax.plot(
            group["WeekDate"],
            group["v4_1"],
            label=geography_name,
            color=colors[i],
            alpha=0.6,
            linewidth=1.5,
        )

    # Set x-axis date formatting
    from matplotlib.dates import DateFormatter, MonthLocator, YearLocator

    # Set major ticks to years and minor ticks to months
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
    """Main function to run the example."""
    print("==== onspy Weekly Deaths Visualization Example ====\n")

    # Get the data using the onspy client
    data = get_weekly_deaths_data()

    if data is not None:
        # Print dataset preview
        print("\nDataset preview:")
        print("--------------")
        print(data.head(3))
        print("...")

        # Print the column names to help diagnose potential issues
        print("\nAll columns in the dataset:")
        print(", ".join(data.columns))

        # Process the data (all geographies)
        processed_data = prepare_data_for_plotting(data)

        if processed_data is not None and not processed_data.empty:
            # Plot the data for all geographies
            fig, ax = plot_weekly_deaths(processed_data)

            # Save the plot
            if fig is not None:
                output_file = "weekly_deaths_by_geography.png"
                plt.savefig(output_file, dpi=300)
                print(f"\nPlot saved to: {output_file}")

                # Show the plot
                plt.show()

                # Also create a plot for each individual geography
                print("\nCreating individual plots for each geography...")

                for geography in processed_data["Geography"].unique():
                    if geography == "England and Wales":
                        continue  # Skip combined data

                    geo_data = processed_data[processed_data["Geography"] == geography]
                    geo_data = geo_data.sort_values("WeekDate")

                    # Create a simpler plot for individual geography
                    fig, ax = plt.subplots(figsize=(10, 5))
                    ax.plot(geo_data["WeekDate"], geo_data["v4_1"], linewidth=2)

                    ax.set_title(f"Weekly Deaths - {geography}")
                    ax.set_xlabel("Date")
                    ax.set_ylabel("Number of Deaths")
                    ax.grid(True, alpha=0.3)

                    # Save individual plot
                    geo_filename = (
                        f"weekly_deaths_{geography.replace(' ', '_').lower()}.png"
                    )
                    plt.tight_layout()
                    plt.savefig(geo_filename, dpi=300)
                    print(f"  - Saved {geo_filename}")
                    plt.close(fig)
        else:
            print(
                "ERROR: Could not prepare data for plotting. Check the data structure."
            )
    else:
        print("ERROR: Could not retrieve data from the ONS API.")

    print("\nExample completed.")


if __name__ == "__main__":
    main()
