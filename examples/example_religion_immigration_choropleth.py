"""Brief example: religion + immigration metrics with a choropleth map.

Requires optional plotting dependencies:
    pip install geopandas matplotlib
"""

from __future__ import annotations

import geopandas as gpd
import matplotlib.pyplot as plt
import onspy


def build_metrics():
    religion = onspy.download_dataset("TS030")
    immigration = onspy.download_dataset("TS004")

    rel_code = "Lower tier local authorities Code"
    rel_name = "Lower tier local authorities"
    rel_category = "Religion (10 categories)"

    rel = (
        religion.pivot_table(
            index=[rel_code, rel_name],
            columns=rel_category,
            values="Observation",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename(columns={rel_code: "ltla_code", rel_name: "ltla_name"})
    )

    rel_dims = [c for c in rel.columns if c not in {"ltla_code", "ltla_name"}]
    rel["religion_total"] = rel[[c for c in rel_dims if c != "Does not apply"]].sum(axis=1)
    rel["non_christian_pct"] = 100.0 * (rel["religion_total"] - rel.get("Christian", 0)) / rel["religion_total"]
    rel["no_religion_pct"] = 100.0 * rel.get("No religion", 0) / rel["religion_total"]

    imm_code = "Lower Tier Local Authorities Code"
    imm_category = "Country of birth (12 categories)"

    imm = (
        immigration.pivot_table(
            index=[imm_code],
            columns=imm_category,
            values="Observation",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename(columns={imm_code: "ltla_code"})
    )

    imm_dims = [c for c in imm.columns if c != "ltla_code"]
    imm["birth_total"] = imm[[c for c in imm_dims if c != "Does not apply"]].sum(axis=1)
    imm["non_uk_born_pct"] = 100.0 * (imm["birth_total"] - imm.get("Europe: United Kingdom", 0)) / imm["birth_total"]

    return rel[["ltla_code", "ltla_name", "non_christian_pct", "no_religion_pct"]].merge(
        imm[["ltla_code", "non_uk_born_pct"]],
        on="ltla_code",
        how="inner",
    )


def main() -> None:
    metrics = build_metrics()

    print(
        "Correlation (non-Christian vs non-UK born):",
        round(metrics["non_christian_pct"].corr(metrics["non_uk_born_pct"]), 3),
    )
    print(
        "Correlation (no religion vs non-UK born):",
        round(metrics["no_religion_pct"].corr(metrics["non_uk_born_pct"]), 3),
    )

    boundary = onspy.download_boundary("lad_2021_uk_bfc", output_dir="examples/boundaries")
    geo = gpd.read_file(boundary["path"])
    geo = geo[geo["LAD21CD"].str.startswith(("E", "W"))]

    choropleth = geo.merge(metrics, left_on="LAD21CD", right_on="ltla_code", how="left")

    fig, ax = plt.subplots(figsize=(8, 10))
    choropleth.plot(
        column="non_uk_born_pct",
        cmap="YlGnBu",
        linewidth=0.15,
        edgecolor="white",
        legend=True,
        ax=ax,
    )
    ax.set_title("Non-UK born share by LTLA (England and Wales, Census 2021)")
    ax.set_axis_off()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
