"""
Interface to organize the data from the different chart types
"""
from __future__ import annotations

import pathlib
import polars as pl
import re

ROOT = pathlib.Path.cwd() / "data"



AS_ENUM = pl.Enum([
    "reg_down",
    "reg_up",
    "non_spin",
    "spin",
])

BID_ENUM = pl.Enum([
    "[-$150,-$100]",
    "(-$100,-$50]",
    "(-$50,-$15]",
    "(-$15, $0]",
    "($0, $15]",
    "($15, $50]",
    "($50, $100]",
    "($100, $200]",
    "($200, $500]",
    "($500, $1000]",
    "($1000, $2000]",
    "self_schedule",
])

ENERGY_ENUM = pl.Enum([
    "ifm",
    "ruc",
    "fmm",
    "rtd",
])

OFFER_ENUM = pl.Enum([
    "self_schedule",
    "[-$150,-$100]",
    "(-$100,-$50]",
    "(-$50,-$15]",
    "(-$15, $0]",
    "($0, $15]",
    "($15, $50]",
    "($50, $100]",
    "($100, $200]",
    "($200, $500]",
    "($500, $1000]",
    "($1000, $2000]",
])

SOC_ENUM = pl.Enum([
    "ifm",
    "ruc",
    "fmm",
    "rtd",
])



class EsrCollater(object):

    HYBRID_ROOT = ROOT / "hybrid"
    STORAGE_ROOT = ROOT / "storage"

    FILE_PATTERNS = {
        "fmm_as_awards": r"(\d{8})_FMM AS Awards.csv",
        "fmm_bids": r"(\d{8})_FMM Energy Bid In Capacity - Charge.csv",
        "fmm_offers": r"(\d{8})_FMM Energy Bid In Capacity - Discharge.csv",
        "ifm_as_awards": r"(\d{8})_IFM AS Awards.csv",
        "ifm_bids": r"(\d{8})_IFM Energy Bid In Capacity - Charge.csv",
        "ifm_offers": r"(\d{8})_IFM Energy Bid In Capacity - Discharge.csv",
        "energy_awards": r"(\d{8})_Total Energy Awards.csv",
        "state_of_charge": r"(\d{8})_Total State of Charge.csv",
    }

    def format_energy_awards(self, file_path: str) -> pl.LazyFrame:
        return (
            pl.scan_csv(
                file_path,
                schema_overrides={
                    "datetime": pl.Datetime,
                    "series_name": pl.String,
                    "value": pl.Float64,
                }
            )
            .drop("category")
            .with_columns(
                pl.col("series_name")
                .str.to_lowercase()
                .str.replace(r"\s", "_")
                .cast(ENERGY_ENUM),
            )
            .rename({
                "datetime": "timestamp",
                "series_name": "market",
                "value": "volume",
            })
        )

    def format_fmm_as_awards(self, file_path: str) -> pl.LazyFrame:
        return self.format_ifm_as_awards(file_path)

    def format_fmm_bids(self, file_path: str) -> pl.LazyFrame:
        return self.format_ifm_bids(file_path)

    def format_fmm_offers(self, file_path: str) -> pl.LazyFrame:
        return self.format_ifm_offers(file_path)

    def format_ifm_as_awards(self, file_path: str) -> pl.LazyFrame:
        return (
            pl.scan_csv(
                file_path,
                schema_overrides={
                    "datetime": pl.Datetime,
                    "series_name": pl.String,
                    "value": pl.Float64,
                }
            )
            .drop("category")
            .with_columns(
                pl.col("series_name")
                .str.to_lowercase()
                .str.replace(r"\s", "_")
                .cast(AS_ENUM),
            )
            .rename({
                "datetime": "timestamp",
                "series_name": "service",
                "value": "volume",
            })
        )

    def format_ifm_bids(self, file_path: str) -> pl.LazyFrame:
        return (
            pl.scan_csv(
                file_path,
                schema_overrides={
                    "datetime": pl.Datetime,
                    "series_name": pl.String,
                    "value": pl.Float64,
                }
            )
            .drop("category")
            .with_columns(
                pl.col("series_name")
                .str.to_lowercase()
                .replace({"self schedule": "self_schedule"})
                .cast(BID_ENUM),
            )
            .rename({
                "datetime": "timestamp",
                "series_name": "service",
                "value": "volume",
            })
        )

    def format_ifm_offers(self, file_path: str) -> pl.LazyFrame:
        return (
            pl.scan_csv(
                file_path,
                schema_overrides={
                    "datetime": pl.Datetime,
                    "series_name": pl.String,
                    "value": pl.Float64,
                }
            )
            .drop("category")
            .with_columns(
                pl.col("series_name")
                .str.to_lowercase()
                .replace({"self schedule": "self_schedule"})
                .cast(OFFER_ENUM),
            )
            .rename({
                "datetime": "timestamp",
                "series_name": "service",
                "value": "volume",
            })
        )

    def format_state_of_charge(self, file_path: str) -> pl.LazyFrame:
        return (
            pl.scan_csv(
                file_path,
                schema_overrides={
                    "datetime": pl.Datetime,
                    "series_name": pl.String,
                    "value": pl.Float64,
                }
            )
            .drop("category")
            .with_columns(
                pl.col("series_name")
                .str.to_lowercase()
                .replace({"self schedule": "self_schedule"})
                .cast(SOC_ENUM),
            )
            .rename({
                "datetime": "timestamp",
                "series_name": "market",
                "value": "volume",
            })
        )



    #
    # Primary method
    # 

    def collate(self, dtype: str, btype: str = "storage") -> pl.LazyFrame:
        try:
            pattern_str = self.FILE_PATTERNS[dtype]
            pattern = re.compile(pattern_str)
        except KeyError as exc:
            valid = ", ".join(map(lambda s: f"'{s}'", self.FILE_PATTERNS.keys()))
            raise ValueError(
                f"'{dtype}' is not a recognized dtype; must be one of {valid}"
            )

        if btype == "storage":
            root = self.STORAGE_ROOT
        elif btype == "hybrid":
            root = self.HYBRID_ROOT
        else:
            raise ValueError(
                f"'{btype}' is not a recognized battery type; must be one of "
                "'storage' or 'hybrid'"
            )

        # lol
        formatter = getattr(self, f"format_{dtype}")

        frames = []
        for csv in root.iterdir():
            if pattern.match(csv.name):
                lf = formatter(csv)
                frames.append(lf)

        if (dtype == "energy_awards") or (dtype == "state_of_charge"):
            by = ["timestamp", "market"]
        else:
            by = ["timestamp", "service"]

        return pl.concat(frames, how="vertical").sort(by)

if __name__ == "__main__":
    col = EsrCollater()

    SINK = pathlib.Path.cwd() / "data" / "formatted" / "storage"
    SINK.mkdir(parents=True, exist_ok=True)

    for dtype in col.FILE_PATTERNS.keys():
        lf = col.collate(dtype)
        lf.sink_parquet(SINK / f"{dtype}.parquet")
