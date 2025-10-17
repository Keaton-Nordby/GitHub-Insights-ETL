"""Reporter: generate daily summary reports from transformed data and DB."""
from typing import Optional
import pandas as pd
from pathlib import Path
from .logger import get_logger

logger = get_logger(__name__)


class Reporter:
    """Generates CSV and JSON summary reports in reports/ directory."""

    def __init__(self, reports_dir: str = "../reports"):
        self.reports_dir = Path(reports_dir)
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def write_csv(self, df: pd.DataFrame, name: str = "daily_summary.csv") -> Path:
        path = self.reports_dir / name
        logger.info("Writing CSV report to %s", path)
        df.to_csv(path, index=False)
        return path

    def write_json(self, df: pd.DataFrame, name: str = "daily_summary.json") -> Path:
        path = self.reports_dir / name
        logger.info("Writing JSON report to %s", path)
        df.to_json(path, orient="records", date_format="iso")
        return path

    def generate_summary(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Generating summary for %d rows", len(df))
        if df.empty:
            return df
        # Example summary: top 10 repos by stars
        summary = df.sort_values("stars", ascending=False).head(10)
        return summary[["repo_id", "name", "full_name", "stars", "stars_per_day"]]
