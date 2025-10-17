"""Transformer: convert raw GitHub API data to DataFrame and compute metrics."""
from typing import List, Dict, Any
import pandas as pd
from .logger import get_logger

logger = get_logger(__name__)


class Transformer:
    """Transforms raw repository dicts into processed metrics.

    Methods:
    - to_dataframe: create DataFrame with selected fields
    - add_growth_metric: compute a simple growth proxy (stars per day since created)
    """

    def to_dataframe(self, repos: List[Dict[str, Any]]) -> pd.DataFrame:
        logger.info("Transforming %d repo records into DataFrame", len(repos))
        df = pd.DataFrame(repos)
        if df.empty:
            return df

        # Select and rename fields we care about
        cols = {
            "id": "repo_id",
            "name": "name",
            "full_name": "full_name",
            "html_url": "url",
            "stargazers_count": "stars",
            "forks_count": "forks",
            "open_issues_count": "open_issues",
            "created_at": "created_at",
            "updated_at": "updated_at",
            "pushed_at": "pushed_at",
        }
        df = df[list(cols.keys())].rename(columns=cols)
        # Convert timestamps
        for tcol in ["created_at", "updated_at", "pushed_at"]:
            df[tcol] = pd.to_datetime(df[tcol], errors="coerce")
        return df

    def add_growth_metric(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Adding growth metric to DataFrame with %d rows", len(df))
        if df.empty:
            return df
        now = pd.Timestamp.utcnow()
        df = df.copy()
        df["age_days"] = (now - df["created_at"]).dt.days.clip(lower=1)
        df["stars_per_day"] = (df["stars"] / df["age_days"]).round(4)
        return df
