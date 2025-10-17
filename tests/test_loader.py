import tempfile
import os
import pandas as pd
from src.loader import Loader


def test_loader_writes_db(tmp_path):
    db_file = tmp_path / "test.db"
    db_url = f"sqlite:///{db_file}"
    loader = Loader(db_url=db_url)
    df = pd.DataFrame([
        {
            "repo_id": 1,
            "name": "r1",
            "full_name": "org/r1",
            "url": "http://",
            "created_at": pd.Timestamp("2020-01-01"),
            "updated_at": pd.Timestamp("2021-01-01"),
            "pushed_at": pd.Timestamp("2021-02-01"),
            "stars": 5,
            "forks": 1,
            "open_issues": 0,
            "age_days": 100,
            "stars_per_day": 0.05,
        }
    ])
    loader.load(df)
    assert db_file.exists()
