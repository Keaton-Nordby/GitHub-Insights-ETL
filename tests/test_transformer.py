import pandas as pd
from src.transformer import Transformer


def test_to_dataframe_and_growth():
    repos = [
        {
            "id": 1,
            "name": "r1",
            "full_name": "org/r1",
            "html_url": "http://",
            "stargazers_count": 10,
            "forks_count": 2,
            "open_issues_count": 1,
            "created_at": "2020-01-01T00:00:00Z",
            "updated_at": "2021-01-01T00:00:00Z",
            "pushed_at": "2021-02-01T00:00:00Z",
        }
    ]
    t = Transformer()
    df = t.to_dataframe(repos)
    assert not df.empty
    df2 = t.add_growth_metric(df)
    assert "stars_per_day" in df2.columns
