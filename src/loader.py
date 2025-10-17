"""Loader: persists transformed data into SQLite using SQLAlchemy ORM."""
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    DateTime,
    Float,
    MetaData,
    Table,
)
from sqlalchemy.orm import registry, sessionmaker
from typing import Optional
import pandas as pd
from .logger import get_logger

logger = get_logger(__name__)

mapper_registry = registry()
metadata = MetaData()

repositories_table = Table(
    "repositories",
    metadata,
    Column("repo_id", Integer, primary_key=True, index=True),
    Column("name", String, nullable=False),
    Column("full_name", String, nullable=False),
    Column("url", String),
    Column("created_at", DateTime),
    Column("updated_at", DateTime),
    Column("pushed_at", DateTime),
)

metrics_table = Table(
    "metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("repo_id", Integer, nullable=False),
    Column("stars", Integer),
    Column("forks", Integer),
    Column("open_issues", Integer),
    Column("age_days", Integer),
    Column("stars_per_day", Float),
)


class Loader:
    """Loader persists DataFrame rows into the SQLite database.

    It creates tables if missing and upserts repository and metrics rows.
    """

    def __init__(self, db_url: str = "sqlite:///./dataflow.db"):
        logger.info("Initializing Loader with db_url=%s", db_url)
        self.engine = create_engine(db_url, echo=False, future=True)
        metadata.create_all(self.engine)
        self.Session = sessionmaker(bind=self.engine)

    def load(self, df: pd.DataFrame) -> None:
        logger.info("Loading %d records into DB", len(df))
        if df.empty:
            logger.info("No records to load")
            return
        with self.engine.begin() as conn:
            for _, row in df.iterrows():
                # Upsert repository
                repo_ins = repositories_table.insert().prefix_with("OR REPLACE").values(
                    repo_id=int(row["repo_id"]),
                    name=row["name"],
                    full_name=row["full_name"],
                    url=row.get("url"),
                    created_at=row.get("created_at"),
                    updated_at=row.get("updated_at"),
                    pushed_at=row.get("pushed_at"),
                )
                conn.execute(repo_ins)
                # Insert metrics
                metrics_ins = metrics_table.insert().values(
                    repo_id=int(row["repo_id"]),
                    stars=int(row["stars"]),
                    forks=int(row.get("forks", 0)),
                    open_issues=int(row.get("open_issues", 0)),
                    age_days=int(row.get("age_days", 0)),
                    stars_per_day=float(row.get("stars_per_day", 0.0)),
                )
                conn.execute(metrics_ins)
        logger.info("Load complete")
