"""Main pipeline runner for DataFlow MVP."""
import os
from pathlib import Path
import sys

# Ensure project root is on sys.path so `python src/main.py` can import `src` package
project_root = Path(__file__).resolve().parents[1]
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import yaml
from dotenv import load_dotenv
from src.logger import get_logger
from src.fetcher import Fetcher
from src.transformer import Transformer
from src.loader import Loader
from src.reporter import Reporter

logger = get_logger(__name__)


class Pipeline:
    """Orchestrates the ETL steps."""

    def __init__(self, config_path: str = "../config/config.yaml"):
        # Load environment from config/.env relative to project root
        load_dotenv(project_root / "config" / ".env")
        self.config = self._load_config(str(project_root / "config" / "config.yaml"))
        self.github_token = os.getenv("GITHUB_TOKEN")
        self.db_path = os.getenv("DB_PATH", "./dataflow.db")
        self.orgs = os.getenv("ORGS", "python").split(',')

        self.fetcher = Fetcher(token=self.github_token)
        self.transformer = Transformer()
        self.loader = Loader(db_url=f"sqlite:///{self.db_path}")
        self.reporter = Reporter(reports_dir=str(Path(__file__).resolve().parents[1] / "reports"))

    def _load_config(self, path: str):
        p = Path(path)
        if not p.exists():
            logger.warning("Config file %s not found, using defaults", p)
            return {}
        return yaml.safe_load(p.read_text()) or {}

    def run(self):
        logger.info("Pipeline started")
        all_repos = []
        for org in self.orgs:
            org = org.strip()
            try:
                repos = self.fetcher.fetch_org_repos(org)
                all_repos.extend(repos)
            except Exception:
                logger.exception("Error fetching org=%s", org)
        df = self.transformer.to_dataframe(all_repos)
        df = self.transformer.add_growth_metric(df)
        self.loader.load(df)
        summary = self.reporter.generate_summary(df)
        self.reporter.write_csv(summary)
        self.reporter.write_json(summary)
        logger.info("Pipeline finished")


if __name__ == "__main__":
    Pipeline().run()
