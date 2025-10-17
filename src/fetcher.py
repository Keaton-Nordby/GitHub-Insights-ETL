"""Fetcher: retrieves GitHub repository data using requests."""
from typing import List, Dict, Any
import requests
from requests.adapters import HTTPAdapter, Retry
from .logger import get_logger

logger = get_logger(__name__)


class Fetcher:
    """Fetch repository metadata from GitHub for a single organization.

    Implements basic retry and error handling.
    """

    BASE_URL = "https://api.github.com"

    def __init__(self, token: str = None, session: requests.Session = None):
        self.token = token
        self.session = session or self._create_session()

    def _create_session(self) -> requests.Session:
        s = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        s.mount("https://", HTTPAdapter(max_retries=retries))
        if self.token:
            s.headers.update({"Authorization": f"token {self.token}"})
        s.headers.update({"Accept": "application/vnd.github.v3+json"})
        return s

    def fetch_org_repos(self, org: str, per_page: int = 100) -> List[Dict[str, Any]]:
        """Fetch public repositories for an organization. Returns a list of repo dicts.

        This is a minimal implementation that pages through results until exhausted.
        """
        logger.info("Fetching repos for org=%s", org)
        repos = []
        page = 1
        while True:
            url = f"{self.BASE_URL}/orgs/{org}/repos"
            params = {"per_page": per_page, "page": page, "type": "public"}
            resp = self.session.get(url, params=params, timeout=10)
            if resp.status_code != 200:
                logger.error("Failed to fetch repos: status=%s body=%s", resp.status_code, resp.text)
                resp.raise_for_status()
            data = resp.json()
            if not data:
                break
            repos.extend(data)
            page += 1
        logger.info("Fetched %d repos for org=%s", len(repos), org)
        return repos
