import pytest
from unittest.mock import Mock
from src.fetcher import Fetcher


class DummyResponse:
    def __init__(self, json_data, status_code=200):
        self._json = json_data
        self.status_code = status_code
        self.text = str(json_data)

    def json(self):
        return self._json


def test_fetch_org_repos_single_page(monkeypatch):
    session = Mock()
    session.get.side_effect = [DummyResponse([{"id": 1, "name": "r1"}]), DummyResponse([])]
    f = Fetcher(token=None, session=session)
    repos = f.fetch_org_repos("org")
    assert isinstance(repos, list)
    assert len(repos) == 1


def test_fetch_org_repos_error(monkeypatch):
    session = Mock()
    session.get.return_value = DummyResponse({"message": "error"}, status_code=404)
    f = Fetcher(token=None, session=session)
    with pytest.raises(Exception):
        f.fetch_org_repos("org")
