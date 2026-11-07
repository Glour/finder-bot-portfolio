"""Optional HTTP smoke tests against a dashboard using disposable services."""

import os

import httpx
import pytest


WEBAPP_URL = os.environ.get("FINDER_WEBAPP_URL")
pytestmark = pytest.mark.skipif(
    not WEBAPP_URL,
    reason="Set FINDER_WEBAPP_URL to a dashboard backed by disposable services",
)


def test_dashboard_and_empty_data_endpoints():
    assert WEBAPP_URL is not None
    with httpx.Client(base_url=WEBAPP_URL, timeout=10.0) as client:
        assert client.get("/").status_code == 200
        assert client.get("/api/mutes").json() == {"data": []}
        assert client.get("/api/search", params={"q": "abcdef"}).json() == {"ids": []}
        assert client.get("/api/prices").json()["data"] == []
        assert client.get("/api/spreads").json()["data"] == []
        response = client.get("/api/token", params={"id": "00000000-0000-0000-0000-000000000000"})
        assert response.status_code == 200
        assert response.json()["data"] is None
        assert client.get("/api/token").status_code == 400
