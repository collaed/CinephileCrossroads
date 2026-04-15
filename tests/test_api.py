"""API endpoint tests — run against live server.
Usage: pytest tests/test_api.py -v (requires server running)
"""
import pytest
import json
import urllib.request

BASE = "https://tools.ecb.pm/cinecross"
USER = "ecb"

def _get(path):
    try:
        r = urllib.request.urlopen(f"{BASE}{path}", timeout=10)
        ct = r.headers.get("Content-Type", "")
        body = r.read().decode()
        return r.status, json.loads(body) if "json" in ct else body
    except urllib.error.URLError:
        pytest.skip("Server unreachable")
    except Exception as e:
        return 0, str(e)

class TestAPIEndpoints:
    def test_api_info(self):
        status, data = _get("/api")
        assert status == 200
        assert "titles" in data
        assert "users" in data
        assert data["titles"] > 0

    def test_api_tasks(self):
        status, data = _get("/api/tasks")
        assert status == 200
        assert "tasks" in data
        assert isinstance(data["tasks"], list)

class TestPageLoads:
    """Every page must return 200 and contain the banner."""

    PAGES = [
        f"/u/{USER}", f"/stats/{USER}", f"/recs/{USER}", f"/ai-friend/{USER}",
        f"/library/{USER}", f"/library/browse/{USER}", f"/tvshows/{USER}",
        f"/scraper/{USER}", f"/library/org/{USER}", f"/confirm/{USER}",
        f"/incoming/{USER}", "/feed", "/compare/", f"/contribute/{USER}",
        "/updates", "/catalog",
    ]

    @pytest.mark.parametrize("path", PAGES)
    def test_returns_200(self, path):
        status, _ = _get(path)
        assert status == 200, f"{path} returned {status}"

    @pytest.mark.parametrize("path", PAGES)
    def test_has_banner(self, path):
        _, body = _get(path)
        assert "Cinephile Crossroads" in body, f"{path} missing banner"

    @pytest.mark.parametrize("path", PAGES)
    def test_has_nav(self, path):
        _, body = _get(path)
        assert "nav-link" in body, f"{path} missing navigation"

class TestPageContent:
    def test_ratings_has_table(self):
        _, body = _get(f"/u/{USER}")
        assert "<tbody>" in body

    def test_recs_has_poster_cards(self):
        _, body = _get(f"/recs/{USER}")
        assert "poster-card" in body

    def test_library_has_progress(self):
        _, body = _get(f"/library/{USER}")
        assert "Sized" in body
        assert "Hashed" in body

    def test_incoming_has_sections(self):
        _, body = _get(f"/incoming/{USER}")
        assert "Incoming" in body

    def test_title_detail(self):
        status, body = _get("/title/tt0137523")
        assert status == 200
        assert "Fight Club" in body
