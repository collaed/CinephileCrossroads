"""CinephileCrossroads Playwright E2E tests.
Install: pip install playwright && playwright install chromium
Run: python -m pytest tests/test_e2e.py --headed
"""
import pytest
from playwright.sync_api import sync_playwright, expect

BASE = "https://tools.ecb.pm/cinecross"
USER = "ecb"

@pytest.fixture(scope="session")
def browser():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        yield browser
        browser.close()

@pytest.fixture
def page(browser):
    page = browser.new_page()
    yield page
    page.close()

# --- Page Load Tests ---

class TestPageLoads:
    """Verify all pages return 200 and have the banner."""

    PAGES = [
        (f"/u/{USER}", "Ratings"),
        (f"/stats/{USER}", "Stats"),
        (f"/recs/{USER}", "Recommendations"),
        (f"/ai-friend/{USER}", "AI Friend"),
        (f"/library/{USER}", "Library"),
        (f"/library/browse/{USER}", "Browse"),
        (f"/tvshows/{USER}", "TV Shows"),
        (f"/scraper/{USER}", "Scraper"),
        (f"/library/org/{USER}", "Organize"),
        (f"/confirm/{USER}", "Confirm"),
        (f"/incoming/{USER}", "Incoming"),
        ("/feed", "Feed"),
        ("/compare/", "Compare"),
        (f"/contribute/{USER}", "Contribute"),
        ("/updates", "Updates"),
        ("/catalog", "Catalog"),
    ]

    @pytest.mark.parametrize("path,name", PAGES)
    def test_page_loads(self, page, path, name):
        resp = page.goto(BASE + path)
        assert resp.status == 200, f"{name} returned {resp.status}"

    @pytest.mark.parametrize("path,name", PAGES)
    def test_has_banner(self, page, path, name):
        page.goto(BASE + path)
        assert page.locator("text=Cinephile Crossroads").count() > 0, f"{name} missing banner"

    @pytest.mark.parametrize("path,name", PAGES)
    def test_has_nav(self, page, path, name):
        page.goto(BASE + path)
        assert page.locator(".nav-link").count() >= 4, f"{name} missing nav links"

# --- Functional Tests ---

class TestRatings:
    def test_search_filter(self, page):
        page.goto(f"{BASE}/u/{USER}")
        page.fill("#s", "matrix")
        page.wait_for_timeout(500)
        visible = page.locator("tbody tr:visible").count()
        assert visible > 0, "Search should find Matrix"

    def test_sort_by_rating(self, page):
        page.goto(f"{BASE}/u/{USER}")
        page.click("th:has-text('★')")
        page.wait_for_timeout(300)
        # Should not crash
        assert page.locator("tbody tr").count() > 0

class TestRecommendations:
    def test_has_categories(self, page):
        page.goto(f"{BASE}/recs/{USER}")
        assert page.locator("h4").count() >= 3, "Should have recommendation categories"

    def test_has_poster_cards(self, page):
        page.goto(f"{BASE}/recs/{USER}")
        assert page.locator(".poster-card").count() > 0, "Should have poster cards"

class TestLibrary:
    def test_progress_bars(self, page):
        page.goto(f"{BASE}/library/{USER}")
        assert page.locator("text=Sized").count() > 0
        assert page.locator("text=Hashed").count() > 0

    def test_agent_status(self, page):
        page.goto(f"{BASE}/library/{USER}")
        assert page.locator("text=Agent").count() > 0

class TestIncoming:
    def test_shows_files(self, page):
        page.goto(f"{BASE}/incoming/{USER}")
        # Should show movie or TV count
        has_content = page.locator("text=Movies").count() > 0 or page.locator("text=TV Episodes").count() > 0
        assert has_content, "Should show incoming file counts"

class TestAPI:
    def test_tasks_endpoint(self, page):
        resp = page.goto(f"{BASE}/api/tasks")
        assert resp.status == 200
        data = resp.json()
        assert "tasks" in data

    def test_api_info(self, page):
        resp = page.goto(f"{BASE}/api")
        assert resp.status == 200
        data = resp.json()
        assert "titles" in data
        assert "users" in data
