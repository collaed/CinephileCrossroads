"""CinephileCrossroads comprehensive tests — inspired by Audiobookshelf patterns.
Run: python3 -m pytest tests/test_app.py -v
"""
import sys, os, json, tempfile, time
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# --- Normalize & Matching (like IncomingManager.parseFilename / titleSimilarity) ---

class TestNormalize:
    def test_strips_tags(self):
        from app import _normalize
        assert "1080p" not in _normalize("Movie_1080p_AAC")
        assert "bluray" not in _normalize("Movie_Blu-ray")
        assert "webrip" not in _normalize("Movie_Webrip")

    def test_strips_year(self):
        from app import _normalize
        assert "2003" not in _normalize("The_Return_(2003)_1080p")

    def test_strips_accents(self):
        from app import _normalize
        assert _normalize("La Jetée") == _normalize("la jetee")

    def test_german_transliteration(self):
        from app import _normalize
        assert _normalize("Strähl") == _normalize("Straehl")
        assert _normalize("Götterdämmerung") == _normalize("Gotterdammerung")

    def test_slash_and_ampersand(self):
        from app import _normalize
        assert "/" not in _normalize("Romeo/Juliet")
        assert "and" in _normalize("Tom & Jerry")

    def test_strips_standalone_numbers(self):
        from app import _normalize
        result = _normalize("fantozzi 2 2")
        assert result == "fantozzi"

    def test_empty_input(self):
        from app import _normalize
        assert _normalize("") == ""

class TestFuzzyMatch:
    def test_exact_match(self):
        from app import _fuzzy_match
        assert _fuzzy_match("the return", "the return") == 1.0

    def test_word_order_irrelevant(self):
        from app import _fuzzy_match
        assert _fuzzy_match("the return", "return the") == 1.0

    def test_no_match(self):
        from app import _fuzzy_match
        assert _fuzzy_match("the return", "something else") == 0.0

    def test_partial_match(self):
        from app import _fuzzy_match
        score = _fuzzy_match("the return of the king", "return king")
        assert 0 < score < 1

    def test_empty_input(self):
        from app import _fuzzy_match
        assert _fuzzy_match("", "test") == 0
        assert _fuzzy_match("test", "") == 0
        assert _fuzzy_match("", "") == 0

# --- Filename Parsing (like IncomingManager.parseFilename) ---

class TestParseFilename:
    def test_movie_with_year(self):
        from app import parse_movie_filename
        p = parse_movie_filename("The_Return_(2003)_1080p_AAC_2.3_Mbps.mp4")
        assert "Return" in p["title"]
        assert p["year"] == "2003"
        assert p["quality"] == "1080p"

    def test_tv_episode(self):
        from app import parse_movie_filename
        p = parse_movie_filename("Tehran.S03E02.Friend.Or.Foe.1080p.WEB-DL.mkv")
        assert p["is_tv"] == True
        assert p["title"] == "Tehran"
        assert p["season"] == 3
        assert p["episode"] == 2

    def test_tv_lowercase(self):
        from app import parse_movie_filename
        p = parse_movie_filename("legion.s01e08.1080p.bluray.x265.mp4")
        assert p["is_tv"] == True
        assert p["title"].lower() == "legion"
        assert p["season"] == 1
        assert p["episode"] == 8

    def test_no_year(self):
        from app import parse_movie_filename
        p = parse_movie_filename("SomeMovie_1080p.mkv")
        assert p["title"] != ""
        assert p["quality"] == "1080p"

    def test_dvd_structure(self):
        from app import parse_movie_filename
        p = parse_movie_filename("VIDEO_TS.IFO")
        # Should not crash
        assert p is not None

# --- Video Source Detection (like QualityManager) ---

class TestDetectVideoSource:
    def test_bluray(self):
        from app import detect_video_source
        assert detect_video_source("movie_Blu-ray/file.mkv") == "bluray"
        assert detect_video_source("movie_brrip.mkv") == "bluray"
        assert detect_video_source("BDMV/index.bdmv") == "bluray"

    def test_dvd(self):
        from app import detect_video_source
        assert detect_video_source("VIDEO_TS/VIDEO_TS.IFO") == "dvd"

    def test_web(self):
        from app import detect_video_source
        assert detect_video_source("movie_Webrip.mkv") == "webrip"
        assert detect_video_source("movie_WEB-DL.mkv") == "webdl"

    def test_remux(self):
        from app import detect_video_source
        assert detect_video_source("movie_Remux.mkv") == "remux"

    def test_unknown(self):
        from app import detect_video_source
        assert detect_video_source("random_file.mkv") == ""

    def test_remux_priority_over_bluray(self):
        from app import detect_video_source
        assert detect_video_source("movie_Remux_Blu-ray.mkv") == "remux"

# --- Convention Detection (like GroupingManager) ---

class TestLibraryConvention:
    def test_empty_library(self):
        from app import detect_library_convention
        conv = detect_library_convention("nonexistent_user")
        assert conv == {} or conv.get("total", 0) == 0

    def test_build_destination(self):
        from app import build_destination_path
        conv = {"separator": "_", "collection": "YiFY/TMM", "year_folder": True,
                "source_tag": True, "bitrate": False}
        dest = build_destination_path("Dune Part Two", "2024", "1080p", "", "", "Blu-ray", "mkv", conv)
        assert "YiFY/TMM/2024/" in dest
        assert "Dune_Part_Two" in dest
        assert "Blu-ray" in dest
        assert dest.endswith(".mkv")

    def test_no_year_folder(self):
        from app import build_destination_path
        conv = {"separator": "_", "collection": "Movies", "year_folder": False,
                "source_tag": False, "bitrate": False}
        dest = build_destination_path("Test", "2024", "1080p", "", "", "", "mp4", conv)
        assert "/2024/" not in dest

# --- Scoring (like RecommendationManager.scoreBook) ---

class TestScoring:
    def test_score_with_matching_keywords(self):
        from app import score_title
        profile = {"keywords": {"dragon": 5, "magic": 3}, "genres": {}, "directors": {}, "actors": {}, "writers": {}}
        title = {"keywords": ["dragon", "magic"], "genres": ""}
        assert score_title(title, profile) > 0

    def test_score_zero_no_match(self):
        from app import score_title
        profile = {"keywords": {"dragon": 5}, "genres": {}, "directors": {}, "actors": {}, "writers": {}}
        title = {"keywords": ["romance"], "genres": ""}
        assert score_title(title, profile) == 0

    def test_director_weight_higher(self):
        from app import score_title
        profile = {"keywords": {}, "genres": {}, "directors": {"Nolan": 5}, "actors": {"Actor": 5}, "writers": {}}
        title_dir = {"keywords": [], "genres": "", "directors": "Nolan", "cast": "", "writers": ""}
        title_act = {"keywords": [], "genres": "", "directors": "", "cast": "Actor", "writers": ""}
        # Director weight 2x vs actor 1.5x
        assert score_title(title_dir, profile) > score_title(title_act, profile)

    def test_score_empty_profile(self):
        from app import score_title
        profile = {"keywords": {}, "genres": {}, "directors": {}, "actors": {}, "writers": {}}
        title = {"keywords": ["anything"], "genres": "Drama"}
        assert score_title(title, profile) == 0

# --- Constants ---

class TestConstants:
    def test_lang_names_coverage(self):
        from app import LANG_NAMES
        for code in ["eng", "ger", "deu", "fre", "fra", "spa", "ita", "jpn", "kor", "zho", "rus"]:
            assert code in LANG_NAMES, f"Missing language: {code}"

    def test_lang_names_aliases(self):
        from app import LANG_NAMES
        assert LANG_NAMES["ger"] == LANG_NAMES["deu"]  # German
        assert LANG_NAMES["fre"] == LANG_NAMES["fra"]  # French

    def test_source_icons(self):
        from app import SOURCE_ICONS
        assert len(SOURCE_ICONS) >= 6
        assert all(isinstance(v, str) for v in SOURCE_ICONS.values())

# --- API Tests (like api-endpoints.test.js) ---

class TestAPI:
    """Test against live server if available."""
    BASE = "https://tools.ecb.pm/cinecross"
    _reachable = None

    def _check_reachable(self):
        if self._reachable is None:
            try:
                import urllib.request
                urllib.request.urlopen(self.BASE + "/api", timeout=5)
                TestAPI._reachable = True
            except:
                TestAPI._reachable = False
        if not self._reachable:
            raise Exception("SKIP: server unreachable")

    def _get(self, path):
        import urllib.request
        try:
            r = urllib.request.urlopen(f"{self.BASE}{path}", timeout=10)
            return r.status, json.loads(r.read()) if r.headers.get("Content-Type","").startswith("application/json") else r.read().decode()
        except Exception as e:
            return 0, str(e)

    def test_api_info(self):
        self._check_reachable()
        status, data = self._get("/api")
        assert status == 200
        assert "titles" in data
        assert "users" in data

    def test_api_tasks(self):
        self._check_reachable()
        status, data = self._get("/api/tasks")
        assert status == 200
        assert "tasks" in data

    def test_ratings_page(self):
        self._check_reachable()
        status, data = self._get("/u/ecb")
        assert status == 200
        assert "Cinephile Crossroads" in data

    def test_recs_page(self):
        self._check_reachable()
        status, data = self._get("/recs/ecb")
        assert status == 200
        assert "poster-card" in data

    def test_library_page(self):
        self._check_reachable()
        status, data = self._get("/library/ecb")
        assert status == 200
        assert "Sized" in data

    def test_incoming_page(self):
        self._check_reachable()
        status, data = self._get("/incoming/ecb")
        assert status == 200

    def test_nonexistent_returns_ratings(self):
        self._check_reachable()
        status, data = self._get("/nonexistent")
        assert status == 200  # Falls through to ratings

if __name__ == "__main__":
    passed = failed = 0
    for cls_name, cls in sorted(globals().items()):
        if isinstance(cls, type) and cls_name.startswith("Test"):
            print(f"\n{cls_name}:")
            obj = cls()
            for method_name in sorted(dir(obj)):
                if method_name.startswith("test_"):
                    try:
                        getattr(obj, method_name)()
                        print(f"  ✅ {method_name}")
                        passed += 1
                    except Exception as e:
                        print(f"  ❌ {method_name}: {e}")
                        failed += 1
    print(f"\n{'='*40}\n{passed} passed, {failed} failed")
