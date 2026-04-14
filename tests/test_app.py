"""CinephileCrossroads whitebox tests — run with: python3 -m pytest tests/test_app.py"""
import sys, os, json, tempfile
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock DATA_DIR for testing
TEST_DIR = tempfile.mkdtemp()
os.environ["CINECROSS_DATA"] = TEST_DIR

import importlib

def setup_test_data():
    """Create minimal test data."""
    os.makedirs(f"{TEST_DIR}/users/testuser", exist_ok=True)
    os.makedirs(f"{TEST_DIR}/imdb_datasets", exist_ok=True)
    json.dump({}, open(f"{TEST_DIR}/titles.json", "w"))
    json.dump({}, open(f"{TEST_DIR}/users/testuser/ratings.json", "w"))
    json.dump({}, open(f"{TEST_DIR}/users/testuser/tmm_library.json", "w"))
    json.dump({}, open(f"{TEST_DIR}/users/testuser/agent_data.json", "w"))
    json.dump({"tmdb": "", "omdb": "", "tvdb": ""}, open(f"{TEST_DIR}/api_keys.json", "w"))

setup_test_data()

# --- Unit Tests ---

def test_normalize():
    from app import _normalize
    assert _normalize("The_Return_(2003)_1080p_AAC") == "the return"
    assert _normalize("Strähl") == "strahl"  # accent stripped
    assert _normalize("Straehl") == "strahl"  # ae -> a transliteration
    assert _normalize("L'été") == _normalize("lete")  # French accent
    assert _normalize("Blu-ray") == "blu ray"

def test_fuzzy_match():
    from app import _fuzzy_match
    assert _fuzzy_match("the return", "the return") == 1.0
    assert _fuzzy_match("the return", "return the") == 1.0  # word order doesn't matter
    assert _fuzzy_match("the return", "something else") == 0.0
    assert _fuzzy_match("fantozzi", "fantozzi") == 1.0

def test_parse_movie_filename():
    from app import parse_movie_filename
    # Movie
    p = parse_movie_filename("The_Return_(2003)_1080p_AAC_2.3_Mbps_Blu-ray/The_Return_(2003)_1080p.mp4")
    assert p["title"].strip() == "The Return"
    assert p["year"] == "2003"
    assert p["quality"] == "1080p"
    # TV episode
    p = parse_movie_filename("Tehran.S03E02.Friend.Or.Foe.1080p.WEB-DL.mkv")
    assert p["is_tv"] == True
    assert p["title"] == "Tehran"
    assert p["season"] == 3
    assert p["episode"] == 2

def test_detect_video_source():
    from app import detect_video_source
    assert detect_video_source("movie_Blu-ray/file.mkv") == "bluray"
    assert detect_video_source("movie_Webrip/file.mp4") == "webrip"
    assert detect_video_source("movie_DVD/VIDEO_TS.IFO") == "dvd"
    assert detect_video_source("movie_Remux/file.mkv") == "remux"
    assert detect_video_source("random/file.mkv") == ""

def test_detect_library_convention():
    from app import detect_library_convention
    # With empty library, should return empty
    conv = detect_library_convention("testuser")
    assert conv == {} or conv.get("total", 0) == 0

def test_build_destination_path():
    from app import build_destination_path
    conv = {"separator": "_", "collection": "YiFY/TMM", "year_folder": True,
            "source_tag": True, "bitrate": False}
    dest = build_destination_path("Dune Part Two", "2024", "1080p", "", "", "Blu-ray", "mkv", conv)
    assert "YiFY/TMM/2024/" in dest
    assert "Dune_Part_Two" in dest
    assert "Blu-ray" in dest
    assert dest.endswith(".mkv")

def test_lang_names():
    from app import LANG_NAMES
    assert LANG_NAMES["eng"] == "English"
    assert LANG_NAMES["ger"] == "German"
    assert LANG_NAMES["deu"] == "German"
    assert LANG_NAMES["fre"] == "French"
    assert LANG_NAMES["fra"] == "French"

def test_source_icons():
    from app import SOURCE_ICONS
    assert SOURCE_ICONS["bluray"] == "💿"
    assert SOURCE_ICONS["dvd"] == "📀"
    assert SOURCE_ICONS["remux"] == "💎"

if __name__ == "__main__":
    for name, func in list(globals().items()):
        if name.startswith("test_"):
            try:
                func()
                print(f"  ✅ {name}")
            except Exception as e:
                print(f"  ❌ {name}: {e}")
