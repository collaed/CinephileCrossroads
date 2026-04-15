"""Shared fixtures for all tests."""
import sys, os, json, tempfile, shutil
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

@pytest.fixture
def data_dir(tmp_path):
    """Create isolated test data directory with minimal structure."""
    os.makedirs(tmp_path / "users" / "testuser", exist_ok=True)
    os.makedirs(tmp_path / "thumbnails", exist_ok=True)
    json.dump({}, open(tmp_path / "titles.json", "w"))
    json.dump({}, open(tmp_path / "users" / "testuser" / "ratings.json", "w"))
    json.dump({}, open(tmp_path / "users" / "testuser" / "tmm_library.json", "w"))
    json.dump({}, open(tmp_path / "users" / "testuser" / "agent_data.json", "w"))
    json.dump({"tmdb": "", "omdb": "", "tvdb": ""}, open(tmp_path / "api_keys.json", "w"))
    json.dump([], open(tmp_path / "task_queue.json", "w"))
    return tmp_path

@pytest.fixture
def sample_library():
    """Library with a few titles for testing."""
    return {
        "tt0137523": {"path": "nfs://192.168.0.235/volume1/Movies/YiFY/TMM/1999/Fight_Club_(1999)_1080p_AAC_2.0_Mbps_Blu-ray/Fight_Club_(1999)_1080p_AAC_2.0_Mbps.mp4",
                      "video_height": 1080, "video_codec": "h264", "subtitles": [{"language": "eng"}]},
        "tt0468569": {"path": "nfs://192.168.0.235/volume1/Movies/V_HD/TMM/2008/The_Dark_Knight_(2008)_720p_AC3_4.0_Mbps/The_Dark_Knight.avi",
                      "video_height": 720, "video_codec": "h264"},
        "tt9138170": {"path": "nfs://192.168.0.235/volume1/Movies/YiFY/TMM/2020/Limbo_(2020)_1080p_AAC_2.3_Mbps_Webrip/Limbo_(2020)_1080p_AAC_2.3_Mbps.mp4",
                      "video_height": 1080, "video_codec": "h264"},
    }

@pytest.fixture
def sample_titles():
    return {
        "tt0137523": {"title": "Fight Club", "year": "1999", "genres": "Drama, Thriller", "tmdb_id": 550,
                      "keywords": ["twist ending", "underground", "violence"], "cast": "Brad Pitt, Edward Norton",
                      "directors": "David Fincher", "imdb_rating": 8.8},
        "tt0468569": {"title": "The Dark Knight", "year": "2008", "genres": "Action, Crime", "tmdb_id": 155,
                      "keywords": ["superhero", "joker", "gotham"], "cast": "Christian Bale, Heath Ledger",
                      "directors": "Christopher Nolan", "imdb_rating": 9.0},
        "tt9138170": {"title": "Limbo", "year": "2020", "genres": "Comedy, Drama", "tmdb_id": 680058,
                      "keywords": ["refugee", "scotland"], "imdb_rating": 7.2},
    }

@pytest.fixture
def sample_ratings():
    return {
        "tt0137523": {"rating": 9, "date": "2024-01-01"},
        "tt0468569": {"rating": 8, "date": "2024-01-02"},
    }
