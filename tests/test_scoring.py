"""Scoring and recommendation tests."""
import pytest
from app import score_title, build_taste_profile, detect_library_convention, build_destination_path


class TestScoring:
    @pytest.fixture
    def profile(self):
        return {
            "keywords": {"twist ending": 5, "underground": 3, "superhero": 4, "gotham": 2},
            "genres": {"Drama": 3, "Action": 2, "Thriller": 4},
            "directors": {"Christopher Nolan": 5, "David Fincher": 4},
            "actors": {"Brad Pitt": 3, "Christian Bale": 2},
            "writers": {"Jonathan Nolan": 2},
        }

    def test_matching_keywords_scores_positive(self, profile):
        title = {"keywords": ["twist ending", "underground"], "genres": "", "directors": "", "cast": "", "writers": ""}
        assert score_title(title, profile) > 0

    def test_no_match_scores_zero(self, profile):
        title = {"keywords": ["romance", "beach"], "genres": "Romance", "directors": "Unknown", "cast": "Nobody", "writers": ""}
        assert score_title(title, profile) == 0

    def test_director_weight_higher_than_actor(self, profile):
        title_dir = {"keywords": [], "genres": "", "directors": "Christopher Nolan", "cast": "", "writers": ""}
        title_act = {"keywords": [], "genres": "", "directors": "", "cast": "Brad Pitt", "writers": ""}
        assert score_title(title_dir, profile) > score_title(title_act, profile)

    def test_genre_weight_lower_than_keyword(self, profile):
        title_kw = {"keywords": ["twist ending"], "genres": "", "directors": "", "cast": "", "writers": ""}
        title_genre = {"keywords": [], "genres": "Drama", "directors": "", "cast": "", "writers": ""}
        assert score_title(title_kw, profile) > score_title(title_genre, profile)

    def test_empty_profile_always_zero(self):
        empty = {"keywords": {}, "genres": {}, "directors": {}, "actors": {}, "writers": {}}
        title = {"keywords": ["anything"], "genres": "Drama", "directors": "Someone", "cast": "Actor", "writers": "Writer"}
        assert score_title(title, empty) == 0

    def test_imdb_rating_boosts_score(self, profile):
        title_high = {"keywords": ["twist ending"], "genres": "", "directors": "", "cast": "", "writers": "", "imdb_rating": 9.0}
        title_low = {"keywords": ["twist ending"], "genres": "", "directors": "", "cast": "", "writers": "", "imdb_rating": 3.0}
        assert score_title(title_high, profile) > score_title(title_low, profile)


class TestTasteProfile:
    def test_builds_from_ratings(self, sample_ratings, sample_titles):
        profile = build_taste_profile(sample_ratings, sample_titles)
        assert "twist ending" in profile["keywords"]
        assert "David Fincher" in profile["directors"]
        assert "Brad Pitt" in profile["actors"]

    def test_ignores_low_ratings(self, sample_titles):
        ratings = {"tt0137523": {"rating": 3, "date": ""}}  # Below 6 threshold
        profile = build_taste_profile(ratings, sample_titles)
        assert len(profile["keywords"]) == 0

    def test_higher_rating_more_weight(self, sample_titles):
        ratings_9 = {"tt0137523": {"rating": 9, "date": ""}}
        ratings_6 = {"tt0137523": {"rating": 6, "date": ""}}
        profile_9 = build_taste_profile(ratings_9, sample_titles)
        profile_6 = build_taste_profile(ratings_6, sample_titles)
        assert profile_9["keywords"]["twist ending"] > profile_6["keywords"]["twist ending"]


class TestConvention:
    def test_build_with_year_folder(self):
        conv = {"separator": "_", "collection": "YiFY/TMM", "year_folder": True, "source_tag": True, "bitrate": False}
        dest = build_destination_path("Dune Part Two", "2024", "1080p", "", "", "Blu-ray", "mkv", conv)
        assert "YiFY/TMM/2024/" in dest
        assert "Dune_Part_Two" in dest
        assert dest.endswith(".mkv")

    def test_build_without_year_folder(self):
        conv = {"separator": "_", "collection": "Movies", "year_folder": False, "source_tag": False, "bitrate": False}
        dest = build_destination_path("Test", "2024", "1080p", "", "", "", "mp4", conv)
        assert "/2024/" not in dest

    def test_build_with_spaces(self):
        conv = {"separator": " ", "collection": "Movies", "year_folder": True, "source_tag": True, "bitrate": False}
        dest = build_destination_path("My Movie", "2024", "1080p", "", "", "Webrip", "mkv", conv)
        assert "My Movie" in dest
        assert "_" not in dest.split("/")[-1]
