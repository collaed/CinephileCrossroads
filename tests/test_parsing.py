"""Pure function tests — normalize, fuzzy match, filename parsing."""
import pytest
from app import _normalize, _fuzzy_match, parse_movie_filename, detect_video_source, SOURCE_ICONS, LANG_NAMES


class TestNormalize:
    @pytest.mark.parametrize("input,expected_absent", [
        ("Movie_1080p_AAC", "1080p"),
        ("Movie_Blu-ray_x264", "x264"),
        ("Movie_(2003)_720p", "2003"),
        ("Movie_Webrip_DTS", "webrip"),
    ])
    def test_strips_tags(self, input, expected_absent):
        assert expected_absent not in _normalize(input)

    @pytest.mark.parametrize("a,b", [
        ("Strähl", "Straehl"),       # German ä→ae
        ("Götterdämmerung", "Gotterdammerung"),  # ö→oe, ä→ae
        ("La Jetée", "la jetee"),     # French accent
        ("Amélie", "amelie"),         # accent
    ])
    def test_transliterations(self, a, b):
        assert _normalize(a) == _normalize(b)

    @pytest.mark.parametrize("input,expected", [
        ("Romeo/Juliet", True),       # / → space
        ("Tom & Jerry", True),        # & → and
        ("fantozzi 2 2", "fantozzi"), # strip standalone numbers
        ("", ""),                      # empty
    ])
    def test_special_cases(self, input, expected):
        result = _normalize(input)
        if isinstance(expected, bool):
            assert "/" not in result and "&" not in result
        else:
            assert result == expected


class TestFuzzyMatch:
    @pytest.mark.parametrize("a,b,expected", [
        ("the return", "the return", 1.0),
        ("the return", "return the", 1.0),
        ("the return", "something else", 0.0),
        ("", "test", 0),
        ("test", "", 0),
        ("", "", 0),
    ])
    def test_basic(self, a, b, expected):
        assert _fuzzy_match(a, b) == expected

    def test_partial_match_between_0_and_1(self):
        score = _fuzzy_match("the return of the king", "return king")
        assert 0 < score < 1


class TestParseFilename:
    @pytest.mark.parametrize("filename,title,year,quality", [
        ("The_Return_(2003)_1080p_AAC.mp4", "The Return", "2003", "1080p"),
        ("Mad_Max_-_Fury_Road_(2015)_2160p.mkv", "Mad Max", "2015", "2160p"),
        ("Parasite_(2019)_1080p_AAC_Blu-ray.mp4", "Parasite", "2019", "1080p"),
    ])
    def test_movies(self, filename, title, year, quality):
        p = parse_movie_filename(filename)
        assert title in p["title"]
        assert p["year"] == year
        assert p["quality"] == quality

    @pytest.mark.parametrize("filename,show,season,episode", [
        ("Tehran.S03E02.Friend.Or.Foe.1080p.mkv", "Tehran", 3, 2),
        ("legion.s01e08.1080p.bluray.x265.mp4", "legion", 1, 8),
        ("The.Lost.Room.1x03.La.Caccia.mkv", None, None, None),  # 1x03 format not handled
    ])
    def test_tv_episodes(self, filename, show, season, episode):
        p = parse_movie_filename(filename)
        if show:
            assert p.get("is_tv") == True
            assert p["title"].lower() == show.lower()
            assert p["season"] == season
            assert p["episode"] == episode


class TestDetectVideoSource:
    @pytest.mark.parametrize("path,expected", [
        ("movie_Blu-ray/file.mkv", "bluray"),
        ("movie_brrip.mkv", "bluray"),
        ("BDMV/index.bdmv", "bluray"),
        ("VIDEO_TS/VIDEO_TS.IFO", "dvd"),
        ("movie_Webrip.mkv", "webrip"),
        ("movie_WEB-DL.mkv", "webdl"),
        ("movie_Remux.mkv", "remux"),
        ("movie_Remux_Blu-ray.mkv", "remux"),  # remux takes priority
        ("random_file.mkv", ""),
    ])
    def test_detection(self, path, expected):
        assert detect_video_source(path) == expected


class TestConstants:
    def test_all_common_languages(self):
        for code in ["eng", "ger", "deu", "fre", "fra", "spa", "ita", "jpn", "kor", "zho", "rus", "ara"]:
            assert code in LANG_NAMES

    def test_language_aliases(self):
        assert LANG_NAMES["ger"] == LANG_NAMES["deu"]
        assert LANG_NAMES["fre"] == LANG_NAMES["fra"]

    def test_source_icons_are_emoji(self):
        for k, v in SOURCE_ICONS.items():
            assert len(v) > 0, f"Empty icon for {k}"
