#!/usr/bin/env python3
"""
CineCross — Self-hosted multi-user movie & TV ratings dashboard.

Aggregates data from IMDB, TMDB, OMDB, TVDB, and Trakt into a single searchable
interface with streaming availability and taste-based recommendations.

Architecture:
    - titles.json: Shared title metadata (posters, scores, keywords, streaming)
    - users/<name>/ratings.json: Per-user ratings {imdb_id: {rating, date}}
    - users/<name>/trakt_token.json: Per-user Trakt OAuth tokens
    - users/<name>/tmm_library.json: Per-user local library (tinyMediaManager)
    - catalog.json: Full streaming catalog for the configured country

Recommendation engine:
    1. Builds a weighted taste profile from user's highly-rated titles
       using TMDB keywords and genres (rating 6+ contributes, weighted by score)
    2. Scores all unrated titles in the store against the profile
    3. Filters to titles available on user's streaming services
    4. Returns top matches sorted by taste score

All heavy operations (enrichment, catalog fetch, Trakt sync) run in background
threads with progress tracking via /jobs API endpoint.

Zero external Python dependencies — pure stdlib + Docker.
"""
import csv, json, os, io, time, urllib.request, urllib.parse, threading, math
from http.server import HTTPServer, BaseHTTPRequestHandler

# ── Config ────────────────────────────────────────────────────────────
TMDB_KEY = os.environ.get("TMDB_KEY", "")
OMDB_KEY = os.environ.get("OMDB_KEY", "")
TVDB_KEY = os.environ.get("TVDB_KEY", "")
TRAKT_ID = os.environ.get("TRAKT_ID", "")
TRAKT_SECRET = os.environ.get("TRAKT_SECRET", "")
TRAKT_REDIRECT = os.environ.get("TRAKT_REDIRECT", "https://your-domain.com/trakt/callback")
WATCH_COUNTRY = os.environ.get("WATCH_COUNTRY", "LU")  # ISO 3166-1 for streaming availability
DEFAULT_PROVIDERS = {"Netflix", "Amazon Prime Video", "Disney Plus", "Max"}  # Defaults for new users  # User's subscriptions
DATA_DIR = "/data"
TITLES_FILE = f"{DATA_DIR}/titles.json"
CATALOG_FILE = f"{DATA_DIR}/catalog.json"
CATALOG_PREV = f"{DATA_DIR}/catalog_prev.json"
KEYS_FILE = f"{DATA_DIR}/api_keys.json"
PORT = 8000
AGENT_TOKEN = os.environ.get("AGENT_TOKEN", "")
BASE = "/imdb"
PROVIDER_ICONS = {"Netflix": "🟥", "Amazon Prime Video": "📦", "Disney Plus": "🏰", "Max": "🟪", "Apple TV Plus": "🍎"}
LU_PROVIDER_IDS = {"Netflix": 8, "Amazon Prime Video": 119, "Disney Plus": 337}  # TMDB provider IDs per country

# ── Background jobs ───────────────────────────────────────────────────
_jobs = {}
_job_lock = threading.Lock()

def start_job(name, fn, *args):
    jid = f"job_{int(time.time()*1000)}"
    with _job_lock:
        _jobs[jid] = {"status": "running", "name": name, "progress": 0, "total": 0, "message": "Starting..."}
    def wrapper():
        try:
            fn(jid, *args)
            with _job_lock: _jobs[jid]["status"] = "done"; _jobs[jid]["message"] = "Complete"
        except Exception as e:
            with _job_lock: _jobs[jid]["status"] = "error"; _jobs[jid]["message"] = str(e)
            print(f"Job {name} error: {e}")
    threading.Thread(target=wrapper, daemon=True).start()
    return jid

def job_progress(jid, progress, total, message=""):
    with _job_lock:
        if jid in _jobs: _jobs[jid].update({"progress": progress, "total": total, "message": message})

def active_job():
    with _job_lock:
        for jid, j in _jobs.items():
            if j["status"] == "running": return jid, j
    return None, None

def get_jobs():
    with _job_lock: return dict(_jobs)

# ── API helpers ───────────────────────────────────────────────────────
def api_get(url, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=10) as r: return json.loads(r.read())
    except Exception as e:
        if "401" not in str(e): print(f"API error {url[:80]}: {e}")
        return None

def api_post(url, data, headers=None):
    req = urllib.request.Request(url, data=json.dumps(data).encode(),
        headers={"User-Agent": "Mozilla/5.0", **(headers or {}), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r: return json.loads(r.read())
    except Exception as e: print(f"API POST error {url[:80]}: {e}"); return None

# ── Title store (shared across users) ─────────────────────────────────
def load_titles():
    """Load shared title metadata. Keyed by IMDB ID (e.g. tt1234567)."""
    if os.path.exists(TITLES_FILE):
        return json.load(open(TITLES_FILE))
    return {}

def save_titles(titles):
    os.makedirs(DATA_DIR, exist_ok=True)
    json.dump(titles, open(TITLES_FILE, "w"))

def get_title(titles, imdb_id):
    """Get metadata for a single title from the shared store."""
    return titles.get(imdb_id, {})

def set_title(titles, imdb_id, data):
    if imdb_id not in titles: titles[imdb_id] = {}
    titles[imdb_id].update(data)

# ── User store ────────────────────────────────────────────────────────
def user_dir(user):
    d = f"{DATA_DIR}/users/{user}"
    os.makedirs(d, exist_ok=True)
    return d

def load_user_ratings(user):
    """Load user ratings. Returns {imdb_id: {rating: int, date: str}}."""
    f = f"{user_dir(user)}/ratings.json"
    if os.path.exists(f): return json.load(open(f))
    return {}

def save_user_ratings(user, ratings):
    json.dump(ratings, open(f"{user_dir(user)}/ratings.json", "w"))

def list_users():
    """List all registered usernames."""
    d = f"{DATA_DIR}/users"
    if os.path.exists(d): return [u for u in os.listdir(d) if os.path.isdir(f"{d}/{u}")]
    return []



def render_user_bar(current, page="u", show_create=True):
    pills = ""
    for u in list_users():
        href = BASE + "/" + page + "/" + u
        bg = "#4fc3f7" if u == current else "#16213e"
        fg = "#1a1a2e" if u == current else "#4fc3f7"
        pills += '<a href="' + href + '" style="padding:4px 12px;border-radius:12px;background:' + bg + ';color:' + fg + ';text-decoration:none;font-size:.9em">' + u + '</a> '
    create = ""
    if show_create:
        create = '<form method="GET" action="' + BASE + '/setup/create" style="display:inline;margin:0"><input name="name" placeholder="New..." style="width:80px;padding:4px 8px;font-size:.85em;border-radius:4px;border:1px solid #444;background:#16213e;color:#eee"><button type="submit" style="padding:4px 10px;background:#4fc3f7;border:none;border-radius:6px;cursor:pointer;font-size:.85em;margin-left:4px">+</button></form>'
    return '<div style="display:flex;gap:8px;align-items:center">' + pills + create + '</div>'


def load_user_trakt_token(user):
    f = f"{user_dir(user)}/trakt_token.json"
    if os.path.exists(f): return json.load(open(f))
    return None

def save_user_trakt_token(user, token):
    json.dump(token, open(f"{user_dir(user)}/trakt_token.json", "w"))

def load_user_tmm(user):
    f = f"{user_dir(user)}/tmm_library.json"
    if os.path.exists(f): return json.load(open(f))
    return {}

def load_user_providers(user):
    """Load user's streaming subscriptions. Returns {name: bool}."""
    f = user_dir(user) + "/providers.json"
    if os.path.exists(f): return json.load(open(f))
    return None

def save_user_providers(user, providers):
    json.dump(providers, open(user_dir(user) + "/providers.json", "w"))

def get_user_active_providers(user):
    """Get set of provider names the user subscribes to."""
    p = load_user_providers(user)
    if p is None: return DEFAULT_PROVIDERS
    return {k for k, v in p.items() if v}

def get_all_providers():
    """Get all available providers for WATCH_COUNTRY from TMDB."""
    if not TMDB_KEY: return []
    cached = DATA_DIR + "/providers_cache.json"
    if os.path.exists(cached):
        c = json.load(open(cached))
        if time.time() - c.get("ts", 0) < 86400:
            return c["providers"]
    providers = []
    for kind in ("movie", "tv"):
        data = api_get(f"https://api.themoviedb.org/3/watch/providers/{kind}?api_key={TMDB_KEY}&watch_region={WATCH_COUNTRY}")
        if data:
            for p in data.get("results", []):
                if p["provider_name"] not in [x["name"] for x in providers]:
                    providers.append({"name": p["provider_name"], "id": p["provider_id"]})
    providers.sort(key=lambda x: x["name"])
    json.dump({"ts": time.time(), "providers": providers}, open(cached, "w"))
    return providers

def save_user_tmm(user, lib):
    json.dump(lib, open(f"{user_dir(user)}/tmm_library.json", "w"))

# ── Migration from old format ─────────────────────────────────────────
def migrate_old_data():
    """One-time migration from v1 (single ratings.json) to v2 (titles + users split)."""
    old = f"{DATA_DIR}/ratings.json"
    if not os.path.exists(old) or os.path.exists(TITLES_FILE):
        return
    print("Migrating old data format...")
    data = json.load(open(old))
    titles = {}
    ratings = {}
    for r in data.get("ratings", []):
        iid = r.get("id", "")
        if not iid: continue
        # Title data (shared)
        titles[iid] = {k: r[k] for k in ("title","year","type","genres","directors","poster","overview",
            "imdb_rating","votes","tmdb_rating","rotten_tomatoes","metacritic","providers","watch_link",
            "tvdb_id","plot","awards","tmdb_id","_enriched") if k in r and r[k]}
        # User rating
        ratings[iid] = {"rating": r.get("rating", 0), "date": r.get("date", "")}
    save_titles(titles)
    save_user_ratings("ecb", ratings)
    # Migrate trakt token
    old_trakt = f"{DATA_DIR}/trakt_token.json"
    if os.path.exists(old_trakt):
        import shutil; shutil.copy(old_trakt, f"{user_dir('ecb')}/trakt_token.json")
    # Migrate TMM
    old_tmm = f"{DATA_DIR}/tmm_library.json"
    if os.path.exists(old_tmm):
        import shutil; shutil.copy(old_tmm, f"{user_dir('ecb')}/tmm_library.json")
    print(f"Migrated {len(titles)} titles, {len(ratings)} ratings for user 'ecb'")

# ── Enrichment APIs ───────────────────────────────────────────────────
def tmdb_enrich(imdb_id):
    """Fetch poster, overview, rating, streaming providers, keywords, and similar titles from TMDB."""
    if not TMDB_KEY: return {}
    data = api_get(f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={TMDB_KEY}&external_source=imdb_id")
    if not data: return {}
    movies, shows = data.get("movie_results") or [], data.get("tv_results") or []
    if not movies and not shows: return {}
    is_tv = len(shows) > 0
    r = shows[0] if is_tv else movies[0]
    tmdb_id = r["id"]; kind = "tv" if is_tv else "movie"
    result = {
        "poster": f"https://image.tmdb.org/t/p/w185{r['poster_path']}" if r.get("poster_path") else "",
        "overview": r.get("overview", ""), "tmdb_rating": r.get("vote_average"), "tmdb_id": tmdb_id,
    }
    # Watch providers
    wp = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/watch/providers?api_key={TMDB_KEY}")
    if wp:
        lu = wp.get("results", {}).get(WATCH_COUNTRY, {})
        result["providers"] = [p["provider_name"] for p in lu.get("flatrate", [])]
        result["watch_link"] = lu.get("link", "")
    # Cast (top 5 actors)
    credits = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/credits?api_key={TMDB_KEY}")
    if credits:
        cast = [c["name"] for c in (credits.get("cast") or [])[:5]]
        if cast: result["cast"] = ", ".join(cast)
        directors = [c["name"] for c in (credits.get("crew") or []) if c.get("job") == "Director"]
        if directors: result["directors"] = ", ".join(directors)
    # Trailer
    vids = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/videos?api_key={TMDB_KEY}")
    if vids:
        yt = next((v for v in vids.get("results",[]) if v.get("site")=="YouTube" and v.get("type")=="Trailer"), None)
        if yt: result["trailer"] = f"https://www.youtube.com/watch?v={yt['key']}"
    # Keywords for taste-based recommendations (the "Movie Genome")
    kw = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/keywords?api_key={TMDB_KEY}")
    if kw:
        kw_list = kw.get("keywords") or kw.get("results") or []
        result["keywords"] = [k["name"] for k in kw_list[:20]]
    # TMDB-recommended similar titles (used to expand recommendation pool)
    sim = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/recommendations?api_key={TMDB_KEY}&page=1")
    if sim:
        result["similar_tmdb"] = [s["id"] for s in (sim.get("results") or [])[:10]]
    return result

def omdb_enrich(imdb_id):
    """Fetch Rotten Tomatoes, Metacritic scores, plot summary from OMDB."""
    if not OMDB_KEY: return {}
    data = api_get(f"https://www.omdbapi.com/?i={imdb_id}&apikey={OMDB_KEY}")
    if not data or data.get("Response") == "False": return {}
    rt = next((r["Value"] for r in data.get("Ratings", []) if "Rotten" in r.get("Source", "")), None)
    mc = data.get("Metascore")
    return {"rotten_tomatoes": rt, "metacritic": int(mc) if mc and mc != "N/A" else None,
            "plot": data.get("Plot", ""), "awards": data.get("Awards", ""),
            "poster": data.get("Poster") if data.get("Poster") != "N/A" else ""}

def tvdb_login():
    if not TVDB_KEY: return None
    data = api_post("https://api4.thetvdb.com/v4/login", {"apikey": TVDB_KEY})
    if data and data.get("data", {}).get("token"):
        token = data["data"]["token"]
        json.dump({"token": token, "ts": time.time()}, open(f"{DATA_DIR}/tvdb_token.json", "w"))
        return token
    return None

def tvdb_token():
    f = f"{DATA_DIR}/tvdb_token.json"
    if os.path.exists(f):
        t = json.load(open(f))
        if time.time() - t.get("ts", 0) < 86000: return t["token"]
    return tvdb_login()

def tvdb_enrich(imdb_id):
    """Fetch TVDB ID for cross-referencing TV show data."""
    token = tvdb_token()
    if not token: return {}
    data = api_get(f"https://api4.thetvdb.com/v4/search/remoteid/{imdb_id}", {"Authorization": f"Bearer {token}"})
    if not data or not data.get("data"): return {}
    r = data["data"][0] if isinstance(data["data"], list) else data["data"]
    return {"tvdb_id": r.get("id")}

# ── Media Server Integrations ─────────────────────────────────────────
# Each adapter returns {imdb_id: {path, quality, size}} like TMM

def fetch_plex_library(url, token):
    """Fetch library from Plex Media Server. URL e.g. http://192.168.1.10:32400"""
    library = {}
    sections = api_get(f"{url}/library/sections?X-Plex-Token={token}")
    if not sections: return library
    for d in sections.get("MediaContainer", {}).get("Directory", []):
        if d.get("type") not in ("movie", "show"): continue
        key = d["key"]
        items = api_get(f"{url}/library/sections/{key}/all?X-Plex-Token={token}")
        if not items: continue
        for item in items.get("MediaContainer", {}).get("Metadata", []):
            # Extract IMDB ID from guids
            iid = ""
            for guid in item.get("Guid", []):
                if "imdb://" in guid.get("id", ""):
                    iid = guid["id"].replace("imdb://", "")
            if not iid: continue
            media = item.get("Media", [{}])[0]
            part = media.get("Part", [{}])[0]
            streams = part.get("Stream", [])
            video = next((s for s in streams if s.get("streamType") == 1), {})
            audio_streams = [s for s in streams if s.get("streamType") == 2]
            sub_streams = [s for s in streams if s.get("streamType") == 3]
            library[iid] = {
                "path": part.get("file", ""),
                "quality": media.get("videoResolution", ""),
                "size": str(part.get("size", "")),
                "video_codec": video.get("codec", media.get("videoCodec", "")),
                "video_bitrate": media.get("bitrate", ""),
                "audio": [{"codec": a.get("codec",""), "channels": a.get("channels",""), "language": a.get("language","")} for a in audio_streams],
                "subtitles": [{"language": s.get("language",""), "codec": s.get("codec",""), "forced": s.get("forced",False)} for s in sub_streams],
                "source": "plex",
            }
    return library

def fetch_jellyfin_library(url, token, user_id=""):
    """Fetch library from Jellyfin/Emby. URL e.g. http://192.168.1.10:8096"""
    library = {}
    # Get user ID if not provided
    if not user_id:
        users = api_get(f"{url}/Users?api_key={token}")
        if users and len(users) > 0:
            user_id = users[0].get("Id", "")
    if not user_id: return library
    # Fetch all items
    items = api_get(f"{url}/Users/{user_id}/Items?api_key={token}&Recursive=true&IncludeItemTypes=Movie,Series&Fields=ProviderIds,Path,MediaSources")
    if not items: return library
    for item in items.get("Items", []):
        iid = item.get("ProviderIds", {}).get("Imdb", "")
        if not iid: continue
        sources = item.get("MediaSources", [{}])
        path = sources[0].get("Path", "") if sources else ""
        size = str(sources[0].get("Size", "")) if sources else ""
        streams = sources[0].get("MediaStreams", []) if sources else []
        video = next((s for s in streams if s.get("Type") == "Video"), {})
        audio_list = [s for s in streams if s.get("Type") == "Audio"]
        sub_list = [s for s in streams if s.get("Type") == "Subtitle"]
        library[iid] = {
            "path": path, "size": size,
            "quality": str(video.get("Height", item.get("Width", ""))),
            "video_codec": video.get("Codec", ""),
            "video_bitrate": video.get("BitRate", ""),
            "audio": [{"codec": a.get("Codec",""), "channels": a.get("Channels",""), "language": a.get("Language","")} for a in audio_list],
            "subtitles": [{"language": s.get("Language",""), "codec": s.get("Codec",""), "forced": s.get("IsForced",False)} for s in sub_list],
            "source": "jellyfin",
        }
    return library

def fetch_emby_library(url, token, user_id=""):
    """Fetch library from Emby — same API as Jellyfin."""
    return fetch_jellyfin_library(url, token, user_id)

def fetch_kodi_library(url):
    """Fetch library from Kodi via JSON-RPC. URL e.g. http://192.168.1.10:8080/jsonrpc"""
    library = {}
    # Movies
    payload = {"jsonrpc": "2.0", "method": "VideoLibrary.GetMovies", "id": 1,
               "params": {"properties": ["imdbnumber", "file", "streamdetails"]}}
    data = api_post(url, payload)
    if data:
        for m in data.get("result", {}).get("movies", []):
            iid = m.get("imdbnumber", "")
            if iid and iid.startswith("tt"):
                sd = m.get("streamdetails", {})
                vstreams = sd.get("video", [{}])
                astreams = sd.get("audio", [])
                sstreams = sd.get("subtitle", [])
                library[iid] = {
                    "path": m.get("file", ""),
                    "quality": str(vstreams[0].get("height", "")) if vstreams else "",
                    "video_codec": vstreams[0].get("codec", "") if vstreams else "",
                    "audio": [{"codec": a.get("codec",""), "channels": a.get("channels",""), "language": a.get("language","")} for a in astreams],
                    "subtitles": [{"language": s.get("language","")} for s in sstreams],
                    "source": "kodi",
                }
    # TV Shows
    payload = {"jsonrpc": "2.0", "method": "VideoLibrary.GetTVShows", "id": 2,
               "params": {"properties": ["imdbnumber"]}}
    data = api_post(url, payload)
    if data:
        for s in data.get("result", {}).get("tvshows", []):
            iid = s.get("imdbnumber", "")
            if iid and iid.startswith("tt"):
                library[iid] = {"path": "", "quality": "", "source": "kodi"}
    return library

def fetch_radarr_library(url, token):
    """Fetch library from Radarr. URL e.g. http://192.168.1.10:7878"""
    library = {}
    movies = api_get(f"{url}/api/v3/movie?apiKey={token}")
    if not movies: return library
    for m in movies:
        iid = m.get("imdbId", "")
        if not iid: continue
        library[iid] = {
            "path": m.get("movieFile", {}).get("relativePath", m.get("path", "")),
            "quality": m.get("movieFile", {}).get("quality", {}).get("quality", {}).get("name", ""),
            "size": str(m.get("movieFile", {}).get("size", "")),
            "source": "radarr",
            "monitored": m.get("monitored", False),
            "downloaded": m.get("hasFile", False),
        }
    return library

def fetch_sonarr_library(url, token):
    """Fetch library from Sonarr. URL e.g. http://192.168.1.10:8989"""
    library = {}
    shows = api_get(f"{url}/api/v3/series?apiKey={token}")
    if not shows: return library
    for s in shows:
        iid = s.get("imdbId", "")
        if not iid: continue
        library[iid] = {
            "path": s.get("path", ""),
            "quality": s.get("qualityProfileId", ""),
            "source": "sonarr",
            "monitored": s.get("monitored", False),
            "downloaded": s.get("statistics", {}).get("percentOfEpisodes", 0) > 0,
        }
    return library

def fetch_folder_library(path):
    """Scan a folder for media files, extract title+year, match to TMDB."""
    import re
    library = {}
    if not os.path.isdir(path): return library
    for root, dirs, files in os.walk(path):
        for f in files:
            if not f.lower().endswith((".mkv", ".mp4", ".avi", ".m4v", ".ts")): continue
            # Parse "Title (Year)" or "Title.Year.Quality" patterns
            match = re.match(r"(.+?)[\.\s\-_]*((?:19|20)\d{2})", f)
            if not match: continue
            title = re.sub(r"[\.\-_]", " ", match.group(1)).strip()
            year = match.group(2)
            # Try TMDB lookup
            if TMDB_KEY:
                search = api_get(f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_KEY}&query={urllib.parse.quote(title)}&year={year}")
                if search and search.get("results"):
                    tmdb_id = search["results"][0]["id"]
                    ext = api_get(f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_KEY}")
                    if ext and ext.get("imdb_id"):
                        library[ext["imdb_id"]] = {
                            "path": os.path.join(root, f),
                            "quality": "",
                            "source": "folder",
                        }
                time.sleep(0.2)
    return library

def fetch_tmm_library(url, token):
    """Fetch library from TMM via HTTP API. Triggers export and parses result."""
    # TMM API is command-based; for server-side we just store the config
    # Actual sync happens via the LAN agent or browser upload
    return {}

MEDIA_SERVERS = {
    "plex": {"name": "Plex", "fields": ["url", "token"], "fetch": lambda c: fetch_plex_library(c["url"], c["token"])},
    "jellyfin": {"name": "Jellyfin", "fields": ["url", "token"], "fetch": lambda c: fetch_jellyfin_library(c["url"], c["token"])},
    "emby": {"name": "Emby", "fields": ["url", "token"], "fetch": lambda c: fetch_emby_library(c["url"], c["token"])},
    "kodi": {"name": "Kodi", "fields": ["url"], "fetch": lambda c: fetch_kodi_library(c["url"])},
    "radarr": {"name": "Radarr", "fields": ["url", "token"], "fetch": lambda c: fetch_radarr_library(c["url"], c["token"])},
    "sonarr": {"name": "Sonarr", "fields": ["url", "token"], "fetch": lambda c: fetch_sonarr_library(c["url"], c["token"])},
    "tmm": {"name": "tinyMediaManager", "fields": ["url", "token"], "fetch": lambda c: fetch_tmm_library(c["url"], c["token"])},
}

def load_user_media_config(user):
    f = user_dir(user) + "/media_servers.json"
    if os.path.exists(f): return json.load(open(f))
    return {}

def save_user_media_config(user, config):
    json.dump(config, open(user_dir(user) + "/media_servers.json", "w"))

def sync_media_servers(user):
    """Sync all configured media servers for a user into their local library."""
    config = load_user_media_config(user)
    library = load_user_tmm(user)  # Start with existing TMM data
    for server_type, server_config in config.items():
        if server_type not in MEDIA_SERVERS: continue
        if not server_config.get("enabled"): continue
        try:
            print(f"  Syncing {server_type}...")
            items = MEDIA_SERVERS[server_type]["fetch"](server_config)
            library.update(items)
            print(f"  {server_type}: {len(items)} titles")
        except Exception as e:
            print(f"  {server_type} error: {e}")
    save_user_tmm(user, library)
    return library


# ── OpenSubtitles ─────────────────────────────────────────────────────
OPENSUBS_API = "https://api.opensubtitles.com/api/v1"

def opensubs_search(imdb_id, languages=None, file_hash=None, file_size=None):
    """Search OpenSubtitles by IMDB ID and optionally by file hash for sync-accurate results."""
    api_key = _load_key("opensubs")
    if not api_key: return []
    params = f"imdb_id={imdb_id.replace('tt','')}"
    if languages: params += "&languages=" + ",".join(languages)
    if file_hash: params += f"&moviehash={file_hash}"
    data = api_get(f"{OPENSUBS_API}/subtitles?{params}",
                   {"Api-Key": api_key, "User-Agent": "CineCross v1.0"})
    if not data: return []
    results = []
    for s in data.get("data", []):
        attr = s.get("attributes", {})
        results.append({
            "language": attr.get("language", ""),
            "release": attr.get("release", ""),
            "download_count": attr.get("download_count", 0),
            "rating": attr.get("ratings", 0),
            "hearing_impaired": attr.get("hearing_impaired", False),
            "file_id": (attr.get("files", [{}])[0]).get("file_id", ""),
            "hash_match": attr.get("moviehash_match", False),
        })
    results.sort(key=lambda x: (x["hash_match"], x["download_count"]), reverse=True)
    return results

def opensubs_download_link(file_id):
    """Get download link for a subtitle file."""
    api_key = _load_key("opensubs")
    if not api_key: return None
    data = api_post(f"{OPENSUBS_API}/download", {"file_id": file_id},
                    {"Api-Key": api_key, "User-Agent": "CineCross v1.0"})
    if data: return data.get("link")
    return None

def _load_key(name):
    if os.path.exists(KEYS_FILE):
        return json.load(open(KEYS_FILE)).get(name, "")
    return ""


# ── Mood system ──────────────────────────────────────────────────────
MOOD_MAP = {
    "light": {"comedy", "feel-good", "family", "romantic comedy", "animation", "friendship", "coming of age", "heartwarming"},
    "intense": {"thriller", "suspense", "tension", "psychological thriller", "chase", "hostage", "survival", "conspiracy"},
    "funny": {"comedy", "satire", "parody", "slapstick", "dark comedy", "absurd humor", "stand-up comedy", "farce"},
    "mind-bending": {"twist ending", "nonlinear timeline", "time travel", "dream", "parallel universe", "simulation", "unreliable narrator", "surrealism", "philosophical"},
    "dark": {"dark", "dystopia", "serial killer", "noir", "death", "revenge", "violence", "crime", "corruption"},
    "epic": {"epic", "war", "battle", "kingdom", "empire", "historical", "sword", "medieval", "ancient"},
    "romantic": {"romance", "love", "love triangle", "wedding", "relationship", "heartbreak", "passion"},
    "scary": {"horror", "ghost", "haunted", "zombie", "supernatural", "slasher", "monster", "demon", "paranormal"},
    "inspiring": {"biography", "based on true story", "underdog", "overcoming", "sports", "triumph", "motivation", "dream"},
}

def mood_filter(titles, mood, user_ratings):
    """Filter unrated titles matching a mood via keyword overlap."""
    mood_kws = MOOD_MAP.get(mood, set())
    mood_genres = {mood.capitalize()} if mood in ("romantic", "scary", "funny") else set()
    results = []
    for iid, t in titles.items():
        if iid in user_ratings: continue
        kws = set(t.get("keywords", []))
        genres = set(g.strip().lower() for g in (t.get("genres") or "").split(","))
        overlap = len(kws & mood_kws) + len(genres & mood_genres) * 0.5
        if overlap > 0:
            results.append((iid, t, overlap))
    results.sort(key=lambda x: (-x[2], -(x[1].get("tmdb_rating") or 0)))
    return results

# ── Seasonal recommendations ──────────────────────────────────────────
def seasonal_keywords():
    """Return extra keyword weights based on current month."""
    import datetime
    month = datetime.datetime.now().month
    if month == 12: return {"christmas": 3, "holiday": 2, "snow": 1, "family": 1, "winter": 1, "santa": 2}
    if month == 10: return {"horror": 3, "halloween": 3, "ghost": 2, "zombie": 2, "haunted": 2, "monster": 1}
    if month in (6, 7, 8): return {"summer": 2, "beach": 1, "adventure": 1, "road trip": 1, "vacation": 1}
    if month == 2: return {"romance": 3, "love": 2, "valentine": 2, "relationship": 1}
    return {}

# ── Watchlist ─────────────────────────────────────────────────────────
def load_watchlist(user):
    f = user_dir(user) + "/watchlist.json"
    if os.path.exists(f): return json.load(open(f))
    return []

def save_watchlist(user, wl):
    json.dump(wl, open(user_dir(user) + "/watchlist.json", "w"))

# ── TasteDive ─────────────────────────────────────────────────────────
def tastedive_similar(imdb_id, title):
    """Get similar titles from TasteDive. Cached in title store."""
    titles = load_titles()
    t = titles.get(imdb_id, {})
    if t.get("_tastedive"):
        return t["_tastedive"]
    q = urllib.parse.quote(title)
    data = api_get(f"https://tastedive.com/api/similar?q={q}&type=movie&limit=5&info=1")
    if not data: return []
    results = [{"title": r.get("Name",""), "type": r.get("Type",""), "description": r.get("wTeaser","")} for r in data.get("Similar",{}).get("Results",[])]
    # Cache
    if imdb_id in titles:
        titles[imdb_id]["_tastedive"] = results
        save_titles(titles)
    return results

# ── IMDB Bulk Datasets ────────────────────────────────────────────────
IMDB_DATASET_DIR = f"{DATA_DIR}/imdb_datasets"
IMDB_BASICS = f"{IMDB_DATASET_DIR}/title.basics.tsv"
IMDB_RATINGS_DS = f"{IMDB_DATASET_DIR}/title.ratings.tsv"
_imdb_cache = {}  # {imdb_id: (title, year, type, genres, runtime, imdb_rating, votes)}

def download_imdb_datasets(jid=None):
    """Download IMDB bulk data files. ~220MB compressed, updated daily."""
    import gzip, shutil
    os.makedirs(IMDB_DATASET_DIR, exist_ok=True)
    for fname in ["title.basics.tsv.gz", "title.ratings.tsv.gz"]:
        url = f"https://datasets.imdbws.com/{fname}"
        dest_gz = f"{IMDB_DATASET_DIR}/{fname}"
        dest = dest_gz.replace(".gz", "")
        if jid: job_progress(jid, 0, 2, f"Downloading {fname}...")
        print(f"Downloading {url}...")
        req = urllib.request.Request(url, headers={"User-Agent": "CineCross/1.0"})
        with urllib.request.urlopen(req, timeout=300) as resp, open(dest_gz, "wb") as f:
            shutil.copyfileobj(resp, f)
        with gzip.open(dest_gz, "rb") as gz, open(dest, "wb") as out:
            shutil.copyfileobj(gz, out)
        os.remove(dest_gz)
        print(f"  Extracted {dest}")
    load_imdb_cache()
    if jid: job_progress(jid, 2, 2, "Done")

def load_imdb_cache(min_votes=100):
    """Load IMDB datasets into memory. Filters to movies/TV with min_votes."""
    global _imdb_cache
    if _imdb_cache: return _imdb_cache
    if not os.path.exists(IMDB_BASICS) or not os.path.exists(IMDB_RATINGS_DS):
        return {}
    import csv
    # Load ratings first
    rated = {}
    with open(IMDB_RATINGS_DS) as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)
        for row in reader:
            try:
                votes = int(row[2])
                if votes >= min_votes:
                    rated[row[0]] = (float(row[1]), votes)
            except: pass
    # Load basics for rated titles
    keep = {"movie", "tvSeries", "tvMiniSeries", "tvMovie"}
    with open(IMDB_BASICS) as f:
        reader = csv.reader(f, delimiter="\t")
        next(reader)
        for row in reader:
            if row[0] in rated and row[1] in keep and row[4] == "0":
                r = rated[row[0]]
                n = lambda v: "" if v == "\\N" else v
                _imdb_cache[row[0]] = {
                    "title": row[2], "year": n(row[5]), "type": row[1],
                    "genres": n(row[8]).replace(",", ", "),
                    "runtime": n(row[7]),
                    "imdb_rating": r[0], "votes": r[1],
                }
    print(f"IMDB cache: {len(_imdb_cache)} titles loaded")
    return _imdb_cache

def imdb_lookup(imdb_id):
    """Look up a title from the IMDB dataset cache."""
    if not _imdb_cache: load_imdb_cache()
    return _imdb_cache.get(imdb_id, {})

def seed_from_imdb_dataset(jid=None):
    """Seed titles.json with IMDB dataset data for titles missing basic info."""
    cache = load_imdb_cache()
    if not cache: return
    titles = load_titles()
    updated = 0
    for iid, t in titles.items():
        if t.get("genres") and t.get("imdb_rating"): continue
        ds = cache.get(iid)
        if not ds: continue
        for k in ("title", "year", "type", "genres", "runtime", "imdb_rating", "votes"):
            if ds.get(k) and not t.get(k):
                t[k] = ds[k]
        updated += 1
    save_titles(titles)
    print(f"Seeded {updated} titles from IMDB dataset")


# ── Trakt ─────────────────────────────────────────────────────────────
def trakt_headers(user):
    token = load_user_trakt_token(user)
    if not token: return None
    return {"Content-Type": "application/json", "trakt-api-version": "2",
            "trakt-api-key": TRAKT_ID, "Authorization": f"Bearer {token['access_token']}"}

def trakt_auth_url():
    return f"https://trakt.tv/oauth/authorize?response_type=code&client_id={TRAKT_ID}&redirect_uri={urllib.parse.quote(TRAKT_REDIRECT)}"

def trakt_exchange_code(code):
    return api_post("https://api.trakt.tv/oauth/token", {"code": code, "client_id": TRAKT_ID,
        "client_secret": TRAKT_SECRET, "redirect_uri": TRAKT_REDIRECT, "grant_type": "authorization_code"})

def trakt_fetch_history(user):
    """Pull watch history from Trakt — returns [{imdb_id, title, watched_at, type}]."""
    h = trakt_headers(user)
    if not h: return []
    history = []
    for kind in ["movies", "shows"]:
        page = 1
        while page <= 10:
            data = api_get(f"https://api.trakt.tv/users/me/history/{kind}?page={page}&limit=100", h)
            if not data or len(data) == 0: break
            for item in data:
                obj = item.get("movie") or item.get("show") or {}
                iid = obj.get("ids", {}).get("imdb", "")
                if iid:
                    history.append({"id": iid, "title": obj.get("title", ""),
                        "watched_at": (item.get("watched_at") or "")[:10],
                        "type": "movie" if "movie" in item else "show"})
            page += 1
            time.sleep(0.3)
    return history

def save_user_history(user, history):
    json.dump(history, open(user_dir(user) + "/history.json", "w"))

def load_user_history(user):
    f = user_dir(user) + "/history.json"
    if os.path.exists(f): return json.load(open(f))
    return []

def trakt_fetch_ratings(user):
    """Pull all movie and show ratings from Trakt for a user."""
    h = trakt_headers(user)
    if not h: return {}
    ratings = {}
    for kind in ["movies", "shows"]:
        data = api_get(f"https://api.trakt.tv/users/me/ratings/{kind}", h)
        if not data: continue
        for item in data:
            obj = item.get("movie") or item.get("show") or {}
            iid = obj.get("ids", {}).get("imdb", "")
            if iid: ratings[iid] = {"rating": item.get("rating", 0), "date": (item.get("rated_at") or "")[:10]}
    return ratings

def trakt_sync_push(user, ratings, titles):
    """Push user ratings to Trakt (bidirectional sync — push half)."""
    h = trakt_headers(user)
    if not h: return
    movies, shows = [], []
    for iid, r in ratings.items():
        t = titles.get(iid, {})
        entry = {"ids": {"imdb": iid}, "rating": r["rating"]}
        if t.get("type") in ("movie", "Movie"): movies.append(entry)
        else: shows.append(entry)
    if movies: api_post("https://api.trakt.tv/sync/ratings", {"movies": movies}, h)
    if shows: api_post("https://api.trakt.tv/sync/ratings", {"shows": shows}, h)

# ── Recommendation engine ─────────────────────────────────────────────
def build_taste_profile(user_ratings, titles):
    """Build weighted taste profile from highly-rated titles (6+)."""
    keyword_scores, genre_scores, director_scores, actor_scores = {}, {}, {}, {}
    for iid, r in user_ratings.items():
        if r["rating"] < 6: continue
        t = titles.get(iid, {})
        weight = (r["rating"] - 5) / 5.0
        for kw in t.get("keywords", []):
            keyword_scores[kw] = keyword_scores.get(kw, 0) + weight
        for g in (t.get("genres") or "").split(","):
            g = g.strip()
            if g: genre_scores[g] = genre_scores.get(g, 0) + weight
        for d in (t.get("directors") or "").split(","):
            d = d.strip()
            if d: director_scores[d] = director_scores.get(d, 0) + weight
        for a in (t.get("cast") or "").split(","):
            a = a.strip()
            if a: actor_scores[a] = actor_scores.get(a, 0) + weight
    return {"keywords": keyword_scores, "genres": genre_scores,
            "directors": director_scores, "actors": actor_scores}

def score_divergence(title):
    """Detect suspicious score manipulation. Returns True if scores diverge >2.0."""
    scores = []
    if title.get("imdb_rating"): scores.append(title["imdb_rating"])
    if title.get("tmdb_rating"): scores.append(title["tmdb_rating"])
    rt = title.get("rotten_tomatoes", "")
    if rt and "%" in str(rt):
        try: scores.append(float(str(rt).replace("%","")) / 10)
        except: pass
    if title.get("metacritic"): scores.append(title["metacritic"] / 10)
    if len(scores) < 2: return False
    return (max(scores) - min(scores)) > 2.0

def score_title(title, profile, seasonal=None):
    """Score a candidate title against user's taste profile.
    Combines keyword match + genre match, boosted by critical ratings."""
    """Score a title against a taste profile. Higher = better match."""
    score = 0
    kw_prof = profile["keywords"]
    g_prof = profile["genres"]
    for kw in title.get("keywords", []):
        score += kw_prof.get(kw, 0)
    for g in (title.get("genres") or "").split(","):
        g = g.strip()
        if g: score += g_prof.get(g, 0) * 0.5
    # Boost for high IMDB/TMDB ratings
    if title.get("imdb_rating"): score *= (0.5 + title["imdb_rating"] / 20)
    if title.get("tmdb_rating"): score *= (0.5 + title["tmdb_rating"] / 20)
    # Seasonal boost
    if seasonal:
        for kw in title.get("keywords", []):
            score += seasonal.get(kw, 0)
        for g in (title.get("genres") or "").split(","):
            g = g.strip().lower()
            if g: score += seasonal.get(g, 0) * 0.5
    return round(score, 2)

def get_recommendations(user, titles, n=50, provider_filter=None):
    """Get top-N recommendations: unrated titles scored against taste profile,
    optionally filtered to specific streaming providers."""
    """Get top-N recommendations for a user."""
    user_ratings = load_user_ratings(user)
    rated_ids = set(user_ratings.keys())
    profile = build_taste_profile(user_ratings, titles)
    candidates = []
    seasonal_kw = seasonal_keywords()
    for iid, t in titles.items():
        if iid in rated_ids: continue
        if provider_filter:
            provs = set(t.get("providers", []))
            if not provs & set(provider_filter): continue
        s = score_title(t, profile, seasonal_kw)
        if s > 0: candidates.append((iid, t, s))
    candidates.sort(key=lambda x: x[2], reverse=True)
    return candidates[:n], profile

def get_5cat_recommendations(user, titles, n_per_cat=6):
    """Get recommendations in 5 categories: DNA, Cast, Community, Overlap, Rewatch."""
    import datetime
    user_ratings = load_user_ratings(user)
    rated_ids = set(user_ratings.keys())
    profile = build_taste_profile(user_ratings, titles)
    provs = get_user_active_providers(user)

    seasonal = seasonal_keywords()
    def _pool():
        cands = []
        for iid, t in titles.items():
            if iid in rated_ids: continue
            if not set(t.get("providers", [])) & provs: continue
            s = score_title(t, profile, seasonal)
            if s > 0: cands.append((iid, t, s))
        cands.sort(key=lambda x: x[2], reverse=True)
        return cands

    base_pool = _pool()
    dna = base_pool  # DNA is the default scoring

    # Cast: re-rank by director/actor match
    cast = []
    for iid, t, s in base_pool:
        dir_s = sum(profile["directors"].get(d.strip(), 0) for d in (t.get("directors") or "").split(",") if d.strip())
        act_s = sum(profile["actors"].get(a.strip(), 0) for a in (t.get("cast") or "").split(",") if a.strip())
        cast_score = dir_s * 3 + act_s * 1.5 + s * 0.3
        if cast_score > 0: cast.append((iid, t, round(cast_score, 2)))
    cast.sort(key=lambda x: x[2], reverse=True)

    # Community: TMDB similar links from rated titles
    community_ids = set()
    for iid in list(rated_ids)[:100]:
        for sim_id in titles.get(iid, {}).get("similar_tmdb", []):
            for cid, ct in titles.items():
                if ct.get("tmdb_id") == sim_id and cid not in rated_ids:
                    community_ids.add(cid)
                    break
    community = [(i, titles[i], score_title(titles[i], profile, seasonal))
                 for i in community_ids if i in titles and set(titles[i].get("providers",[])) & provs]
    community.sort(key=lambda x: x[2], reverse=True)

    # Overlap: multi-source agreement, no divergence
    overlap = []
    for iid, t in titles.items():
        if iid in rated_ids or score_divergence(t): continue
        if not set(t.get("providers", [])) & provs: continue
        sc = [v for v in [t.get("imdb_rating"), t.get("tmdb_rating")] if v]
        mc = t.get("metacritic")
        if mc: sc.append(mc / 10)
        if len(sc) >= 2 and all(s >= 7.0 for s in sc):
            overlap.append((iid, t, round(sum(sc)/len(sc), 2)))
    overlap.sort(key=lambda x: x[2], reverse=True)

    # Rewatch: rated 8+, not watched in 2+ years
    rewatch = []
    for iid, r in user_ratings.items():
        if r["rating"] < 8: continue
        date = r.get("date", "")
        if not date: continue
        try:
            days = (datetime.date.today() - datetime.date.fromisoformat(date)).days
            if days > 730:
                rewatch.append((iid, titles.get(iid, {}), round(r["rating"] * days / 365, 1)))
        except: pass
    rewatch.sort(key=lambda x: x[2], reverse=True)

    # Deduplicate
    seen = set()
    def dedup(lst, n):
        result = []
        for item in lst:
            if item[0] not in seen:
                seen.add(item[0])
                result.append(item)
                if len(result) >= n: break
        return result

    return {
        "dna": dedup(dna, n_per_cat),
        "cast": dedup(cast, n_per_cat),
        "community": dedup(community, n_per_cat),
        "overlap": dedup(overlap, n_per_cat),
        "rewatch": dedup(rewatch, n_per_cat),
    }, profile


def get_streaming_recs(user, titles, n=30):
    """Recommendations filtered to user's streaming subscriptions."""
    """Recommendations filtered to user's streaming subscriptions."""
    """Recommendations filtered to user's streaming services."""
    return get_recommendations(user, titles, n, provider_filter=get_user_active_providers(user))

# ── Enrichment ────────────────────────────────────────────────────────
def _richness(t):
    """Score how complete a title's metadata is (0-8). Used to prioritize re-enrichment."""
    score = 0
    if t.get("poster"): score += 2
    if t.get("overview") or t.get("plot"): score += 1
    if t.get("rotten_tomatoes"): score += 1
    if t.get("metacritic"): score += 1
    if t.get("tmdb_rating"): score += 1
    if t.get("providers"): score += 1
    if t.get("keywords"): score += 1
    return score

def enrich_titles(jid=None, fast=False):
    """Enrich all titles: unenriched first, then poorest metadata.
    Pulls from TMDB (poster, keywords, streaming), OMDB (RT, Metacritic), TVDB.
    Saves incrementally every 50 titles. Runs as background job."""
    titles = load_titles()
    never = [(k, v) for k, v in titles.items() if not v.get("_enriched")]
    partial = sorted([(k, v) for k, v in titles.items() if v.get("_enriched") and _richness(v) < 5],
                     key=lambda x: _richness(x[1]))
    # FIFO: re-enrich the 50 oldest-enriched titles regardless of age
    stale = sorted([(k, v) for k, v in titles.items()
                    if v.get("_enriched") and v.get("_enriched_ts") and _richness(v) >= 5],
                   key=lambda x: x[1].get("_enriched_ts", ""))[:50]
    todo = never + partial + stale
    total = len(todo)
    count = 0
    cache = load_imdb_cache()
    omdb_calls = 0
    for iid, t in todo:
        # Fill basics from IMDB dataset (free, no API call)
        ds = cache.get(iid, {})
        for k in ("title", "year", "type", "genres", "runtime", "imdb_rating", "votes"):
            if ds.get(k) and not t.get(k): t[k] = ds[k]
        t.pop("_enriched", None)
        if TMDB_KEY:
            for k, v in tmdb_enrich(iid).items():
                if v: t[k] = v
        if OMDB_KEY and omdb_calls < 500 and not fast:
            o = omdb_enrich(iid)
            omdb_calls += 1
            for k in ("rotten_tomatoes", "metacritic", "plot", "awards"):
                if o.get(k): t[k] = o[k]
            if o.get("poster") and not t.get("poster"): t["poster"] = o["poster"]
        if TVDB_KEY:
            for k, v in tvdb_enrich(iid).items():
                if v: t[k] = v
        t["_enriched"] = True
        t["_enriched_ts"] = time.strftime("%Y-%m-%dT%H:%M")
        count += 1
        if jid and count % 5 == 0:
            job_progress(jid, count, total, f"Enriching {t.get('title',iid)}")
        if count % 50 == 0:
            save_titles(titles)
            print(f"  Enriched {count}/{total}...")
        time.sleep(0.03 if fast else 0.08)
    save_titles(titles)
    print(f"Enriched {count} titles")

def fetch_streaming_catalog(jid=None):
    """Fetch full streaming catalog for WATCH_COUNTRY from TMDB discover API.
    Seeds the shared title store with unrated titles for recommendations."""
    if not TMDB_KEY: return
    catalog = []
    for kind in ("movie", "tv"):
        for pname, pid in LU_PROVIDER_IDS.items():
            page = 1
            while page <= 20:
                data = api_get(f"https://api.themoviedb.org/3/discover/{kind}?api_key={TMDB_KEY}&watch_region={WATCH_COUNTRY}&with_watch_providers={pid}&with_watch_monetization_types=flatrate&sort_by=vote_average.desc&vote_count.gte=100&page={page}")
                if not data or not data.get("results"): break
                for r in data["results"]:
                    catalog.append({"tmdb_id": r["id"], "title": r.get("title") or r.get("name", ""),
                        "year": (r.get("release_date") or r.get("first_air_date") or "")[:4], "type": kind,
                        "tmdb_rating": r.get("vote_average"),
                        "poster": f"https://image.tmdb.org/t/p/w185{r['poster_path']}" if r.get("poster_path") else "",
                        "overview": r.get("overview", "")[:200], "provider": pname})
                if page >= data.get("total_pages", 1): break
                if jid: job_progress(jid, len(catalog), 0, f"{pname} {kind} page {page}")
                page += 1; time.sleep(0.15)
    merged = {}
    for c in catalog:
        key = c["tmdb_id"]
        if key in merged:
            if c["provider"] not in merged[key]["providers"]: merged[key]["providers"].append(c["provider"])
        else: c["providers"] = [c["provider"]]; del c["provider"]; merged[key] = c
    result = sorted(merged.values(), key=lambda x: x.get("tmdb_rating", 0), reverse=True)
    # Save previous catalog for "leaving soon" detection
    if os.path.exists(CATALOG_FILE):
        import shutil
        shutil.copy(CATALOG_FILE, CATALOG_PREV)
    json.dump({"updated": time.strftime("%Y-%m-%d %H:%M"), "count": len(result), "catalog": result}, open(CATALOG_FILE, "w"))
    print(f"Catalog: {len(result)} titles")
    # Seed title store with catalog entries for recommendations
    if jid: job_progress(jid, 0, len(merged), "Seeding title store...")
    titles = load_titles()
    added = 0
    for c in result:
        # Find IMDB ID via TMDB lookup
        tmdb_id = c.get("tmdb_id")
        kind = c.get("type", "movie")
        if tmdb_id and TMDB_KEY:
            detail = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/external_ids?api_key={TMDB_KEY}")
            if detail and detail.get("imdb_id"):
                iid = detail["imdb_id"]
                if iid not in titles:
                    # Also fetch genres
                    info = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}?api_key={TMDB_KEY}")
                    genres = ", ".join(g["name"] for g in (info or {}).get("genres", []))
                    titles[iid] = {"title": c["title"], "year": c.get("year"), "type": kind,
                        "tmdb_id": tmdb_id, "tmdb_rating": c.get("tmdb_rating"),
                        "poster": c.get("poster"), "overview": c.get("overview", ""),
                        "providers": c.get("providers", []), "genres": genres}
                    added += 1
                    if jid and added % 20 == 0:
                        job_progress(jid, added, len(merged), f"Seeding titles ({added})")
                elif not titles[iid].get("providers"):
                    titles[iid]["providers"] = c.get("providers", [])
            time.sleep(0.1)
    save_titles(titles)
    print(f"Seeded {added} new titles from catalog")

# ── Background job wrappers ───────────────────────────────────────────
def get_leaving_titles():
    """Compare current vs previous catalog to find titles that disappeared from a provider."""
    if not os.path.exists(CATALOG_PREV) or not os.path.exists(CATALOG_FILE):
        return []
    prev = json.load(open(CATALOG_PREV))
    curr = json.load(open(CATALOG_FILE))
    # Build provider sets: {tmdb_id: set(providers)}
    prev_map = {}
    for c in prev.get("catalog", []):
        prev_map[c["tmdb_id"]] = set(c.get("providers", []))
    curr_map = {}
    for c in curr.get("catalog", []):
        curr_map[c["tmdb_id"]] = set(c.get("providers", []))
    leaving = []
    for tid, prev_provs in prev_map.items():
        curr_provs = curr_map.get(tid, set())
        lost = prev_provs - curr_provs
        if lost:
            # Find title info
            info = next((c for c in prev.get("catalog", []) if c["tmdb_id"] == tid), {})
            leaving.append({"title": info.get("title", "?"), "year": info.get("year", ""),
                           "lost_from": list(lost), "still_on": list(curr_provs),
                           "poster": info.get("poster", "")})
    return leaving


def _bg_auto_subs(jid, user):
    """Background job: search subtitles for local titles that have no subs."""
    library = load_user_tmm(user)
    missing = [(iid, info) for iid, info in library.items()
               if not info.get("subtitles") and info.get("path")]
    if not missing:
        job_progress(jid, 1, 1, "All titles have subtitles")
        return
    total = len(missing)
    found = 0
    for i, (iid, info) in enumerate(missing):
        subs = opensubs_search(iid, languages=["en", "fr"],
                               file_hash=info.get("file_hash"),
                               file_size=info.get("file_size"))
        if subs:
            # Store best match info (don't auto-download, just flag)
            best = subs[0]
            info["suggested_sub"] = {
                "language": best["language"],
                "release": best["release"],
                "file_id": best["file_id"],
                "hash_match": best["hash_match"],
                "downloads": best["download_count"],
            }
            found += 1
        if (i + 1) % 10 == 0:
            job_progress(jid, i + 1, total, f"Searched {i+1}/{total}, found {found}")
            save_user_tmm(user, library)
        time.sleep(0.3)  # Rate limit
    save_user_tmm(user, library)
    job_progress(jid, total, total, f"Done: found subs for {found}/{total}")


def _bg_history(jid, user):
    job_progress(jid, 0, 1, "Fetching Trakt history...")
    history = trakt_fetch_history(user)
    save_user_history(user, history)
    job_progress(jid, 1, 1, f"Saved {len(history)} watch events")

def _bg_enrich(jid): enrich_titles(jid, fast=True)
def _bg_catalog(jid): fetch_streaming_catalog(jid)
def _bg_trakt_sync(jid, user):
    titles = load_titles(); ratings = load_user_ratings(user)
    job_progress(jid, 0, 3, "Pushing to Trakt...")
    trakt_sync_push(user, ratings, titles)
    job_progress(jid, 1, 3, "Pulling from Trakt...")
    tr = trakt_fetch_ratings(user)
    for iid, r in tr.items():
        if iid not in ratings: ratings[iid] = r
        if iid not in titles: titles[iid] = {"title": "", "_enriched": False}
    save_user_ratings(user, ratings); save_titles(titles)

# ── CSV import ────────────────────────────────────────────────────────
def import_letterboxd(user, text):
    """Import Letterboxd CSV export (diary.csv or ratings.csv)."""
    titles_db = load_titles()
    ratings = load_user_ratings(user)
    imported = 0
    for row in csv.DictReader(io.StringIO(text)):
        name = row.get("Name", row.get("Film", ""))
        year = row.get("Year", "")
        rating_str = row.get("Rating", "")
        date = row.get("Date", row.get("Watched Date", row.get("Date Rated", "")))
        if not name or not rating_str: continue
        # Letterboxd uses 0.5-5.0 scale, convert to 1-10
        try: score = int(float(rating_str) * 2)
        except: continue
        if score < 1: continue
        # Lookup IMDB ID via TMDB search
        if TMDB_KEY:
            q = urllib.parse.quote(name)
            search = api_get(f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_KEY}&query={q}&year={year}")
            if search and search.get("results"):
                tmdb_id = search["results"][0]["id"]
                ext = api_get(f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_KEY}")
                if ext and ext.get("imdb_id"):
                    iid = ext["imdb_id"]
                    if iid not in titles_db:
                        r = search["results"][0]
                        titles_db[iid] = {"title": name, "year": year, "tmdb_id": tmdb_id,
                            "poster": f"https://image.tmdb.org/t/p/w185{r['poster_path']}" if r.get("poster_path") else "",
                            "overview": r.get("overview", ""), "tmdb_rating": r.get("vote_average")}
                    if iid not in ratings:
                        ratings[iid] = {"rating": score, "date": date}
                        imported += 1
            time.sleep(0.15)
    save_titles(titles_db)
    save_user_ratings(user, ratings)
    print(f"Letterboxd: imported {imported} ratings for {user}")
    return imported

def import_csv(user, text):
    """Import IMDB CSV export. Splits data into shared titles + user ratings."""
    titles = load_titles(); ratings = {}
    for row in csv.DictReader(io.StringIO(text)):
        iid = row.get("Const", "")
        if not iid: continue
        try: imdb_r = float(row["IMDb Rating"]) if row.get("IMDb Rating") and row["IMDb Rating"] != "null" else None
        except: imdb_r = None
        try: votes = int(row["Num Votes"]) if row.get("Num Votes") and row["Num Votes"] != "null" else 0
        except: votes = 0
        if iid not in titles: titles[iid] = {}
        titles[iid].update({k: v for k, v in {"title": row.get("Title",""), "year": row.get("Year",""),
            "type": row.get("Title Type",""), "imdb_rating": imdb_r, "votes": votes,
            "genres": row.get("Genres",""), "directors": row.get("Directors","")}.items() if v})
        ratings[iid] = {"rating": int(row.get("Your Rating", 0)), "date": row.get("Date Rated", "")}
    # Fill missing data from IMDB dataset
    cache = load_imdb_cache()
    if cache:
        for iid, t in titles.items():
            ds = cache.get(iid)
            if not ds: continue
            for k in ("genres", "runtime", "imdb_rating", "votes"):
                if ds.get(k) and not t.get(k): t[k] = ds[k]
    save_titles(titles); save_user_ratings(user, ratings)
    print(f"Imported {len(ratings)} ratings for {user}, {len(titles)} titles total")

# ── HTML rendering ────────────────────────────────────────────────────
def _render_provider_config(user):
    all_provs = get_all_providers()
    active = get_user_active_providers(user)
    if not all_provs:
        return '<p style="color:#888">Add a TMDB API key to see available providers</p>'
    checks = ""
    for p in all_provs:
        name = p["name"]
        checked = "checked" if name in active else ""
        icon = PROVIDER_ICONS.get(name, "")
        checks += '<label style="display:inline-block;margin:3px 8px;cursor:pointer"><input type="checkbox" name="prov" value="' + name + '" ' + checked + '> ' + icon + ' ' + name + '</label>'
    return '<form method="POST" action="' + BASE + '/providers/' + user + '"><div style="max-height:200px;overflow-y:auto;background:#1a1a2e;padding:8px;border-radius:6px">' + checks + '</div><button type="submit" style="margin-top:8px;padding:6px 16px;background:#4fc3f7;border:none;border-radius:6px;cursor:pointer">Save subscriptions</button></form>'

def _render_media_servers(user):
    config = load_user_media_config(user)
    html = ""
    for stype, sinfo in MEDIA_SERVERS.items():
        sc = config.get(stype, {})
        status = '<span style="color:#2d7">connected</span>' if sc.get("enabled") else ""
        fields = ""
        for fname in sinfo["fields"]:
            val = sc.get(fname, "")
            placeholder = "http://192.168.1.x:" + {"plex":"32400","jellyfin":"8096","emby":"8096","kodi":"8080","radarr":"7878","sonarr":"8989","tmm":"7878"}.get(stype,"8080") if fname == "url" else "API token"
            fields += '<input name="' + fname + '" value="' + val + '" placeholder="' + placeholder + '" style="width:45%;display:inline-block;margin-right:4px">'
        html += '<div style="margin:8px 0"><b>' + sinfo["name"] + '</b> ' + status + '<form method="POST" action="' + BASE + '/media/' + user + '" style="display:inline"><input type="hidden" name="type" value="' + stype + '">' + fields + '<button type="submit" style="padding:4px 10px;background:#4fc3f7;border:none;border-radius:4px;cursor:pointer;font-size:.85em">Save</button></form></div>'
    sync_btn = '<a href="' + BASE + '/media/sync/' + user + '" style="display:inline-block;margin-top:8px;padding:6px 16px;background:#16213e;border:1px solid #4fc3f7;border-radius:6px;color:#4fc3f7;text-decoration:none">🔄 Sync all servers</a>' if config else ""
    return html + sync_btn

def render_ratings(user):
    titles = load_titles(); ratings = load_user_ratings(user)
    if not ratings: return render_setup(user)
    tmm = load_user_tmm(user)
    user_provs = get_user_active_providers(user)
    genres = sorted(set(g.strip() for iid in ratings for g in titles.get(iid, {}).get("genres", "").split(",") if g.strip()))
    genre_opts = "".join(f'<option value="{g}">{g}</option>' for g in genres)
    has_trakt = load_user_trakt_token(user) is not None
    services = []
    if TMDB_KEY: services.append("TMDB✓")
    if OMDB_KEY: services.append("OMDB✓")
    if TVDB_KEY: services.append("TVDB✓")
    leaving = get_leaving_titles()
    leaving_html = ""
    if leaving:
        lrows = ""
        for l in leaving[:20]:
            poster = '<img src="' + l.get("poster","") + '" height=40>' if l.get("poster") else ""
            lost = ", ".join(l["lost_from"])
            still = ", ".join(l["still_on"]) or "nowhere"
            lrows += "<tr><td>" + poster + "</td><td>" + l["title"] + " (" + l["year"] + ")</td><td>Left: " + lost + "</td><td>Still on: " + still + "</td></tr>"
        leaving_html = '<details style="margin-bottom:15px"><summary style="cursor:pointer;color:#d72">' + str(len(leaving)) + ' titles recently left a service</summary><table style="margin-top:8px">' + lrows + '</table></details>'
    rows = ""
    sorted_ratings = sorted(ratings.items(), key=lambda x: x[1].get("date", ""), reverse=True)
    for iid, r in sorted_ratings:
        t = titles.get(iid, {})
        c = "#2d7" if r["rating"] >= 7 else "#d72" if r["rating"] <= 4 else "#aaa"
        imdb = f'{t.get("imdb_rating","")}' if t.get("imdb_rating") else "—"
        poster = f'<img src="{t["poster"]}" height="70" loading="lazy">' if t.get("poster") else ""
        scores = []
        if t.get("rotten_tomatoes"): scores.append(f'🍅{t["rotten_tomatoes"]}')
        if t.get("metacritic"): scores.append(f'Ⓜ{t["metacritic"]}')
        if t.get("tmdb_rating"): scores.append(f'T{t["tmdb_rating"]}')
        provs = t.get("providers", [])
        mine = [p for p in provs if p in user_provs]
        stream = ""
        if mine:
            icons = " ".join(PROVIDER_ICONS.get(p, "▪") for p in mine)
            link = t.get("watch_link", "")
            stream = f'<a href="{link}" target="_blank" title="{", ".join(mine)}">{icons}</a>' if link else f'<span title="{", ".join(mine)}">{icons}</span>'
        local_info = tmm.get(iid, {})
        local_src = local_info.get("source", "tmm") if local_info else ""
        has_subs = bool(local_info.get("subtitles")) if local_info else False
        has_suggested = bool(local_info.get("suggested_sub")) if local_info else False
        sub_icon = "🗨" if has_subs else ("💬" if has_suggested else ('<a href="' + BASE + '/subs/' + iid + '" title="Find subtitles">🔤</a>' if iid in tmm else ""))
        local = ('💾 ' + local_src + " " + sub_icon) if iid in tmm else ""
        tooltip = f' title="{t.get("overview","")[:200]}"' if t.get("overview") else ""
        awards_badge = " 🏆" if t.get("awards") and ("Oscar" in t.get("awards","") or "Won" in t.get("awards","")) else ""
        trailer_link = (' <a href="' + t.get("trailer","") + '" target="_blank" title="Trailer">▶️</a>') if t.get("trailer") else ""
        similar_link = ' <a href="' + BASE + '/similar/' + iid + '" title="Similar">🔗</a>'
        rows += f'<tr data-g="{t.get("genres","")}" data-r="{r["rating"]}" data-s="{" ".join(provs)}" data-d="{str(t.get("year",""))[:3]}0"><td>{poster}</td><td><a href="https://www.imdb.com/title/{iid}/" target="_blank"{tooltip}>{t.get("title",iid)}</a>{awards_badge}{trailer_link}{similar_link}</td><td>{t.get("year","")}</td><td style="font-weight:bold;color:{c}">{r["rating"]}</td><td>{imdb}</td><td class="x">{" ".join(scores)}</td><td>{stream}</td><td class="x">{t.get("genres","")}</td><td class="x">{r.get("date","")}</td><td>{local}</td></tr>'
    jb = active_job()[1]
    job_banner = f'<div id="jb" style="background:#1a3a1a;padding:8px 15px;border-radius:6px;margin-bottom:10px"><span id="jm">⏳ {jb["name"]}: {jb["message"]}</span> <progress id="jp" max="100" value="{jb["progress"]/max(jb["total"],1)*100 if jb else 0}" style="vertical-align:middle"></progress></div><script>setInterval(()=>fetch("{BASE}/jobs").then(r=>r.json()).then(d=>{{let a=Object.values(d).find(j=>j.status=="running");if(a){{document.getElementById("jb").style.display="block";document.getElementById("jm").textContent="⏳ "+a.name+": "+a.message;document.getElementById("jp").value=a.total?a.progress/a.total*100:0}}else{{document.getElementById("jb").style.display="none"}}}}),3000)</script>' if jb else ""
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{user}'s Ratings ({len(ratings)})</title>
<style>body{{font-family:-apple-system,sans-serif;margin:20px;background:var(--bg,#1a1a2e);color:var(--fg,#eee)}}
:root{{--bg:#1a1a2e;--fg:#eee;--card:#16213e;--border:#333;--accent:#4fc3f7}}
.light{{--bg:#f5f5f5;--fg:#222;--card:#fff;--border:#ddd;--accent:#0077cc}}
@media(max-width:768px){{table{{font-size:.8em}}th,td{{padding:4px 6px}}img{{height:50px!important}}.bar{{flex-direction:column}}.x{{display:none}}}}
table{{border-collapse:collapse;width:100%}}th,td{{padding:6px 10px;text-align:left;border-bottom:1px solid #333}}
th{{background:#16213e;position:sticky;top:0;cursor:pointer;white-space:nowrap}}th:hover{{background:#1a3a5e}}
tr:hover{{background:#16213e}}a{{color:#4fc3f7;text-decoration:none}}img{{border-radius:4px}}.x{{font-size:.8em;color:#aaa}}
.bar{{display:flex;gap:10px;align-items:center;margin-bottom:15px;flex-wrap:wrap}}
input,select{{padding:6px;border-radius:4px;border:1px solid #444;background:#16213e;color:#eee}}</style>
<script>function f(){{const q=document.getElementById('s').value.toLowerCase(),g=document.getElementById('g').value,mr=document.getElementById('mr').value,st=document.getElementById('st').value,dec=document.getElementById('dec').value;
document.querySelectorAll('tbody tr').forEach(r=>r.style.display=(r.textContent.toLowerCase().includes(q)&&(!g||r.dataset.g.includes(g))&&(!mr||parseInt(r.dataset.r)>=parseInt(mr))&&(!st||r.dataset.s.includes(st))&&(!dec||r.dataset.d===dec))?'':'none')}}
function sortTable(n){{const tb=document.querySelector('tbody'),rows=[...tb.rows],dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:'';
rows.sort((a,b)=>{{let x=a.cells[n].textContent,y=b.cells[n].textContent;return(!isNaN(x)&&!isNaN(y)?(x-y):x.localeCompare(y))*dir}});rows.forEach(r=>tb.appendChild(r))}}</script><script>if(localStorage.getItem("theme")==="light")document.body.classList.add("light")</script></head><body>
{job_banner}
<div style="display:flex;justify-content:space-between;align-items:center"><h2>🎬 {user}'s Ratings — {len(ratings)} titles</h2>{render_user_bar(user)}</div>
<div class="bar"><input id="s" onkeyup="f()" placeholder="Search..." style="width:220px">
<select id="g" onchange="f()"><option value="">All genres</option>{genre_opts}</select>
<select id="mr" onchange="f()"><option value="">Min ★</option>{''.join(f'<option value="{i}">{i}+</option>' for i in range(10,0,-1))}</select>
<select id="dec" onchange="f()"><option value="">All decades</option><option value="2020">2020s</option><option value="2010">2010s</option><option value="2000">2000s</option><option value="1990">1990s</option><option value="1980">1980s</option><option value="1970">1970s</option><option value="1960">1960s</option><option value="1950">1950s</option></select>
<select id="st" onchange="f()"><option value="">All streams</option>{"".join('<option value="' + p + '">' + PROVIDER_ICONS.get(p,"▪") + " " + p + '</option>' for p in sorted(user_provs))}</select>
<a href="{BASE}/tonight/{user}">🎲 <span title="Pick a random recommendation">Tonight</span></a> <a href="{BASE}/stats/{user}" title="Your rating stats">📊</a> <a href="{BASE}/export/{user}" title="Export ratings as CSV">⬇</a> <a href="{BASE}/enrich" title="Enrich titles with metadata">⚡</a> <a href="{BASE}/recs/{user}" title="Personalized recommendations">🎯 Recs</a> <a href="{BASE}/catalog" title="Streaming catalog">📺</a> <a href="{BASE}/new" title="New on streaming">🆕</a> <a href="{BASE}/random/{user}" title="Random unwatched title">🎰</a> <a href="{BASE}/compare/" title="Compare users">👥</a> <a href="{BASE}/rss/{user}" title="RSS feed of your ratings">📡</a> <a href="{BASE}/setup/{user}" title="Setup & configuration">⚙</a> <a href="{BASE}/library/{user}" title="Library curation">📚</a>
{f'<a href="{BASE}/trakt/sync/{user}">↕ Trakt</a>' if has_trakt else ""}
<button onclick="document.body.classList.toggle('light');localStorage.setItem('theme',document.body.classList.contains('light')?'light':'dark')" style="background:none;border:1px solid #444;border-radius:4px;cursor:pointer;padding:2px 8px;color:var(--fg)" title="Toggle dark/light theme">🌓</button> <span style="color:#666;font-size:.8em">{" ".join(services)}</span></div>
<div style="margin-bottom:10px;font-size:1.2em">Mood: <a href="{BASE}/mood/{user}/light" title="Light" style="text-decoration:none">☀️</a><a href="{BASE}/mood/{user}/intense" title="Intense" style="text-decoration:none">🔥</a><a href="{BASE}/mood/{user}/funny" title="Funny" style="text-decoration:none">😂</a><a href="{BASE}/mood/{user}/mind-bending" title="Mind-Bending" style="text-decoration:none">🌀</a><a href="{BASE}/mood/{user}/dark" title="Dark" style="text-decoration:none">🌑</a><a href="{BASE}/mood/{user}/epic" title="Epic" style="text-decoration:none">⚔️</a><a href="{BASE}/mood/{user}/romantic" title="Romantic" style="text-decoration:none">💕</a><a href="{BASE}/mood/{user}/scary" title="Scary" style="text-decoration:none">👻</a><a href="{BASE}/mood/{user}/inspiring" title="Inspiring" style="text-decoration:none">✨</a></div>
<table><thead><tr><th></th><th onclick="sortTable(1)">Title</th><th onclick="sortTable(2)">Year</th><th onclick="sortTable(3)">★</th><th onclick="sortTable(4)">IMDB</th><th>Scores</th><th>Stream</th><th onclick="sortTable(7)">Genres</th><th onclick="sortTable(8)">Rated</th><th>💾</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""

def render_recs(user):
    titles = load_titles()
    cats, profile = get_5cat_recommendations(user, titles, n_per_cat=6)
    watchlist = set(load_watchlist(user))
    
    top_kw = sorted(profile["keywords"].items(), key=lambda x: x[1], reverse=True)[:12]
    top_g = sorted(profile["genres"].items(), key=lambda x: x[1], reverse=True)[:6]
    top_d = sorted(profile["directors"].items(), key=lambda x: x[1], reverse=True)[:5]
    
    taste_kw = " ".join('<span style="background:#16213e;padding:2px 8px;border-radius:10px;font-size:.8em">' + k + '</span>' for k, v in top_kw)
    taste_g = " ".join('<span style="background:#1a3a5e;padding:2px 8px;border-radius:10px;font-size:.8em">' + k + '</span>' for k, v in top_g)
    taste_d = " ".join('<span style="background:#3a1a5e;padding:2px 8px;border-radius:10px;font-size:.8em">' + k + '</span>' for k, v in top_d)
    
    cat_meta = [
        ("dna", "🧬 Deep Cuts for You", "Based on your keyword DNA — themes, moods, plot elements"),
        ("cast", "🎬 The Director\'s Chair", "From creators and actors you love"),
        ("community", "👥 Community Picks", "Loved by users with similar taste"),
        ("overlap", "✅ Unanimous Hits", "Highly rated across IMDB, TMDB, and critics"),
        ("rewatch", "💫 Blast from the Past", "Favorites you haven\'t seen in years"),
    ]
    
    sections = ""
    for cat_key, cat_title, cat_desc in cat_meta:
        items = cats.get(cat_key, [])
        if not items: continue
        cards = ""
        for iid, t, score in items:
            poster = '<img src="' + t.get("poster","") + '" style="border-radius:6px;height:120px" loading="lazy">' if t.get("poster") else ""
            provs = " ".join(PROVIDER_ICONS.get(p,"") for p in t.get("providers",[]) if p in get_user_active_providers(user))
            wl = '<a href="' + BASE + '/watchlist/add/' + iid + '">🤍</a>' if iid not in watchlist else '<a href="' + BASE + '/watchlist/rm/' + iid + '">❤️</a>'
            trailer = ' <a href="' + t.get("trailer","") + '" target="_blank">▶️</a>' if t.get("trailer") else ""
            diverge = ' <span title="Score divergence detected" style="color:#d72">⚠</span>' if score_divergence(t) else ""
            stars = "".join('<a href="' + BASE + '/rate/' + user + '/' + iid + '/' + str(s) + '" style="text-decoration:none;color:gold" title="' + str(s) + '">' + ("★" if s <= 5 else "☆") + '</a>' for s in range(1, 11))
            tooltip = t.get("overview","")[:150]
            cards += '<div style="background:var(--card,#16213e);border-radius:10px;padding:12px;display:flex;gap:12px;align-items:start">'
            cards += poster
            cards += '<div style="flex:1;min-width:0">'
            cards += '<div style="display:flex;justify-content:space-between;align-items:center"><b><a href="https://www.imdb.com/title/' + iid + '/" target="_blank" title="' + tooltip + '">' + t.get("title","?") + '</a></b> ' + wl + trailer + diverge + '</div>'
            cards += '<div style="color:#888;font-size:.85em">' + str(t.get("year","")) + ' · ' + provs + ' · IMDB ' + str(t.get("imdb_rating","?")) + ' · Match: ' + str(score) + '</div>'
            cards += '<div style="font-size:.8em;color:#666;margin-top:4px">' + (t.get("genres",""))[:50] + '</div>'
            cards += '<div style="margin-top:4px">' + stars + '</div>'
            cards += '</div></div>'
        sections += '<div style="margin-bottom:25px"><h3>' + cat_title + '</h3>'
        sections += '<p style="color:#888;font-size:.85em;margin-top:-10px">' + cat_desc + '</p>'
        sections += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(350px,1fr));gap:10px">' + cards + '</div></div>'
    
    user_bar = render_user_bar(user, "recs", False)
    html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Recommendations for ' + user + '</title>'
    html += '<meta name="viewport" content="width=device-width,initial-scale=1">'
    html += '<style>body{font-family:-apple-system,sans-serif;margin:20px;background:var(--bg,#1a1a2e);color:var(--fg,#eee)}'
    html += ':root{--bg:#1a1a2e;--fg:#eee;--card:#16213e}.light{--bg:#f5f5f5;--fg:#222;--card:#fff}'
    html += 'a{color:#4fc3f7;text-decoration:none}h3{margin-bottom:5px}'
    html += '@media(max-width:768px){div[style*="grid"]{grid-template-columns:1fr!important}}'
    html += '</style><script>if(localStorage.getItem("theme")==="light")document.body.classList.add("light")</script></head><body>'
    html += '<div style="display:flex;justify-content:space-between;align-items:center">'
    html += '<h2>🎯 Recommendations for ' + user + '</h2>' + user_bar + '</div>'
    html += '<details style="margin-bottom:20px"><summary style="cursor:pointer;color:#4fc3f7">Your taste profile</summary>'
    html += '<p><b>Keywords:</b> ' + taste_kw + '</p>'
    html += '<p><b>Genres:</b> ' + taste_g + '</p>'
    html += '<p><b>Directors:</b> ' + taste_d + '</p></details>'
    html += sections
    html += '<p style="margin-top:20px"><a href="' + BASE + '/tonight/' + user + '">🎲 Pick one for tonight</a> · '
    html += '<a href="' + BASE + '/u/' + user + '">← Ratings</a></p></body></html>'
    return html


def render_setup(user):
    has_trakt = load_user_trakt_token(user) is not None
    user_bar = render_user_bar(user, "setup")
    media_servers = _render_media_servers(user)
    provider_config = _render_provider_config(user)
    trakt_section = '<span style="color:#2d7">✓ Connected</span> <a href="' + BASE + '/trakt/auth/' + user + '">(reconnect)</a>' if has_trakt else ('<a href="' + BASE + '/trakt/auth/' + user + '"><button>Connect Trakt</button></a>' if TRAKT_ID else '')
    
    # Build page with concatenation (avoids f-string issues with JS braces)
    html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Setup</title>'
    html += '<meta name="viewport" content="width=device-width,initial-scale=1">'
    html += '<style>body{font-family:sans-serif;background:#1a1a2e;color:#eee;display:flex;justify-content:center;padding-top:30px}'
    html += '.box{background:#16213e;padding:30px;border-radius:12px;max-width:600px;width:100%}'
    html += 'a{color:#4fc3f7}input,textarea{width:100%;padding:8px;border-radius:4px;border:1px solid #444;background:#1a1a2e;color:#eee;margin:8px 0;box-sizing:border-box}'
    html += 'button{padding:10px 30px;background:#4fc3f7;border:none;border-radius:6px;cursor:pointer;font-size:1em;margin-top:10px}'
    html += 'hr{border-color:#333;margin:20px 0}</style></head>'
    html += '<body><div class="box">'
    html += '<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap">'
    html += '<h2>Setup — ' + user + '</h2>' + user_bar + '</div>'
    
    # Upload CSV
    html += '<h3>Upload IMDB CSV</h3>'
    html += '<form method="POST" action="' + BASE + '/upload/' + user + '" enctype="multipart/form-data">'
    html += '<input type="file" name="csv" accept=".csv"><button type="submit">Upload</button></form><hr>'
    
    # API Keys
    html += '<h3>API Keys</h3>'
    html += '<form method="POST" action="' + BASE + '/keys">'
    html += '<label>TMDB</label><input name="tmdb" value="' + TMDB_KEY + '">'
    html += '<label>OMDB</label><input name="omdb" value="' + OMDB_KEY + '">'
    html += '<label>TVDB</label><input name="tvdb" value="' + TVDB_KEY + '">'
    html += '<label>OpenSubtitles (<a href="https://www.opensubtitles.com/consumers" target="_blank">get key</a>)</label>'
    html += '<input name="opensubs" placeholder="OpenSubtitles API key">'
    html += '<button type="submit">Save</button></form><hr>'
    
    # Trakt
    html += '<h3>Trakt</h3>' + trakt_section + '<hr>'
    
    # Media Servers + LAN Scanner
    html += '<h3>Media Servers</h3>' + media_servers + '<hr>'
    
    # Local Library
    html += '<h3>Local Library (TMM / file upload)</h3>'
    html += '<form method="POST" action="' + BASE + '/tmm/' + user + '" enctype="multipart/form-data">'
    html += '<input type="file" name="tmm" accept=".csv,.txt"><button type="submit">Upload</button></form><hr>'
    
    # Streaming Providers
    html += '<h3>My Streaming Services</h3>' + provider_config + '<hr>'
    
    # IMDB Dataset
    html += '<h3>Agent Token</h3>'
    html += '<p>Token for the LAN agent to push library data. Set in agent.json as <code>token</code>.</p>'
    html += '<form method="POST" action="' + BASE + '/keys">'
    html += '<input name="agent_token" value="' + AGENT_TOKEN + '" placeholder="Generate a random token">'
    html += '<button type="submit">Save</button></form><hr>'
    html += '<h3>IMDB Dataset</h3>'
    html += '<p>Download IMDB bulk data (200K+ titles, ~220MB). Eliminates most API calls.</p>'
    html += '<a href="' + BASE + '/datasets/download" style="display:inline-block;padding:8px 16px;background:#1a1a2e;border:1px solid #4fc3f7;border-radius:6px;color:#4fc3f7;text-decoration:none">Download IMDB Datasets</a><hr>'
    
    # Streaming Region
    html += '<h3>Streaming Region</h3>'
    html += '<p>Region: <b>' + WATCH_COUNTRY + '</b> | <a href="' + BASE + '/catalog">Browse catalog</a></p>'
    
    html += '</div></body></html>'
    return html


def render_library(user):
    """Library curation: duplicates, quality comparison, cleanup suggestions."""
    library = load_user_tmm(user)
    titles = load_titles()
    if not library:
        return '<html><body style="background:#1a1a2e;color:#eee;font-family:sans-serif;padding:40px"><h2>No local library synced</h2><a href="' + BASE + '/setup/' + user + '" style="color:#4fc3f7">Setup media servers</a></body></html>'

    # Find duplicates by IMDB ID (multiple files for same title)
    # Group by title name (case-insensitive) to catch different versions
    from collections import defaultdict
    by_title = defaultdict(list)
    for iid, info in library.items():
        if not isinstance(info, dict): continue
        name = (info.get("title") or titles.get(iid, {}).get("title") or iid).lower().strip()
        by_title[name].append((iid, info))

    # Also detect same IMDB ID with different paths (true duplicates)
    # For now, focus on quality comparison of all titles
    total = len(library)
    has_video = sum(1 for v in library.values() if isinstance(v, dict) and v.get("video_codec"))
    no_subs = sum(1 for v in library.values() if isinstance(v, dict) and not v.get("subtitles"))

    # Quality breakdown
    quality_dist = defaultdict(int)
    codec_dist = defaultdict(int)
    for info in library.values():
        if not isinstance(info, dict): continue
        h = info.get("video_height") or info.get("quality", "")
        if h:
            h = int(h) if str(h).isdigit() else 0
            if h >= 2160: quality_dist["4K"] += 1
            elif h >= 1080: quality_dist["1080p"] += 1
            elif h >= 720: quality_dist["720p"] += 1
            elif h > 0: quality_dist["SD"] += 1
        c = info.get("video_codec", "")
        if c: codec_dist[c] += 1

    # Find titles with multiple entries (potential duplicates)
    dupes = [(name, entries) for name, entries in by_title.items() if len(entries) > 1]
    dupes.sort(key=lambda x: len(x[1]), reverse=True)

    # Build duplicate rows
    dupe_rows = ""
    for name, entries in dupes[:50]:
        for iid, info in entries:
            h = info.get("video_height") or info.get("quality", "")
            codec = info.get("video_codec", "")
            audio = info.get("audio", [])
            audio_str = ", ".join(a.get("codec","") + " " + str(a.get("channels","")) + "ch " + a.get("language","") for a in audio[:3]) if audio else "?"
            subs = info.get("subtitles", [])
            sub_str = ", ".join(s.get("language","") for s in subs[:5]) if subs else "none"
            path = info.get("path", "")
            # Suggest: keep highest resolution
            best = max(entries, key=lambda x: int(x[1].get("video_height", 0) or 0) if isinstance(x[1], dict) else 0)
            is_best = "✅" if (iid, info) == best else "❌"
            dupe_rows += "<tr><td>" + name[:40] + "</td><td>" + iid + "</td><td>" + str(h) + "</td><td>" + codec + "</td><td>" + audio_str + "</td><td>" + sub_str + "</td><td>" + is_best + "</td><td style='font-size:.7em;color:#666'>" + path[-60:] + "</td></tr>"
        dupe_rows += '<tr><td colspan="8" style="border-bottom:2px solid #4fc3f7"></td></tr>'

    # Quality bars
    q_bars = ""
    for q in ["4K", "1080p", "720p", "SD"]:
        c = quality_dist.get(q, 0)
        if c: q_bars += '<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:50px;text-align:right">' + q + '</span><div style="background:#4fc3f7;height:18px;width:' + str(min(c/max(quality_dist.values())*300, 300)) + 'px;border-radius:3px"></div><span style="color:#888">' + str(c) + '</span></div>'

    codec_bars = ""
    for c, n in sorted(codec_dist.items(), key=lambda x: x[1], reverse=True)[:8]:
        codec_bars += '<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:60px;text-align:right;font-size:.85em">' + c + '</span><div style="background:#4fc3f7;height:16px;width:' + str(min(n/max(codec_dist.values())*250, 250)) + 'px;border-radius:3px"></div><span style="color:#888">' + str(n) + '</span></div>'

    html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Library Curation</title>'
    html += '<meta name="viewport" content="width=device-width,initial-scale=1">'
    html += '<style>body{font-family:-apple-system,sans-serif;background:#1a1a2e;color:#eee;margin:20px}'
    html += '.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:15px}'
    html += '.card{background:#16213e;padding:15px;border-radius:10px}'
    html += 'table{border-collapse:collapse;width:100%}th,td{padding:4px 8px;text-align:left;border-bottom:1px solid #333;font-size:.85em}'
    html += 'th{background:#16213e;position:sticky;top:0}a{color:#4fc3f7;text-decoration:none}</style></head><body>'
    html += '<h2>📚 Library Curation — ' + user + '</h2>'

    # Stats cards
    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em">' + str(total) + '</div>total titles</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em">' + str(has_video) + '</div>with media info</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em;color:#d72">' + str(no_subs) + '</div>missing subtitles</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em;color:#f90">' + str(len(dupes)) + '</div>potential duplicates</div>'
    html += '</div>'

    # Quality & codec breakdown
    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card"><h3>Resolution</h3>' + q_bars + '</div>'
    html += '<div class="card"><h3>Codecs</h3>' + codec_bars + '</div>'
    html += '</div>'

    # Duplicates table
    if dupes:
        html += '<h3>🔍 Potential Duplicates (' + str(len(dupes)) + ' titles)</h3>'
        html += '<p style="color:#888;font-size:.85em">✅ = suggested keep (highest resolution) · ❌ = candidate for removal</p>'
        html += '<table><thead><tr><th>Title</th><th>IMDB</th><th>Res</th><th>Codec</th><th>Audio</th><th>Subs</th><th>Keep?</th><th>Path</th></tr></thead>'
        html += '<tbody>' + dupe_rows + '</tbody></table>'

    html += '<p style="margin-top:20px"><a href="' + BASE + '/u/' + user + '">← Ratings</a> · <a href="' + BASE + '/setup/' + user + '">⚙ Setup</a></p>'
    html += '</body></html>'
    return html

def render_stats(user):
    titles = load_titles()
    ratings = load_user_ratings(user)
    if not ratings:
        return '<html><body style="background:#1a1a2e;color:#eee;padding:40px;font-family:sans-serif"><h2>No ratings</h2></body></html>'
    scores = [r["rating"] for r in ratings.values()]
    avg = sum(scores) / len(scores)
    genre_count, director_count = {}, {}
    for iid, r in ratings.items():
        t = titles.get(iid, {})
        for g in (t.get("genres") or "").split(","):
            g = g.strip()
            if g: genre_count[g] = genre_count.get(g, 0) + 1
        for d in (t.get("directors") or "").split(","):
            d = d.strip()
            if d: director_count[d] = director_count.get(d, 0) + 1
    top_genres = sorted(genre_count.items(), key=lambda x: x[1], reverse=True)[:12]
    top_dirs = sorted(director_count.items(), key=lambda x: x[1], reverse=True)[:10]
    rating_dist = [sum(1 for s in scores if s == i) for i in range(1, 11)]
    max_bar = max(rating_dist) or 1
    dist_bars = "".join('<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:30px;text-align:right">' + str(i) + '</span><div style="background:#4fc3f7;height:18px;width:' + str(rating_dist[i-1]/max_bar*300) + 'px;border-radius:3px"></div><span style="color:#888;font-size:.85em">' + str(rating_dist[i-1]) + '</span></div>' for i in range(10, 0, -1))
    genre_bars = "".join('<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:100px;text-align:right;font-size:.85em">' + g + '</span><div style="background:#4fc3f7;height:16px;width:' + str(c/top_genres[0][1]*250) + 'px;border-radius:3px"></div><span style="color:#888;font-size:.85em">' + str(c) + '</span></div>' for g, c in top_genres)
    dir_list = "".join("<tr><td>" + d + "</td><td>" + str(c) + "</td></tr>" for d, c in top_dirs)
    html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Stats</title>'
    html += '<meta name="viewport" content="width=device-width,initial-scale=1">'
    html += '<style>body{font-family:-apple-system,sans-serif;background:#1a1a2e;color:#eee;margin:20px}'
    html += '.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(300px,1fr));gap:20px}'
    html += '.card{background:#16213e;padding:20px;border-radius:12px}'
    html += 'table{border-collapse:collapse;width:100%}td{padding:4px 8px;border-bottom:1px solid #333}'
    html += 'a{color:#4fc3f7}</style></head><body>'
    html += '<h2>📊 ' + user + " Stats</h2>"
    html += '<div style="display:flex;gap:20px;margin-bottom:20px;flex-wrap:wrap">'
    html += '<div class="card" style="text-align:center"><div style="font-size:3em">' + str(len(ratings)) + '</div>titles</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:3em">' + f"{avg:.1f}" + '</div>average</div></div>'
    html += '<div class="grid"><div class="card"><h3>Rating Distribution</h3>' + dist_bars + '</div>'
    html += '<div class="card"><h3>Top Genres</h3>' + genre_bars + '</div>'
    html += '<div class="card"><h3>Top Directors</h3><table>' + dir_list + '</table></div></div>'
    html += '<p><a href="' + BASE + '/u/' + user + '">← Back</a></p></body></html>'
    return html

def render_compare(u1, u2):
    titles = load_titles()
    r1, r2 = load_user_ratings(u1), load_user_ratings(u2)
    common = set(r1.keys()) & set(r2.keys())
    agree = [(titles.get(i,{}).get("title","?"), r1[i]["rating"], r2[i]["rating"]) for i in common if abs(r1[i]["rating"]-r2[i]["rating"]) <= 1]
    disagree = [(titles.get(i,{}).get("title","?"), r1[i]["rating"], r2[i]["rating"], abs(r1[i]["rating"]-r2[i]["rating"])) for i in common if abs(r1[i]["rating"]-r2[i]["rating"]) >= 3]
    disagree.sort(key=lambda x: x[3], reverse=True)
    agree_rows = "".join("<tr><td>" + t + "</td><td>" + str(a) + "</td><td>" + str(b) + "</td></tr>" for t,a,b in agree[:15])
    disagree_rows = "".join("<tr><td>" + t + "</td><td>" + str(a) + "</td><td>" + str(b) + "</td><td>" + str(d) + "</td></tr>" for t,a,b,d in disagree[:15])
    html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>Compare</title>'
    html += '<style>body{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}'
    html += '.card{background:#16213e;padding:20px;border-radius:12px;display:inline-block;margin:5px;text-align:center}'
    html += 'table{border-collapse:collapse;width:100%}td,th{padding:4px 8px;border-bottom:1px solid #333;text-align:left}'
    html += 'a{color:#4fc3f7}</style></head><body>'
    html += '<h2>' + u1 + ' vs ' + u2 + '</h2>'
    html += '<div class="card"><div style="font-size:2em">' + str(len(common)) + '</div>both rated</div>'
    html += '<div class="card"><div style="font-size:2em">' + str(len(agree)) + '</div>agree</div>'
    html += '<div class="card"><div style="font-size:2em">' + str(len(disagree)) + '</div>disagree</div>'
    html += '<h3>🤝 Agree</h3><table><tr><th>Title</th><th>' + u1 + '</th><th>' + u2 + '</th></tr>' + agree_rows + '</table>'
    html += '<h3>🥊 Disagree</h3><table><tr><th>Title</th><th>' + u1 + '</th><th>' + u2 + '</th><th>Gap</th></tr>' + disagree_rows + '</table>'
    html += '<p><a href="' + BASE + '/">← Back</a></p></body></html>'
    return html

def render_new_on_streaming():
    if not os.path.exists(CATALOG_FILE) or not os.path.exists(CATALOG_PREV):
        return '<html><body style="background:#1a1a2e;color:#eee;font-family:sans-serif;padding:40px"><h2>Need 2+ catalog refreshes</h2><a href="' + BASE + '/catalog/fetch" style="color:#4fc3f7">Refresh catalog</a></body></html>'
    prev = {c["tmdb_id"] for c in json.load(open(CATALOG_PREV)).get("catalog", [])}
    curr_data = json.load(open(CATALOG_FILE))
    new_titles = [c for c in curr_data.get("catalog", []) if c["tmdb_id"] not in prev]
    new_titles.sort(key=lambda x: x.get("tmdb_rating", 0), reverse=True)
    rows = ""
    for r in new_titles[:50]:
        poster = '<img src="' + r.get("poster","") + '" height="60" loading="lazy">' if r.get("poster") else ""
        provs = " ".join(PROVIDER_ICONS.get(p, "") for p in r.get("providers", []))
        rows += "<tr><td>" + poster + "</td><td>" + r["title"] + "</td><td>" + r.get("year","") + "</td><td>" + str(r.get("tmdb_rating","")) + "</td><td>" + provs + "</td></tr>"
    html = '<!DOCTYPE html><html><head><meta charset="utf-8"><title>New on Streaming</title>'
    html += '<style>body{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}'
    html += 'table{border-collapse:collapse;width:100%}th,td{padding:6px 10px;text-align:left;border-bottom:1px solid #333}'
    html += 'th{background:#16213e}img{border-radius:4px}a{color:#4fc3f7}</style></head>'
    html += '<body><h2>🆕 New on Streaming — ' + str(len(new_titles)) + ' titles</h2>'
    html += '<table><thead><tr><th></th><th>Title</th><th>Year</th><th>TMDB</th><th>On</th></tr></thead>'
    html += '<tbody>' + rows + '</tbody></table>'
    html += '<p><a href="' + BASE + '/">← Back</a></p></body></html>'
    return html

def render_catalog():
    if not os.path.exists(CATALOG_FILE):
        return f'<html><body style="background:#1a1a2e;color:#eee;font-family:sans-serif;padding:40px"><h2>No catalog</h2><a href="{BASE}/catalog/fetch" style="color:#4fc3f7">Fetch catalog for {WATCH_COUNTRY}</a></body></html>'
    data = json.load(open(CATALOG_FILE))
    leaving = get_leaving_titles()
    leaving_html = ""
    if leaving:
        lrows = ""
        for l in leaving[:20]:
            poster = '<img src="' + l.get("poster","") + '" height=40>' if l.get("poster") else ""
            lost = ", ".join(l["lost_from"])
            still = ", ".join(l["still_on"]) or "nowhere"
            lrows += "<tr><td>" + poster + "</td><td>" + l["title"] + " (" + l["year"] + ")</td><td>Left: " + lost + "</td><td>Still on: " + still + "</td></tr>"
        leaving_html = '<details style="margin-bottom:15px"><summary style="cursor:pointer;color:#d72">' + str(len(leaving)) + ' titles recently left a service</summary><table style="margin-top:8px">' + lrows + '</table></details>'
    rows = ""
    for r in data["catalog"]:
        poster = f'<img src="{r["poster"]}" height="60" loading="lazy">' if r.get("poster") else ""
        provs = " ".join(PROVIDER_ICONS.get(p, "▪") for p in r.get("providers", []))
        rows += f'<tr><td>{poster}</td><td>{r["title"]}</td><td>{r.get("year","")}</td><td>{r.get("tmdb_rating","")}</td><td>{provs}</td><td>{r["type"]}</td></tr>'
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Catalog {WATCH_COUNTRY} ({data["count"]})</title>
<style>body{{font-family:-apple-system,sans-serif;margin:20px;background:var(--bg,#1a1a2e);color:var(--fg,#eee)}}
:root{{--bg:#1a1a2e;--fg:#eee;--card:#16213e;--border:#333;--accent:#4fc3f7}}
.light{{--bg:#f5f5f5;--fg:#222;--card:#fff;--border:#ddd;--accent:#0077cc}}
@media(max-width:768px){{table{{font-size:.8em}}th,td{{padding:4px 6px}}img{{height:50px!important}}.bar{{flex-direction:column}}.x{{display:none}}}}
table{{border-collapse:collapse;width:100%}}th,td{{padding:6px 10px;text-align:left;border-bottom:1px solid #333}}
th{{background:#16213e;position:sticky;top:0}}tr:hover{{background:#16213e}}img{{border-radius:4px}}
input{{padding:6px;border-radius:4px;border:1px solid #444;background:#16213e;color:#eee;width:250px}}a{{color:#4fc3f7;text-decoration:none}}</style>
<script>function f(){{const q=document.getElementById("s").value.toLowerCase();document.querySelectorAll("tbody tr").forEach(r=>r.style.display=r.textContent.toLowerCase().includes(q)?"":"none")}}</script>
</head><body><h2>📺 Streaming Catalog — {WATCH_COUNTRY} — {data["count"]} titles</h2>
<div style="margin-bottom:15px;display:flex;gap:12px"><input id="s" onkeyup="f()" placeholder="Search...">
<a href="{BASE}/catalog/fetch">↻ Refresh</a> <a href="{BASE}/">← Ratings</a></div>
{leaving_html}<table><thead><tr><th></th><th>Title</th><th>Year</th><th>TMDB</th><th>On</th><th>Type</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""

# ── HTTP Server ───────────────────────────────────────────────────────
class H(BaseHTTPRequestHandler):
    """HTTP request handler. Routes are relative to BASE (stripped by reverse proxy).
    GET routes: /, /u/<user>, /recs/<user>, /catalog, /setup/<user>, /enrich, /jobs
    POST routes: /upload/<user>, /tmm/<user>, /keys"""
    def _user(self, path_parts):
        """Extract user from URL or default to first user."""
        for i, p in enumerate(path_parts):
            if p == "u" and i + 1 < len(path_parts): return path_parts[i + 1]
        users = list_users()
        return users[0] if users else "default"

    def do_GET(self):
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        p = "/" + "/".join(parts)
        user = self._user(parts)

        if p.startswith("/trakt/auth/"):
            u = parts[-1]
            # Store user in state for callback
            os.makedirs(DATA_DIR, exist_ok=True)
            json.dump({"user": u}, open(f"{DATA_DIR}/_trakt_state.json", "w"))
            self._redirect(trakt_auth_url())
        elif p == "/trakt/callback":
            code = qs.get("code", [None])[0]
            state = json.load(open(f"{DATA_DIR}/_trakt_state.json")) if os.path.exists(f"{DATA_DIR}/_trakt_state.json") else {}
            u = state.get("user", user)
            if code:
                token = trakt_exchange_code(code)
                if token and "access_token" in token: save_user_trakt_token(u, token)
            self._redirect(f"{BASE}/")
        elif p.startswith("/trakt/sync/"):
            u = parts[-1]
            if not active_job()[1]: start_job("trakt_sync", _bg_trakt_sync, u)
            self._redirect(f"{BASE}/")
        elif p.startswith("/subs/"):
            imdb_id = parts[-1]
            langs = qs.get("lang", ["en"])
            user = self._user(parts)
            lib = load_user_tmm(user)
            info = lib.get(imdb_id, {})
            subs = opensubs_search(imdb_id, langs, file_hash=info.get("file_hash"), file_size=info.get("file_size"))
            titles = load_titles()
            t = titles.get(imdb_id, {})
            rows = ""
            for s in subs[:20]:
                badge = '<span style="color:#2d7">★ hash match</span>' if s["hash_match"] else ""
                hi = "🦻" if s["hearing_impaired"] else ""
                dl = f'<a href="{BASE}/subs/dl/{s["file_id"]}" title="Export ratings as CSV">⬇</a>' if s["file_id"] else ""
                rows += "<tr><td>" + s["language"] + "</td><td>" + s["release"][:60] + "</td><td>" + str(s["download_count"]) + "</td><td>" + str(s["rating"]) + "</td><td>" + badge + " " + hi + "</td><td>" + dl + "</td></tr>"
            self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Subtitles</title>
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}}table{{border-collapse:collapse;width:100%}}
th,td{{padding:6px 10px;text-align:left;border-bottom:1px solid #333}}th{{background:#16213e}}a{{color:#4fc3f7}}</style></head>
<body><h2>Subtitles for {t.get("title", imdb_id)}</h2>
<table><thead><tr><th>Lang</th><th>Release</th><th>Downloads</th><th>Rating</th><th>Match</th><th></th></tr></thead>
<tbody>{rows}</tbody></table>
<p><a href="{BASE}/">← Back</a></p></body></html>""")
            return
        elif p.startswith("/subs/auto/"):
            u = parts[-1]
            if not active_job()[1]:
                start_job("auto_subs", _bg_auto_subs, u)
            self._redirect(f"{BASE}/u/{u}")
            return
        elif p.startswith("/subs/dl/"):
            file_id = int(parts[-1])
            link = opensubs_download_link(file_id)
            if link:
                self._redirect(link)
            else:
                self._html("<html><body>Download failed</body></html>")
            return
        elif p.startswith("/rss/"):
            u = parts[-1]
            titles = load_titles(); ratings = load_user_ratings(u)
            items = ""
            for iid, r in sorted(ratings.items(), key=lambda x: x[1].get("date",""), reverse=True)[:30]:
                t = titles.get(iid, {})
                items += f"<item><title>{t.get('title',iid)} — {r['rating']}/10</title><link>https://www.imdb.com/title/{iid}/</link><description>{t.get('overview','')[:200]}</description><pubDate>{r.get('date','')}</pubDate></item>\n"
            self.send_response(200)
            self.send_header("Content-Type", "application/rss+xml")
            self.end_headers()
            self.wfile.write(f"""<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel>
<title>{u}'s Ratings</title><description>Recent movie ratings</description>
{items}</channel></rss>""".encode())
            return
        elif p == "/new":
            self._html(render_new_on_streaming())
            return
        elif p.startswith("/random/"):
            u = parts[-1]
            import random
            titles = load_titles(); ratings = load_user_ratings(u)
            provs = get_user_active_providers(u)
            unwatched = [(iid, t) for iid, t in titles.items() if iid not in ratings and set(t.get("providers",[])) & provs]
            if unwatched:
                iid, t = random.choice(unwatched)
                poster = f'<img src="{t.get("poster","")}" style="border-radius:8px;max-height:350px">' if t.get("poster") else ""
                stream = " ".join(PROVIDER_ICONS.get(p,"") for p in t.get("providers",[]) if p in provs)
                trailer = ""
                if t.get("trailer"): trailer = f'<a href="{t["trailer"]}" target="_blank" style="font-size:1.5em">▶️ Trailer</a>'
                self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Random Pick</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;display:flex;justify-content:center;padding:30px;text-align:center}}
.card{{background:#16213e;padding:30px;border-radius:16px;max-width:500px}}a{{color:#4fc3f7}}
button{{padding:10px 24px;background:#4fc3f7;border:none;border-radius:8px;cursor:pointer;font-size:1em;margin:6px}}</style></head>
<body><div class="card">{poster}<h2>{t.get("title","?")} ({t.get("year","")})</h2>
<p style="font-size:1.2em">{stream}</p><p style="color:#aaa">{t.get("overview","")[:250]}</p>
<p>IMDB: {t.get("imdb_rating","-")} | TMDB: {t.get("tmdb_rating","-")}</p>{trailer}
<div style="margin-top:15px"><a href="{BASE}/random/{u}"><button>🎲 Another</button></a>
<a href="{BASE}/u/{u}"><button style="background:#16213e;border:1px solid #4fc3f7;color:#4fc3f7">← Back</button></a></div></div></body></html>""")
            else:
                self._html(f'<html><body style="background:#1a1a2e;color:#eee;padding:40px;font-family:sans-serif"><h2>No unwatched titles on your services</h2><a href="{BASE}/" style="color:#4fc3f7">Back</a></body></html>')
            return
        elif p.startswith("/compare/"):
            # /compare/user1/user2
            if len(parts) >= 3:
                u1, u2 = parts[-2], parts[-1]
                self._html(render_compare(u1, u2))
            else:
                users = list_users()
                links = "".join(f'<a href="{BASE}/compare/{users[i]}/{users[j]}" style="display:inline-block;margin:5px;padding:8px 16px;background:#16213e;border-radius:6px;color:#4fc3f7;text-decoration:none">{users[i]} vs {users[j]}</a>' for i in range(len(users)) for j in range(i+1, len(users)))
                self._html(f'<html><head><meta charset="utf-8"><style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;padding:40px;text-align:center}}</style></head><body><h2>Compare Users</h2>{links or "Need 2+ users"}<br><br><a href="{BASE}/" style="color:#4fc3f7">Back</a></body></html>')
            return
        elif p.startswith("/profile/"):
            u = parts[-1]
            self._html(render_public_profile(u))
            return
        elif p.startswith("/history/"):
            u = parts[-1]
            if not active_job()[1]:
                start_job("history", lambda jid: _bg_history(jid, u))
            self._redirect(f"{BASE}/u/{u}")
            return
        elif p.startswith("/mood/"):
            u = parts[-2] if len(parts) >= 2 else self._user(parts)
            mood = parts[-1]
            titles = load_titles()
            ratings = load_user_ratings(u)
            provs = get_user_active_providers(u)
            results = mood_filter(titles, mood, ratings)
            # Filter to streaming
            results = [(iid, t, s) for iid, t, s in results if set(t.get("providers",[])) & provs][:30]
            rows = ""
            for iid, t, s in results:
                poster = '<img src="' + t.get("poster","") + '" height="60" loading="lazy">' if t.get("poster") else ""
                stream = " ".join(PROVIDER_ICONS.get(p,"") for p in t.get("providers",[]) if p in provs)
                rows += "<tr><td>" + poster + "</td><td>" + t.get("title","") + "</td><td>" + str(t.get("year","")) + "</td><td>" + str(t.get("tmdb_rating","")) + "</td><td>" + stream + "</td></tr>"
            mood_emoji = {"light":"☀️","intense":"🔥","funny":"😂","mind-bending":"🌀","dark":"🌑","epic":"⚔️","romantic":"💕","scary":"👻","inspiring":"✨"}.get(mood,"🎬")
            self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{mood} picks</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}}table{{border-collapse:collapse;width:100%}}
th,td{{padding:6px 10px;text-align:left;border-bottom:1px solid #333}}th{{background:#16213e}}img{{border-radius:4px}}a{{color:#4fc3f7}}</style></head>
<body><h2>{mood_emoji} {mood.title()} picks — {len(results)} titles</h2>
<table><thead><tr><th></th><th>Title</th><th>Year</th><th>TMDB</th><th>Stream</th></tr></thead><tbody>{rows}</tbody></table>
<p style="margin-top:15px"><a href="{BASE}/u/{u}">← Back</a></p></body></html>""")
            return

        elif p.startswith("/stats/"):
            u = parts[-1]
            self._html(render_stats(u))
            return
        elif p.startswith("/export/"):
            u = parts[-1]
            titles = load_titles()
            ratings = load_user_ratings(u)
            csv_out = "Const,Your Rating,Date Rated,Title,Year,Genres,IMDb Rating,Type\n"
            for iid, r in sorted(ratings.items(), key=lambda x: x[1].get("date",""), reverse=True):
                t = titles.get(iid, {})
                csv_out += f'{iid},{r["rating"]},{r.get("date","")},"{t.get("title","")}",{t.get("year","")},"{t.get("genres","")}",{t.get("imdb_rating","")},{t.get("type","")}\n'
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", f'attachment; filename="{u}_ratings.csv"')
            self.end_headers()
            self.wfile.write(csv_out.encode())
            return
        elif p.startswith("/tonight/"):
            u = parts[-1]
            import random
            titles = load_titles()
            recs, _ = get_streaming_recs(u, titles, 20)
            if recs:
                iid, t, score = random.choice(recs[:10])
                provs = " ".join(PROVIDER_ICONS.get(p,"") for p in t.get("providers",[]) if p in get_user_active_providers(u))
                trailer = ""
                if t.get("trailer"):
                    yt_id = t["trailer"].split("v=")[-1] if "v=" in t.get("trailer","") else ""
                    if yt_id: trailer = f'<iframe width="560" height="315" src="https://www.youtube.com/embed/{yt_id}" frameborder="0" allowfullscreen style="border-radius:8px;margin-top:15px"></iframe>'
                poster = f'<img src="{t["poster"]}" style="border-radius:8px;max-height:400px">' if t.get("poster") else ""
                self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Tonight</title>
<meta property="og:title" content="🎬 Tonight: {t.get('title','?')}">
<meta property="og:description" content="{t.get('overview','')[:150]}">
<meta property="og:image" content="{t.get('poster','')}">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:-apple-system,sans-serif;background:#1a1a2e;color:#eee;display:flex;justify-content:center;padding:30px;text-align:center}}
.card{{background:#16213e;padding:30px;border-radius:16px;max-width:600px}}a{{color:#4fc3f7;text-decoration:none}}
button{{padding:12px 30px;background:#4fc3f7;border:none;border-radius:8px;cursor:pointer;font-size:1.1em;margin:8px}}</style></head>
<body><div class="card">
<h1>🎬 Tonight you should watch...</h1>
{poster}
<h2>{t.get("title","?")} ({t.get("year","")})</h2>
<p style="font-size:1.2em">{provs}</p>
<p style="color:#aaa">{t.get("overview","")[:300]}</p>
<p>IMDB: {t.get("imdb_rating","-")} | Match score: {score}</p>
{trailer}
<div style="margin-top:20px">
<a href="{BASE}/tonight/{u}"><button>🎲 Pick another</button></a>
<a href="{BASE}/recs/{u}"><button style="background:#16213e;border:1px solid #4fc3f7;color:#4fc3f7">See all recs</button></a>
<a href="{BASE}/u/{u}"><button style="background:#16213e;border:1px solid #4fc3f7;color:#4fc3f7">← Ratings</button></a>
</div></div></body></html>""")
            else:
                self._html(f'<html><body style="background:#1a1a2e;color:#eee;font-family:sans-serif;padding:40px;text-align:center"><h2>No recommendations yet</h2><p>Enrich your titles first</p><a href="{BASE}/" style="color:#4fc3f7">Back</a></body></html>')
            return
        elif p.startswith("/watchlist/add/"):
            u = self._user(parts)
            iid = parts[-1]
            wl = load_watchlist(u)
            if iid not in wl: wl.append(iid)
            save_watchlist(u, wl)
            self._redirect(self.headers.get("Referer", f"{BASE}/u/{u}"))
            return
        elif p.startswith("/watchlist/rm/"):
            u = self._user(parts)
            iid = parts[-1]
            wl = load_watchlist(u)
            if iid in wl: wl.remove(iid)
            save_watchlist(u, wl)
            self._redirect(self.headers.get("Referer", f"{BASE}/u/{u}"))
            return
        elif p.startswith("/similar/"):
            iid = parts[-1]
            titles = load_titles()
            t = titles.get(iid, {})
            results = tastedive_similar(imdb_id, t.get("title", ""))
            rows = "".join("<tr><td><b>" + r["title"] + "</b></td><td>" + r.get("description","")[:200] + "</td></tr>" for r in results)
            self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Similar</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}}table{{border-collapse:collapse;width:100%}}
td{{padding:8px;border-bottom:1px solid #333}}a{{color:#4fc3f7}}</style></head>
<body><h2>Similar to {t.get("title","?")}</h2>
<table>{rows}</table><p><a href="{BASE}/">← Back</a></p></body></html>""")
            return
        elif p.startswith("/rate/"):
            # Inline rating: /rate/<user>/<imdb_id>/<score>
            u = parts[-3] if len(parts) >= 3 else self._user(parts)
            iid = parts[-2]
            score = int(parts[-1])
            if 1 <= score <= 10:
                ratings = load_user_ratings(u)
                ratings[iid] = {"rating": score, "date": time.strftime("%Y-%m-%d")}
                save_user_ratings(u, ratings)
            self._redirect(self.headers.get("Referer", f"{BASE}/u/{u}"))
            return
        elif p == "/datasets/download":
            if not active_job()[1]:
                start_job("imdb_datasets", lambda jid: (download_imdb_datasets(jid), seed_from_imdb_dataset(jid)))
            self._redirect(f"{BASE}/")
            return
        elif p.startswith("/library/"):
            u = parts[-1] if len(parts) > 1 else self._user(parts)
            self._html(render_library(u))
            return
        elif p.startswith("/media/sync/"):
            u = parts[-1]
            if not active_job()[1]:
                start_job("media_sync", lambda jid: sync_media_servers(u))
            self._redirect(f"{BASE}/u/{u}")
        elif p == "/enrich":
            if not active_job()[1]: start_job("enrich", _bg_enrich)
            self._redirect(f"{BASE}/")
        elif p.startswith("/recs/"):
            u = parts[-1]
            self._html(render_recs(u))
        elif p == "/catalog":
            self._html(render_catalog())
        elif p == "/catalog/fetch":
            if not active_job()[1]: start_job("catalog", _bg_catalog)
            self._redirect(f"{BASE}/catalog")
        elif p.startswith("/setup/"):
            u = parts[-1]
            if u == "new":
                self._html(f'''<html><head><meta charset="utf-8"><title>New User</title>
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;display:flex;justify-content:center;padding-top:80px}}
.box{{background:#16213e;padding:30px;border-radius:12px}}input{{padding:8px;border-radius:4px;border:1px solid #444;background:#1a1a2e;color:#eee;margin:8px}}
button{{padding:10px 20px;background:#4fc3f7;border:none;border-radius:6px;cursor:pointer}}</style></head>
<body><div class="box"><h2>New User</h2><form method="GET" action="{BASE}/setup/create">
<input name="name" placeholder="Username"><button type="submit">Create</button></form></div></body></html>''')
            elif u == "create":
                name = qs.get("name", [""])[0].strip().lower()
                if name: user_dir(name)
                self._redirect(f"{BASE}/setup/{name}" if name else f"{BASE}/setup/new")
            else:
                self._html(render_setup(u))
        elif p == "/jobs":
            self._json(get_jobs())
        elif p == "/api":
            self._json({"titles": len(load_titles()), "users": {u: len(load_user_ratings(u)) for u in list_users()}})
        elif p.startswith("/u/"):
            u = parts[-1]
            self._html(render_ratings(u))
        else:
            self._html(render_ratings(user))

    def do_POST(self):
        global TMDB_KEY, OMDB_KEY, TVDB_KEY, AGENT_TOKEN
        cl = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(cl)
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        if self.path.startswith("/upload/"):
            user = parts[-1]
            boundary = self.headers["Content-Type"].split("boundary=")[1].encode()
            for part in body.split(b"--" + boundary):
                if b'name="csv"' in part:
                    csv_data = part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0]
                    import_csv(user, csv_data.decode("utf-8-sig"))
                    break
            self._redirect(f"{BASE}/u/{user}")
        elif self.path.startswith("/tmm/"):
            user = parts[-1]
            boundary = self.headers["Content-Type"].split("boundary=")[1].encode()
            for part in body.split(b"--" + boundary):
                if b'name="tmm"' in part:
                    raw = part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0].decode("utf-8-sig")
                    import re
                    if "," in raw.split("\n")[0]:
                        lib = {}
                        for row in csv.DictReader(io.StringIO(raw)):
                            iid = row.get("IMDb Id", row.get("imdb_id", ""))
                            if iid: lib[iid] = {"path": row.get("Path", "")}
                    else:
                        ids = re.findall(r'(tt\d{7,})', raw)
                        lib = {i: {} for i in set(ids)}
                    save_user_tmm(user, lib)
                    break
            self._redirect(f"{BASE}/u/{user}")
        elif self.path.startswith("/media/"):
            user = parts[-1]
            params = urllib.parse.parse_qs(body.decode())
            server_type = params.get("type", [""])[0]
            if server_type and server_type in MEDIA_SERVERS:
                config = load_user_media_config(user)
                config[server_type] = {
                    "enabled": True,
                    "url": params.get("url", [""])[0].rstrip("/"),
                    "token": params.get("token", [""])[0],
                }
                save_user_media_config(user, config)
            self._redirect(f"{BASE}/setup/{user}")
        elif self.path.startswith("/providers/"):
            user = parts[-1]
            params = urllib.parse.parse_qs(body.decode())
            selected = params.get("prov", [])
            all_provs = get_all_providers()
            config = {p["name"]: (p["name"] in selected) for p in all_provs}
            save_user_providers(user, config)
            self._redirect(f"{BASE}/setup/{user}")
            return
        elif self.path.startswith("/letterboxd/"):
            user = parts[-1]
            boundary = self.headers["Content-Type"].split("boundary=")[1].encode()
            for part in body.split(b"--" + boundary):
                if b'name="csv"' in part:
                    csv_data = part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0]
                    if not active_job()[1]:
                        text = csv_data.decode("utf-8-sig")
                        start_job("letterboxd", lambda jid: import_letterboxd(user, text))
                    break
            self._redirect(f"{BASE}/u/{user}")
            return
        elif self.path.startswith("/api/library/"):
            user = parts[-1]
            # Verify agent token
            token = self.headers.get("X-Agent-Token", "")
            if AGENT_TOKEN and token != AGENT_TOKEN:
                self.send_response(403)
                self.end_headers()
                self.wfile.write(b'{"error":"invalid token"}')
                return
            data = json.loads(body.decode())
            library = load_user_tmm(user)
            library.update(data.get("library", {}))
            save_user_tmm(user, library)
            self._json({"status": "ok", "count": len(library)})
            return
        elif self.path.startswith("/keys"):
            params = urllib.parse.parse_qs(body.decode())
            keys = {k: params.get(k, [""])[0] for k in ("tmdb", "omdb", "tvdb", "opensubs", "agent_token")}
            json.dump(keys, open(KEYS_FILE, "w"))
            TMDB_KEY, OMDB_KEY, TVDB_KEY = keys["tmdb"], keys["omdb"], keys["tvdb"]
            AGENT_TOKEN = keys.get("agent_token", "")
            self._redirect(f"{BASE}/")

    def _html(self, body):
        self.send_response(200); self.send_header("Content-Type", "text/html"); self.end_headers()
        self.wfile.write(body.encode())
    def _json(self, data):
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
    def _redirect(self, url):
        self.send_response(302); self.send_header("Location", url); self.end_headers()
    def log_message(self, *a): pass

# ── Main ──────────────────────────────────────────────────────────────
def _discover_highly_rated():
    """Discover highly-rated movies in EN/FR/PT/ES from TMDB and add to title store."""
    if not TMDB_KEY: return
    titles = load_titles()
    existing_tmdb = {t.get("tmdb_id") for t in titles.values() if t.get("tmdb_id")}
    added = 0
    for lang in ["en", "fr", "pt", "es"]:
        for vmin, vmax in [(500, 0), (100, 5000)]:  # mainstream + arthouse
            for page in range(1, 30):
                params = f"&vote_count.gte={vmin}&vote_average.gte=8.0&with_original_language={lang}&page={page}"
                if vmax: params += f"&vote_count.lte={vmax}"
                url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_KEY}&sort_by=vote_average.desc{params}"
                data = api_get(url)
                if not data or not data.get("results"): break
                for r in data["results"]:
                    tmdb_id = r["id"]
                    if tmdb_id in existing_tmdb: continue
                    ext = api_get(f"https://api.themoviedb.org/3/movie/{tmdb_id}/external_ids?api_key={TMDB_KEY}")
                    if ext and ext.get("imdb_id"):
                        iid = ext["imdb_id"]
                        if iid not in titles:
                            titles[iid] = {
                                "title": r.get("title", ""), "year": (r.get("release_date") or "")[:4],
                                "type": "movie", "tmdb_id": tmdb_id, "tmdb_rating": r.get("vote_average"),
                                "poster": f"https://image.tmdb.org/t/p/w185{r['poster_path']}" if r.get("poster_path") else "",
                                "overview": r.get("overview", ""), "language": lang,
                            }
                            existing_tmdb.add(tmdb_id)
                            added += 1
                    time.sleep(0.08)
                if page >= data.get("total_pages", 1): break
        if added % 50 == 0 and added > 0:
            save_titles(titles)
    save_titles(titles)
    print(f"Discovery: added {added} new titles. Total: {len(titles)}")

def _scheduler():
    """Background scheduler: enrichment daily 3am, catalog+discovery weekly Sunday 4am."""
    import datetime
    last_enrich = last_catalog = last_discover = None
    while True:
        now = datetime.datetime.now()
        # Daily enrichment at 3am
        if now.hour == 3 and last_enrich != now.date():
            print("Scheduled: enrichment")
            enrich_titles(fast=False)
            last_enrich = now.date()
        # Weekly on Sundays at 4am: catalog refresh + discovery + re-seed
        if now.weekday() == 6 and now.hour == 4 and last_catalog != now.date():
            print("Scheduled: catalog refresh")
            fetch_streaming_catalog()
            last_catalog = now.date()
        if now.weekday() == 6 and now.hour == 5 and last_discover != now.date():
            print("Scheduled: discovery sweep")
            _discover_highly_rated()
            last_discover = now.date()
        time.sleep(600)  # Check every 10 min

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(KEYS_FILE):
        keys = json.load(open(KEYS_FILE))
        TMDB_KEY = keys.get("tmdb", TMDB_KEY)
        OMDB_KEY = keys.get("omdb", OMDB_KEY)
        TVDB_KEY = keys.get("tvdb", TVDB_KEY)
        AGENT_TOKEN = keys.get("agent_token", AGENT_TOKEN)
    migrate_old_data()
    users = list_users()
    titles = load_titles()
    load_imdb_cache()
    print(f"CineCross — {len(titles)} titles, users: {users}")
    print(f"  TMDB:{'✓' if TMDB_KEY else '✗'} OMDB:{'✓' if OMDB_KEY else '✗'} TVDB:{'✓' if TVDB_KEY else '✗'} Trakt:{'✓' if TRAKT_ID else '✗'} Region:{WATCH_COUNTRY}")
    threading.Thread(target=_scheduler, daemon=True).start()
    HTTPServer(("0.0.0.0", PORT), H).serve_forever()
