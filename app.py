#!/usr/bin/env python3
"""IMDB Ratings + TMDB posters/streaming + OMDB scores + Trakt sync + TVDB."""
import csv, json, os, io, time, urllib.request, urllib.parse
from http.server import HTTPServer, BaseHTTPRequestHandler

IMDB_UR = os.environ.get("IMDB_UR", "")
TMDB_KEY = os.environ.get("TMDB_KEY", "")
OMDB_KEY = os.environ.get("OMDB_KEY", "")
TRAKT_ID = os.environ.get("TRAKT_ID", "")
TRAKT_SECRET = os.environ.get("TRAKT_SECRET", "")
TRAKT_REDIRECT = os.environ.get("TRAKT_REDIRECT", "https://your-domain.com/trakt/callback")
TVDB_KEY = os.environ.get("TVDB_KEY", "")
WATCH_COUNTRY = os.environ.get("WATCH_COUNTRY", "LU")
MY_PROVIDERS = {"Netflix", "Amazon Prime Video", "Disney Plus", "Max"}
LU_PROVIDER_IDS = {"Netflix": 8, "Amazon Prime Video": 119, "Disney Plus": 337}
TMM_FILE = "/data/tmm_library.json"
DATA_FILE = "/data/ratings.json"
TRAKT_TOKEN_FILE = "/data/trakt_token.json"
TVDB_TOKEN_FILE = "/data/tvdb_token.json"
PORT = 8000

PROVIDER_ICONS = {
    "Netflix": "🟥", "Amazon Prime Video": "📦", "Disney Plus": "🏰",
    "Max": "🟪", "Apple TV Plus": "🍎", "Crunchyroll": "🍊",
}
BASE = "/imdb"

# ── API helpers ───────────────────────────────────────────────────────
def api_get(url, headers=None):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"API error {url[:80]}: {e}")
        return None

def api_post(url, data, headers=None):
    req = urllib.request.Request(url, data=json.dumps(data).encode(),
        headers={"User-Agent": "Mozilla/5.0", **(headers or {}), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"API POST error {url[:80]}: {e}")
        return None

# ── TMDB ──────────────────────────────────────────────────────────────
def tmdb_enrich(imdb_id):
    if not TMDB_KEY:
        return {}
    data = api_get(f"https://api.themoviedb.org/3/find/{imdb_id}?api_key={TMDB_KEY}&external_source=imdb_id")
    if not data:
        return {}
    movies = data.get("movie_results") or []
    shows = data.get("tv_results") or []
    if not movies and not shows:
        return {}
    is_tv = len(shows) > 0
    r = shows[0] if is_tv else movies[0]
    tmdb_id = r["id"]
    kind = "tv" if is_tv else "movie"
    result = {
        "poster": f"https://image.tmdb.org/t/p/w185{r['poster_path']}" if r.get("poster_path") else "",
        "overview": r.get("overview", ""),
        "tmdb_rating": r.get("vote_average"),
        "tmdb_id": tmdb_id,
    }
    # Fetch streaming providers
    wp = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/watch/providers?api_key={TMDB_KEY}")
    if wp:
        lu = wp.get("results", {}).get(WATCH_COUNTRY, {})
        flatrate = lu.get("flatrate", [])
        providers = [p["provider_name"] for p in flatrate]
        result["providers"] = providers
        result["watch_link"] = lu.get("link", "")
    return result

# ── OMDB ──────────────────────────────────────────────────────────────
def omdb_enrich(imdb_id):
    if not OMDB_KEY:
        return {}
    data = api_get(f"https://www.omdbapi.com/?i={imdb_id}&apikey={OMDB_KEY}")
    if not data or data.get("Response") == "False":
        return {}
    rt = next((r["Value"] for r in data.get("Ratings", []) if "Rotten" in r.get("Source", "")), None)
    mc = data.get("Metascore")
    return {
        "rotten_tomatoes": rt,
        "metacritic": int(mc) if mc and mc != "N/A" else None,
        "plot": data.get("Plot", ""),
        "awards": data.get("Awards", ""),
        "poster": data.get("Poster") if data.get("Poster") != "N/A" else "",
    }

# ── TVDB ──────────────────────────────────────────────────────────────
def tvdb_login():
    if not TVDB_KEY:
        return None
    data = api_post("https://api4.thetvdb.com/v4/login", {"apikey": TVDB_KEY})
    if data and data.get("data", {}).get("token"):
        token = data["data"]["token"]
        os.makedirs(os.path.dirname(TVDB_TOKEN_FILE), exist_ok=True)
        json.dump({"token": token, "ts": time.time()}, open(TVDB_TOKEN_FILE, "w"))
        return token
    return None

def tvdb_token():
    if os.path.exists(TVDB_TOKEN_FILE):
        t = json.load(open(TVDB_TOKEN_FILE))
        if time.time() - t.get("ts", 0) < 86000:
            return t["token"]
    return tvdb_login()

def tvdb_enrich(imdb_id):
    token = tvdb_token()
    if not token:
        return {}
    data = api_get(f"https://api4.thetvdb.com/v4/search/remoteid/{imdb_id}",
                   {"Authorization": f"Bearer {token}"})
    if not data or not data.get("data"):
        return {}
    r = data["data"][0] if isinstance(data["data"], list) else data["data"]
    return {
        "tvdb_id": r.get("id"),
        "tvdb_name": r.get("name", ""),
        "tvdb_image": r.get("image", ""),
    }

# ── Trakt ─────────────────────────────────────────────────────────────
def trakt_headers():
    token = trakt_load_token()
    if not token:
        return None
    return {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": TRAKT_ID,
        "Authorization": f"Bearer {token['access_token']}",
    }

def trakt_load_token():
    if os.path.exists(TRAKT_TOKEN_FILE):
        return json.load(open(TRAKT_TOKEN_FILE))
    return None

def trakt_save_token(token):
    os.makedirs(os.path.dirname(TRAKT_TOKEN_FILE), exist_ok=True)
    json.dump(token, open(TRAKT_TOKEN_FILE, "w"))

def trakt_auth_url():
    return f"https://trakt.tv/oauth/authorize?response_type=code&client_id={TRAKT_ID}&redirect_uri={urllib.parse.quote(TRAKT_REDIRECT)}"

def trakt_exchange_code(code):
    return api_post("https://api.trakt.tv/oauth/token", {
        "code": code, "client_id": TRAKT_ID, "client_secret": TRAKT_SECRET,
        "redirect_uri": TRAKT_REDIRECT, "grant_type": "authorization_code",
    })

def trakt_fetch_ratings():
    h = trakt_headers()
    if not h:
        return None
    ratings = []
    for kind in ["movies", "shows"]:
        data = api_get(f"https://api.trakt.tv/users/me/ratings/{kind}", h)
        if not data:
            continue
        for item in data:
            obj = item.get("movie") or item.get("show") or {}
            ids = obj.get("ids", {})
            ratings.append({
                "id": ids.get("imdb", ""),
                "title": obj.get("title", ""),
                "year": obj.get("year"),
                "type": "movie" if "movie" in item else "show",
                "rating": item.get("rating", 0),
                "date": (item.get("rated_at") or "")[:10],
                "source": "trakt",
            })
    return ratings

def trakt_sync_ratings(ratings):
    h = trakt_headers()
    if not h:
        return False
    movies, shows = [], []
    for r in ratings:
        if not r.get("id"):
            continue
        entry = {"ids": {"imdb": r["id"]}, "rating": r["rating"]}
        if r.get("type") in ("movie", "Movie"):
            movies.append(entry)
        else:
            shows.append(entry)
    if movies:
        api_post("https://api.trakt.tv/sync/ratings", {"movies": movies}, h)
    if shows:
        api_post("https://api.trakt.tv/sync/ratings", {"shows": shows}, h)
    return True

# ── Data ──────────────────────────────────────────────────────────────
def save(ratings):
    os.makedirs(os.path.dirname(DATA_FILE), exist_ok=True)
    with open(DATA_FILE, "w") as f:
        json.dump({"updated": time.strftime("%Y-%m-%d %H:%M"), "count": len(ratings), "ratings": ratings}, f)

def load():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE) as f:
            return json.load(f)
    return None


# ── TMM ───────────────────────────────────────────────────────────────
def parse_tmm_csv(text):
    """Parse tinyMediaManager CSV export."""
    library = {}
    for row in csv.DictReader(io.StringIO(text)):
        imdb = row.get("IMDb Id", row.get("imdb_id", row.get("IMDB ID", "")))
        if not imdb:
            continue
        library[imdb] = {
            "path": row.get("Path", row.get("path", row.get("Video File", ""))),
            "quality": row.get("Video Resolution", row.get("resolution", "")),
            "size": row.get("File Size", row.get("size", "")),
        }
    return library

def parse_tmm_nfo_list(text):
    """Parse a simple list of IMDB IDs (one per line) or NFO content."""
    import re
    ids = re.findall(r'(tt\d{7,})', text)
    return {i: {"path": "", "quality": "", "size": ""} for i in set(ids)}

def load_tmm():
    if os.path.exists(TMM_FILE):
        return json.load(open(TMM_FILE))
    return {}

def save_tmm(library):
    os.makedirs(os.path.dirname(TMM_FILE), exist_ok=True)
    json.dump(library, open(TMM_FILE, "w"))

def tag_local(ratings):
    library = load_tmm()
    if not library:
        return ratings
    for r in ratings:
        if r.get("id") in library:
            r["local"] = True
            info = library[r["id"]]
            if info.get("quality"):
                r["quality"] = info["quality"]
            if info.get("path"):
                r["local_path"] = info["path"]
    return ratings

# ── Streaming catalog ─────────────────────────────────────────────────
def fetch_streaming_catalog():
    """Fetch full catalog of movies/shows on our providers in LU."""
    if not TMDB_KEY:
        return []
    catalog = []
    for kind in ("movie", "tv"):
        for pname, pid in LU_PROVIDER_IDS.items():
            page = 1
            while page <= 20:  # max 20 pages = 400 titles per provider
                url = f"https://api.themoviedb.org/3/discover/{kind}?api_key={TMDB_KEY}&watch_region={WATCH_COUNTRY}&with_watch_providers={pid}&with_watch_monetization_types=flatrate&sort_by=vote_average.desc&vote_count.gte=100&page={page}"
                data = api_get(url)
                if not data or not data.get("results"):
                    break
                for r in data["results"]:
                    catalog.append({
                        "tmdb_id": r["id"],
                        "title": r.get("title") or r.get("name", ""),
                        "year": (r.get("release_date") or r.get("first_air_date") or "")[:4],
                        "type": kind,
                        "tmdb_rating": r.get("vote_average"),
                        "poster": f"https://image.tmdb.org/t/p/w185{r['poster_path']}" if r.get("poster_path") else "",
                        "overview": r.get("overview", "")[:200],
                        "provider": pname,
                    })
                if page >= data.get("total_pages", 1):
                    break
                page += 1
                time.sleep(0.15)
            print(f"  {pname} {kind}: {len([c for c in catalog if c['provider']==pname and c['type']==kind])}")
    # Deduplicate by tmdb_id, merge providers
    merged = {}
    for c in catalog:
        key = c["tmdb_id"]
        if key in merged:
            if c["provider"] not in merged[key]["providers"]:
                merged[key]["providers"].append(c["provider"])
        else:
            c["providers"] = [c["provider"]]
            del c["provider"]
            merged[key] = c
    result = sorted(merged.values(), key=lambda x: x.get("tmdb_rating", 0), reverse=True)
    os.makedirs("/data", exist_ok=True)
    json.dump({"updated": time.strftime("%Y-%m-%d %H:%M"), "count": len(result), "catalog": result}, open("/data/catalog.json", "w"))
    print(f"Catalog: {len(result)} unique titles")
    return result

def load_catalog():
    if os.path.exists("/data/catalog.json"):
        return json.load(open("/data/catalog.json"))
    return None

def render_catalog(data):
    if not data:
        return f'<html><body style="background:#1a1a2e;color:#eee;font-family:sans-serif;padding:40px"><h2>No catalog yet</h2><a href="{BASE}/catalog/fetch" style="color:#4fc3f7">Fetch streaming catalog for {WATCH_COUNTRY}</a></body></html>'
    # Load rated IDs to mark what we've seen
    rated = load()
    rated_ids = set()
    if rated:
        rated_ids = {r.get("tmdb_id") for r in rated["ratings"] if r.get("tmdb_id")}
        rated_imdb = {r.get("id") for r in rated["ratings"]}
    rows = ""
    for r in data["catalog"]:
        seen = "✓" if r["tmdb_id"] in rated_ids else ""
        provs = " ".join(PROVIDER_ICONS.get(p, "▪") for p in r.get("providers", []))
        poster = f'<img src=\"{r["poster"]}\" height=\"60\" loading=\"lazy\">' if r.get("poster") else ""
        tmdb = f'{r["tmdb_rating"]}' if r.get("tmdb_rating") else ""
        rows += f'<tr><td>{poster}</td><td>{r["title"]}</td><td>{r.get("year","")}</td><td>{tmdb}</td><td>{provs}</td><td>{r["type"]}</td><td style="color:#2d7">{seen}</td></tr>'
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>Streaming Catalog {WATCH_COUNTRY} ({data["count"]})</title>
<style>
body{{font-family:-apple-system,sans-serif;margin:20px;background:#1a1a2e;color:#eee}}
table{{border-collapse:collapse;width:100%}}th,td{{padding:6px 10px;text-align:left;border-bottom:1px solid #333}}
th{{background:#16213e;position:sticky;top:0}}tr:hover{{background:#16213e}}img{{border-radius:4px}}
input{{padding:6px;border-radius:4px;border:1px solid #444;background:#16213e;color:#eee;width:250px}}
a{{color:#4fc3f7;text-decoration:none}}
</style>
<script>function f(){{const q=document.getElementById("s").value.toLowerCase();document.querySelectorAll("tbody tr").forEach(r=>r.style.display=r.textContent.toLowerCase().includes(q)?"":"none")}}</script>
</head><body>
<h2>📺 Streaming Catalog — {WATCH_COUNTRY} — {data["count"]} titles</h2>
<div style="margin-bottom:15px;display:flex;gap:12px;align-items:center">
<input id="s" onkeyup="f()" placeholder="Search...">
<a href="{BASE}/catalog/fetch">↻ Refresh</a> <a href="{BASE}/">← Ratings</a>
<span style="color:#666;font-size:.85em">Updated: {data["updated"]}</span></div>
<table><thead><tr><th></th><th>Title</th><th>Year</th><th>TMDB</th><th>On</th><th>Type</th><th>Seen</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""

def enrich_all(ratings):
    count = 0
    for r in ratings:
        if not r.get("id") or r.get("_enriched"):
            continue
        if TMDB_KEY:
            t = tmdb_enrich(r["id"])
            for k in ("poster", "overview", "tmdb_rating", "providers", "watch_link"):
                if t.get(k):
                    r[k] = t[k]
        if OMDB_KEY:
            o = omdb_enrich(r["id"])
            for k in ("rotten_tomatoes", "metacritic", "plot", "awards"):
                if o.get(k):
                    r[k] = o[k]
            if o.get("poster") and not r.get("poster"):
                r["poster"] = o["poster"]
        if TVDB_KEY:
            tv = tvdb_enrich(r["id"])
            for k in ("tvdb_id", "tvdb_image"):
                if tv.get(k):
                    r[k] = tv[k]
        r["_enriched"] = True
        count += 1
        if count % 20 == 0:
            print(f"  Enriched {count}...")
            time.sleep(0.1)
    print(f"Enriched {count} titles")
    ratings = tag_local(ratings)
    return ratings

def parse_csv(text):
    ratings = []
    for row in csv.DictReader(io.StringIO(text)):
        try:
            imdb_r = float(row["IMDb Rating"]) if row.get("IMDb Rating") and row["IMDb Rating"] != "null" else None
            votes = int(row["Num Votes"]) if row.get("Num Votes") and row["Num Votes"] != "null" else 0
        except:
            imdb_r, votes = None, 0
        ratings.append({
            "id": row.get("Const", ""), "title": row.get("Title", ""),
            "year": row.get("Year", ""), "type": row.get("Title Type", ""),
            "rating": int(row.get("Your Rating", 0)),
            "imdb_rating": imdb_r, "votes": votes,
            "date": row.get("Date Rated", ""), "genres": row.get("Genres", ""),
            "directors": row.get("Directors", ""), "poster": "",
        })
    ratings.sort(key=lambda r: r["date"], reverse=True)
    return ratings

# ── HTML ──────────────────────────────────────────────────────────────
def render(data):
    if not data:
        return setup_page()

    genres = sorted(set(g.strip() for r in data["ratings"] for g in r.get("genres", "").split(",") if g.strip()))
    genre_opts = "".join(f'<option value="{g}">{g}</option>' for g in genres)

    has_trakt = os.path.exists(TRAKT_TOKEN_FILE)
    services = []
    if TMDB_KEY: services.append("TMDB ✓")
    if OMDB_KEY: services.append("OMDB ✓")
    if TVDB_KEY: services.append("TVDB ✓")
    trakt_str = '<span style="color:#2d7">Trakt ✓</span>' if has_trakt else f'<a href="{BASE}/trakt/auth">Trakt ✗</a>' if TRAKT_ID else ""

    rows = ""
    for r in data["ratings"]:
        c = "#2d7" if r["rating"] >= 7 else "#d72" if r["rating"] <= 4 else "#aaa"
        imdb = f'{r["imdb_rating"]}' if r.get("imdb_rating") else "—"
        poster = f'<img src="{r["poster"]}" height="70" loading="lazy">' if r.get("poster") else ""
        # Scores
        scores = []
        if r.get("rotten_tomatoes"): scores.append(f'🍅{r["rotten_tomatoes"]}')
        if r.get("metacritic"): scores.append(f'Ⓜ{r["metacritic"]}')
        if r.get("tmdb_rating"): scores.append(f'T{r["tmdb_rating"]}')
        # Streaming
        provs = r.get("providers", [])
        mine = [p for p in provs if p in MY_PROVIDERS]
        others = [p for p in provs if p not in MY_PROVIDERS]
        stream = ""
        if mine:
            icons = " ".join(PROVIDER_ICONS.get(p, "▪") for p in mine)
            names = ", ".join(mine)
            link = r.get("watch_link", "")
            stream = f'<a href="{link}" target="_blank" title="{names}">{icons}</a>' if link else f'<span title="{names}">{icons}</span>'
        if others:
            stream += f' <span style="color:#666" title="{", ".join(others)}">+{len(others)}</span>'
        tooltip = f' title="{r.get("overview","")[:200]}"' if r.get("overview") else ""
        local = '<span title="Local" style="color:#2d7">💾</span>' if r.get("local") else ""
        rows += f'<tr data-g="{r.get("genres","")}" data-r="{r["rating"]}" data-s="{" ".join(provs)}"><td>{poster}</td><td><a href="https://www.imdb.com/title/{r["id"]}/" target="_blank"{tooltip}>{r["title"]}</a></td><td>{r.get("year","")}</td><td style="font-weight:bold;color:{c}">{r["rating"]}</td><td>{imdb}</td><td class="x">{" ".join(scores)}</td><td>{stream}</td><td class="x">{r.get("genres","")}</td><td class="x">{r.get("date","")}</td></tr>'

    return f"""<!DOCTYPE html><html><head><meta charset="utf-8">
<title>My Ratings ({data['count']})</title>
<style>
body{{font-family:-apple-system,sans-serif;margin:20px;background:#1a1a2e;color:#eee}}
table{{border-collapse:collapse;width:100%}}th,td{{padding:6px 10px;text-align:left;border-bottom:1px solid #333}}
th{{background:#16213e;position:sticky;top:0;cursor:pointer;white-space:nowrap}}th:hover{{background:#1a3a5e}}
tr:hover{{background:#16213e}}a{{color:#4fc3f7;text-decoration:none}}img{{border-radius:4px}}
.bar{{display:flex;gap:10px;align-items:center;margin-bottom:15px;flex-wrap:wrap}}
.x{{font-size:.8em;color:#aaa}}
input,select{{padding:6px;border-radius:4px;border:1px solid #444;background:#16213e;color:#eee}}
.tag{{display:inline-block;padding:2px 6px;border-radius:3px;font-size:.75em;margin:1px}}
</style>
<script>
function f(){{const q=document.getElementById('s').value.toLowerCase(),g=document.getElementById('g').value,mr=document.getElementById('mr').value,st=document.getElementById('st').value;
document.querySelectorAll('tbody tr').forEach(r=>r.style.display=(r.textContent.toLowerCase().includes(q)&&(!g||r.dataset.g.includes(g))&&(!mr||parseInt(r.dataset.r)>=parseInt(mr))&&(!st||r.dataset.s.includes(st)))?'':'none')}}
function sortTable(n){{const tb=document.querySelector('tbody'),rows=[...tb.rows],dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:'';
rows.sort((a,b)=>{{let x=a.cells[n].textContent,y=b.cells[n].textContent;return(!isNaN(x)&&!isNaN(y)?(x-y):x.localeCompare(y))*dir}});rows.forEach(r=>tb.appendChild(r))}}
</script></head><body>
<h2>🎬 My Ratings — {data['count']} titles</h2>
<div class="bar"><input id="s" onkeyup="f()" placeholder="Search..." style="width:220px">
<select id="g" onchange="f()"><option value="">All genres</option>{genre_opts}</select>
<select id="mr" onchange="f()"><option value="">Min ★</option>{''.join(f'<option value="{i}">{i}+</option>' for i in range(10,0,-1))}</select>
<select id="st" onchange="f()"><option value="">All streams</option><option value="Netflix">🟥 Netflix</option><option value="Amazon Prime">📦 Prime</option><option value="Disney">🏰 Disney+</option><option value="Max">🟪 Max</option></select>
<a href="{BASE}/enrich">⚡ Enrich</a> <a href="{BASE}/catalog">📺 Catalog</a> <a href="{BASE}/setup">⚙</a>
{f'<a href="{BASE}/trakt/sync">↕ Trakt</a>' if has_trakt else ""}
<span style="color:#666;font-size:.8em">{" | ".join(services)} {trakt_str} · {data['updated']}</span></div>
<table><thead><tr><th></th><th onclick="sortTable(1)">Title</th><th onclick="sortTable(2)">Year</th><th onclick="sortTable(3)">★</th><th onclick="sortTable(4)">IMDB</th><th>Scores</th><th>Stream</th><th onclick="sortTable(7)">Genres</th><th onclick="sortTable(8)">Rated</th><th>💾</th></tr></thead>
<tbody>{rows}</tbody></table></body></html>"""

def setup_page():
    has_trakt = os.path.exists(TRAKT_TOKEN_FILE)
    has_tvdb = bool(TVDB_KEY)
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Ratings Setup</title>
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;display:flex;justify-content:center;padding-top:30px}}
.box{{background:#16213e;padding:30px;border-radius:12px;max-width:600px;width:100%}}
a{{color:#4fc3f7}}input,textarea{{width:100%;padding:8px;border-radius:4px;border:1px solid #444;background:#1a1a2e;color:#eee;margin:8px 0;box-sizing:border-box}}
button{{padding:10px 30px;background:#4fc3f7;border:none;border-radius:6px;cursor:pointer;font-size:1em;margin-top:10px}}
hr{{border-color:#333;margin:20px 0}}</style></head>
<body><div class="box"><h2>🎬 Ratings Setup</h2>
<h3>Upload IMDB CSV</h3>
<p><a href="https://www.imdb.com/list/ratings/export" target="_blank">Export from IMDB</a> and upload:</p>
<form method="POST" action="{BASE}/upload" enctype="multipart/form-data">
<input type="file" name="csv" accept=".csv"><button type="submit">Upload CSV</button></form>
<hr>
<h3>API Keys</h3>
<form method="POST" action="{BASE}/keys">
<label>TMDB (<a href="https://www.themoviedb.org/settings/api" target="_blank">get key</a>)</label>
<input name="tmdb" value="{TMDB_KEY}">
<label>OMDB (<a href="https://www.omdbapi.com/apikey.aspx" target="_blank">get key</a>)</label>
<input name="omdb" value="{OMDB_KEY}">
<label>TVDB (<a href="https://thetvdb.com/dashboard/account/apikey" target="_blank">get key</a>)</label>
<input name="tvdb" value="{TVDB_KEY}">
<button type="submit">Save Keys</button></form>
<hr>
<h3>Trakt</h3>
{'<span style="color:#2d7">✓ Connected</span> <a href="'+BASE+'/trakt/auth">(reconnect)</a>' if has_trakt else f'<a href="{BASE}/trakt/auth"><button>Connect Trakt</button></a>' if TRAKT_ID else '<p style="color:#888">Set TRAKT_ID env to enable</p>'}
<hr>
<h3>Local Library (TMM)</h3>
<p>Upload a tinyMediaManager CSV export, or a text file with IMDB IDs:</p>
<form method="POST" action="{BASE}/tmm" enctype="multipart/form-data">
<input type="file" name="tmm" accept=".csv,.txt,.nfo"><button type="submit">Upload</button></form>
<hr>
<h3>Streaming Catalog</h3>
<p><a href="{BASE}/catalog">Browse full streaming catalog for {WATCH_COUNTRY}</a></p>
<hr>
<h3>Streaming Region</h3>
<p>Currently: <b>{WATCH_COUNTRY}</b> — filtering for: {", ".join(MY_PROVIDERS)}</p>
<p style="color:#888">Change via WATCH_COUNTRY env var</p>
</div></body></html>"""

# ── Server ────────────────────────────────────────────────────────────
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        p = self.path.split("?")[0]
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        if p == "/trakt/auth":
            self.send_response(302); self.send_header("Location", trakt_auth_url()); self.end_headers()
        elif p == "/trakt/callback":
            code = qs.get("code", [None])[0]
            if code:
                token = trakt_exchange_code(code)
                if token and "access_token" in token:
                    trakt_save_token(token)
            self.send_response(302); self.send_header("Location", f"{BASE}/"); self.end_headers()
        elif p == "/trakt/sync":
            data = load()
            if data:
                trakt_sync_ratings(data["ratings"])
                tr = trakt_fetch_ratings()
                if tr:
                    existing = {r["id"] for r in data["ratings"]}
                    for t in tr:
                        if t["id"] and t["id"] not in existing:
                            data["ratings"].append(t)
                    data["count"] = len(data["ratings"])
                    data["updated"] = time.strftime("%Y-%m-%d %H:%M")
                    save(data["ratings"])
            self.send_response(302); self.send_header("Location", f"{BASE}/"); self.end_headers()
        elif p == "/enrich":
            data = load()
            if data:
                data["ratings"] = enrich_all(data["ratings"])
                data["updated"] = time.strftime("%Y-%m-%d %H:%M")
                save(data["ratings"])
            self.send_response(302); self.send_header("Location", f"{BASE}/"); self.end_headers()
        elif p == "/catalog":
            self._html(render_catalog(load_catalog()))
        elif p == "/catalog/fetch":
            fetch_streaming_catalog()
            self.send_response(302); self.send_header("Location", f"{BASE}/catalog"); self.end_headers()
            return
        elif p == "/setup":
            self._html(setup_page())
        elif p == "/api":
            self.send_response(200); self.send_header("Content-Type", "application/json"); self.end_headers()
            self.wfile.write(json.dumps(load() or {}).encode())
        else:
            self._html(render(load()))

    def do_POST(self):
        cl = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(cl)
        if self.path.startswith("/upload"):
            boundary = self.headers["Content-Type"].split("boundary=")[1].encode()
            for part in body.split(b"--" + boundary):
                if b'name="csv"' in part:
                    csv_data = part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0]
                    save(parse_csv(csv_data.decode("utf-8-sig")))
                    break
            self.send_response(302); self.send_header("Location", f"{BASE}/"); self.end_headers()
        elif self.path.startswith("/tmm"):
            # Accept CSV, NFO list, or plain IMDB ID list
            boundary = self.headers.get("Content-Type", "").split("boundary=")
            if len(boundary) > 1:
                for part in body.split(boundary[1].encode()):
                    if b'name="tmm"' in part:
                        raw = part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0].decode("utf-8-sig")
                        if "," in raw.split("\n")[0]:
                            lib = parse_tmm_csv(raw)
                        else:
                            lib = parse_tmm_nfo_list(raw)
                        save_tmm(lib)
                        # Re-tag ratings
                        data = load()
                        if data:
                            data["ratings"] = tag_local(data["ratings"])
                            save(data["ratings"])
                        break
            self.send_response(302); self.send_header("Location", f"{BASE}/"); self.end_headers()
            return
        elif self.path.startswith("/keys"):
            params = urllib.parse.parse_qs(body.decode())
            keys = {k: params.get(k, [""])[0] for k in ("tmdb", "omdb", "tvdb")}
            os.makedirs("/data", exist_ok=True)
            json.dump(keys, open("/data/api_keys.json", "w"))
            global TMDB_KEY, OMDB_KEY, TVDB_KEY
            TMDB_KEY, OMDB_KEY, TVDB_KEY = keys["tmdb"], keys["omdb"], keys["tvdb"]
            self.send_response(302); self.send_header("Location", f"{BASE}/"); self.end_headers()

    def _html(self, body):
        self.send_response(200); self.send_header("Content-Type", "text/html"); self.end_headers()
        self.wfile.write(body.encode())

    def log_message(self, *a): pass

if __name__ == "__main__":
    if os.path.exists("/data/api_keys.json"):
        keys = json.load(open("/data/api_keys.json"))
        TMDB_KEY = keys.get("tmdb", TMDB_KEY)
        OMDB_KEY = keys.get("omdb", OMDB_KEY)
        TVDB_KEY = keys.get("tvdb", TVDB_KEY)
    print(f"Ratings — TMDB:{'✓' if TMDB_KEY else '✗'} OMDB:{'✓' if OMDB_KEY else '✗'} TVDB:{'✓' if TVDB_KEY else '✗'} Trakt:{'✓' if TRAKT_ID else '✗'} Region:{WATCH_COUNTRY}")
    HTTPServer(("0.0.0.0", PORT), H).serve_forever()
