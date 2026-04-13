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
from socketserver import ThreadingMixIn


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

import threading as _threading
_file_locks = {}
_lock_lock = _threading.Lock()

def _get_lock(path):
    with _lock_lock:
        if path not in _file_locks:
            _file_locks[path] = _threading.Lock()
        return _file_locks[path]

def safe_json_load(path):
    lock = _get_lock(path)
    with lock:
        if os.path.exists(path):
            return json.load(open(path))
    return None

def safe_json_save(path, data):
    lock = _get_lock(path)
    with lock:
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else '.', exist_ok=True)
        with open(path + '.tmp', 'w') as f:
            json.dump(data, f)
        os.replace(path + '.tmp', path)

PORT = 8000
AGENT_TOKEN = os.environ.get("AGENT_TOKEN", "")
BASE = "/cinecross"

# ── Shared UI ─────────────────────────────────────────────────────────
APP_BANNER = '<div style="background:var(--card);padding:6px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px"><span style="font-size:1.2em">🎬</span><b style="font-size:1.1em;letter-spacing:.5px">Cinephile Crossroads</b></div>'

def nav_bar(active="ratings", user=""):
    u = user or (list_users() or ["default"])[0]
    sections = [("ratings", "Ratings", f"{BASE}/u/{u}"), ("discover", "Discover", f"{BASE}/recs/{u}"),
                ("library", "Library", f"{BASE}/library/{u}"), ("social", "Social", f"{BASE}/feed"),
                ("setup", "Setup", f"{BASE}/setup/{u}")]
    links = ""
    for key, label, href in sections:
        cls = "nav-active" if key == active else ""
        links += f'<a href="{href}" class="nav-link {cls}">{label}</a>'
    return APP_BANNER + f'<nav class="top-nav"><div class="nav-links">{links}</div>{render_user_bar(u)}</nav>'

def sub_nav(items, active=""):
    links = ""
    for key, label, href in items:
        cls = "sub-active" if key == active else ""
        links += f'<a href="{href}" class="sub-link {cls}">{label}</a>'
    return f'<div class="sub-nav">{links}</div>'

SHARED_CSS = """
:root{--bg:#1a1a2e;--fg:#eee;--card:#16213e;--border:#333;--accent:#4fc3f7;--accent2:#2d7;--warn:#d72;--muted:#888}
.light{--bg:#f5f5f5;--fg:#222;--card:#fff;--border:#ddd;--accent:#0077cc}
body{font-family:-apple-system,sans-serif;background:var(--bg);color:var(--fg);margin:0}
.top-nav{background:var(--card);padding:8px 20px;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid var(--border);position:sticky;top:0;z-index:100}
.nav-links{display:flex;gap:4px}.nav-link{padding:8px 14px;border-radius:6px;color:var(--muted);text-decoration:none;font-size:.9em}
.nav-link:hover{background:var(--bg);color:var(--fg)}.nav-active{background:var(--accent);color:#1a1a2e!important;font-weight:600}
.sub-nav{padding:8px 20px;background:var(--card);border-bottom:1px solid var(--border);display:flex;gap:4px}
.sub-link{padding:5px 12px;border-radius:4px;color:var(--muted);text-decoration:none;font-size:.85em}
.sub-active{color:var(--accent)!important;border-bottom:2px solid var(--accent)}
.page{padding:20px;max-width:1400px;margin:0 auto}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:15px;margin-bottom:20px}
.card{background:var(--card);padding:15px;border-radius:10px}
table{border-collapse:collapse;width:100%}th,td{padding:6px 10px;text-align:left;border-bottom:1px solid var(--border)}
th{background:var(--card);cursor:pointer;position:sticky;top:88px;cursor:pointer}tr:hover{background:var(--card)}
a{color:var(--accent);text-decoration:none}img{border-radius:4px}
.btn{display:inline-block;padding:6px 14px;border-radius:6px;background:var(--card);border:1px solid var(--border);color:var(--fg);text-decoration:none;font-size:.85em;margin:2px}
.btn:hover{border-color:var(--accent);color:var(--accent)}.btn-primary{background:var(--accent);color:#1a1a2e;border-color:var(--accent)}
.x{font-size:.8em;color:var(--muted)}
@media(max-width:768px){.top-nav{flex-direction:column;gap:8px;padding:8px}.page{padding:10px}table{font-size:.8em}th,td{padding:4px 6px}img{height:50px!important}.x{display:none}}
"""

SHARED_JS = ('<script>'
    'if(localStorage.getItem("theme")==="light")document.body.classList.add("light");'
    'function sortTable(n){var tb=document.querySelector("tbody");if(!tb)return;var rows=[].slice.call(tb.rows),dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:"";rows.sort(function(a,b){var x=a.cells[n].dataset.sort||a.cells[n].textContent,y=b.cells[n].dataset.sort||b.cells[n].textContent;x=isNaN(x)?x:Number(x);y=isNaN(y)?y:Number(y);return(typeof x==="number"&&typeof y==="number"?(x-y):(String(x)).localeCompare(String(y),undefined,{numeric:true}))*dir});rows.forEach(function(r){tb.appendChild(r)})}'
    'function filterTable(){var q=(document.getElementById("s")||{}).value;q=q?q.toLowerCase():"";var rows=document.querySelectorAll("tbody tr");rows.forEach(function(r){r.style.display=r.textContent.toLowerCase().indexOf(q)>=0?"":"none"})}'
    '</script>')

def page_head(title, extra_css=""):
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{title}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{SHARED_CSS}{extra_css}</style>
</head><body>"""

def page_foot():
    return "</body></html>"

def wrap_page(html, section="ratings", user=""):
    if "top-nav" in html: return html
    nav = nav_bar(section, user)
    if "<body>" in html:
        html = html.replace("<body>", "<body>" + nav + '<div class="page">', 1)
        html = html.replace("</body>", "</div></body>", 1)
    return html

PROVIDER_ICONS = {"Netflix": "🟥", "Amazon Prime Video": "📦", "Disney Plus": "🏰", "Max": "🟪", "Apple TV Plus": "🍎"}
LU_PROVIDER_IDS = {"Netflix": 8, "Amazon Prime Video": 119, "Disney Plus": 337}  # TMDB provider IDs per country


# ── Server Task Queue (for agent) ─────────────────────────────────────
TASK_QUEUE_FILE = f"{DATA_DIR}/task_queue.json"
AGENT_STATUS_FILE = f"{DATA_DIR}/agent_status.json"

def load_task_queue():
    return safe_json_load(TASK_QUEUE_FILE) or []

def save_task_queue(queue):
    safe_json_save(TASK_QUEUE_FILE, queue)


# ── Auto-task generation ──────────────────────────────────────────────
def save_agent_status(status):
    status["last_seen"] = time.strftime("%Y-%m-%d %H:%M:%S")
    json.dump(status, open(AGENT_STATUS_FILE, "w"))

def load_agent_status():
    if os.path.exists(AGENT_STATUS_FILE): return json.load(open(AGENT_STATUS_FILE))
    return {}

def generate_tasks_for_library(user):
    """Analyze library and enqueue tasks for the agent. Priority: 0=dupes, 1=quality, 2=subs."""
    library = load_user_tmm(user)
    if not library: return 0
    
    # Load queue, preserve priority/exec_code tasks and all done tasks
    queue = load_task_queue()
    keep = [t for t in queue if t["status"] != "pending" or t.get("priority") == -1 or t["type"] in ("exec_code", "update_agent")]
    dropped = len(queue) - len(keep)
    preserved = [t for t in keep if t["status"] == "pending"]
    if preserved: print("  Preserved " + str(len(preserved)) + " priority tasks: " + ", ".join(t["id"] for t in preserved))
    if dropped: print("  Cleared " + str(dropped) + " old auto-generated tasks")
    
    new_tasks = []
    def _add(task_type, params, priority):
        new_tasks.append({"id": f"task_{int(time.time()*1000)}_{len(new_tasks)}", "type": task_type,
            "params": params or {}, "priority": priority, "status": "pending",
            "created": time.strftime("%Y-%m-%d %H:%M:%S")})
    
    count = 0
    from collections import defaultdict
    
    # Collect all needs
    needs_size = [(iid, library[iid]["path"]) for iid in library
                  if isinstance(library.get(iid), dict) and library[iid].get("path")
                  and not library[iid].get("file_size") and not library[iid].get("size")]
    # Hash ALL files (needed for OpenSubtitles matching)
    needs_hash = [(iid, info["path"]) for iid, info in library.items()
                  if isinstance(info, dict) and info.get("path")
                  and info.get("file_size") and not info.get("file_hash")]
    needs_subs = [(iid, info["path"]) for iid, info in library.items()
                  if isinstance(info, dict) and info.get("path")
                  and not info.get("subtitles") and not info.get("suggested_sub")][:50]
    needs_quality = [info["path"] for iid, info in library.items()
                     if isinstance(info, dict) and info.get("path")
                     and not info.get("video_codec") and not info.get("quality")]

    # Batch sizing at 50 per task
    for i in range(0, len(needs_size), 50):
        batch = needs_size[i:i+50]
        _add("size_files", {"paths": [p for _,p in batch], "imdb_ids": [i for i,_ in batch]}, PRIORITY_QUALITY)
        count += 1
    # Hash dupes
    for i in range(0, len(needs_hash), 50):
        batch = needs_hash[i:i+50]
        _add("hash_files", {"paths": [p for _,p in batch]}, PRIORITY_QUALITY)
        count += 1
    # Subs - interleave with other tasks
    for iid, path in needs_subs:
        _add("download_subs", {"imdb_id": iid, "path": path, "language": "en"}, PRIORITY_SUBS)
        count += 1
    # Quality
    for i in range(0, len(needs_quality), 50):
        _add("check_quality", {"paths": needs_quality[i:i+50]}, PRIORITY_QUALITY)
        count += 1

    # Save all at once: preserved + new + done
    new_tasks.sort(key=lambda t: t["priority"])
    save_task_queue(keep + new_tasks)
    count = len(new_tasks)
    print(f"Generated {count} tasks for {user}: {len(needs_size)} sizing, {len(needs_hash)} hashing, {len(needs_subs)} subs, {len(needs_quality)} quality")
    return count

PRIORITY_HUMAN = -1   # User clicked something — do it NOW
PRIORITY_DUPES = 0    # Duplicate detection
PRIORITY_QUALITY = 1  # Sizing, quality checks
PRIORITY_SUBS = 2     # Subtitle search (API rate limited)


def enqueue_human_task(task_type, params=None):
    """Enqueue a user-triggered task at highest priority."""
    return enqueue_task(task_type, params, priority=PRIORITY_HUMAN)

def request_file_hash(user, imdb_id):
    """User wants hash for a specific file (e.g. clicked 'find subs')."""
    library = load_user_tmm(user)
    info = library.get(imdb_id, {})
    if info.get("path"):
        enqueue_human_task("hash_files", {"paths": [info["path"]]})

def request_subs(user, imdb_id, language="en"):
    """User clicked 'find subtitles' for a specific title."""
    library = load_user_tmm(user)
    info = library.get(imdb_id, {})
    if info.get("path"):
        enqueue_human_task("download_subs", {
            "imdb_id": imdb_id, "path": info["path"], "language": language
        })

def request_quality_check(user, imdb_ids):
    """User wants quality info for specific titles."""
    library = load_user_tmm(user)
    paths = [library[iid]["path"] for iid in imdb_ids if iid in library and library[iid].get("path")]
    if paths:
        enqueue_human_task("check_quality", {"paths": paths})

def enqueue_task(task_type, params=None, priority=0):
    """Add a task for the agent. Priority: 0=high, 1=medium, 2=low.
    Types: find_duplicates, check_quality, download_subs, scrape_movie, rename_file"""
    queue = load_task_queue()
    queue.append({
        "id": f"task_{int(time.time()*1000)}",
        "type": task_type,
        "params": params or {},
        "priority": priority,
        "status": "pending",
        "created": time.strftime("%Y-%m-%d %H:%M:%S"),
    })
    queue.sort(key=lambda t: t["priority"])
    save_task_queue(queue)

def get_pending_tasks(limit=5):
    """Get next pending tasks for the agent, highest priority first.
    Returns small batches so agent stays responsive to human actions."""
    queue = load_task_queue()
    pending = sorted([t for t in queue if t["status"] == "pending"], key=lambda t: t["priority"])
    return pending[:limit]

_exec_results = {}  # Persistent store for exec_code results

def complete_task(task_id, result=None):
    """Mark a task as complete and feed results back into library."""
    queue = load_task_queue()
    task = None
    for t in queue:
        if t["id"] == task_id:
            t["status"] = "done"
            t["result"] = result
            t["completed"] = time.strftime("%Y-%m-%d %H:%M:%S")
            task = t
    # Handle NFO batch results (no matching task)
    if not task and task_id.startswith("nfo_batch_") and result:
        fake_task = {"id": task_id, "type": "exec_code", "params": {}}
        _apply_task_result(fake_task, result)
    # Feed results back into library data
    if task and result and not result.get("error"):
        _apply_task_result(task, result)
    # Store exec_code results persistently
    if result and (task_id.startswith("inspect_") or task_id.startswith("nfo_") or (task and task.get("type") == "exec_code")):
        _exec_results[task_id] = {"result": result, "completed": time.strftime("%Y-%m-%d %H:%M:%S")}
    # Keep only last 100 completed
    done = [t for t in queue if t["status"] == "done"]
    pending = [t for t in queue if t["status"] != "done"]
    save_task_queue(pending + done[-100:])

def _apply_task_result(task, result):
    """Update library with task results."""
    ttype = task["type"]
    params = task.get("params", {})
    data = result.get("data", {})
    if not data: return
    print(f"[tasks] Applying {ttype}: {len(data)} items")
    # Find which user this is for (check all users)
    for user in list_users():
        library = load_user_tmm(user)
        updated = False
        if ttype == "size_files":
            imdb_ids = params.get("imdb_ids", [])
            paths = params.get("paths", [])
            for i, path in enumerate(paths):
                if path in data:
                    iid = imdb_ids[i] if i < len(imdb_ids) else None
                    if iid and iid in library:
                        library[iid]["file_size"] = data[path]
                        updated = True
        elif ttype == "hash_files":
            for path, info in data.items():
                for iid, lib_info in library.items():
                    if lib_info.get("path") == path:
                        lib_info["file_hash"] = info.get("hash")
                        lib_info["file_size"] = info.get("size")
                        updated = True
        elif ttype == "check_quality":
            for path, info in data.items():
                for iid, lib_info in library.items():
                    if lib_info.get("path") == path:
                        lib_info.update({k: v for k, v in info.items() if v})
                        updated = True
        elif task_id.startswith("thumb_"):
            # Thumbnail results: {nfs_path: base64_jpg}
            PATH_MAP = {"//zeus/Movies": "nfs://192.168.0.235/volume1/Movies",
                        "//zeus/TVShows": "nfs://192.168.0.235/volume1/TVShows",
                        "//zeus/V_HD": "nfs://192.168.0.235/volume1/V_HD"}
            for path, b64 in data.items():
                nfs_path = path.replace("\\", "/")
                for smb, nfs in PATH_MAP.items():
                    if nfs_path.startswith(smb):
                        nfs_path = nfs + nfs_path[len(smb):]
                        break
                for lib_iid, lib_info in library.items():
                    if isinstance(lib_info, dict) and nfs_path in lib_info.get("path", ""):
                        lib_info["thumbnail"] = b64[:50000]  # cap at 50KB
                        updated = True
                        break
        elif ttype == "exec_code" and (task.get("id", "").startswith("nfo_") or task_id.startswith("nfo_batch_")):
            # NFO scan results: {dir_path: imdb_id}
            # Reverse path mappings: //zeus/Movies -> nfs://192.168.0.235/volume1/Movies
            PATH_MAP = {"//zeus/Movies": "nfs://192.168.0.235/volume1/Movies",
                        "//zeus/TVShows": "nfs://192.168.0.235/volume1/TVShows",
                        "//zeus/V_HD": "nfs://192.168.0.235/volume1/V_HD"}
            titles = load_titles()
            for dir_path, iid in data.items():
                if not iid or not iid.startswith("tt"): continue
                # Convert Windows path to NFS path
                nfs_path = dir_path.replace("\\", "/")
                for smb, nfs in PATH_MAP.items():
                    if nfs_path.startswith(smb):
                        nfs_path = nfs + nfs_path[len(smb):]
                        break
                for lib_iid, lib_info in list(library.items()):
                    if lib_iid.startswith("_") or not isinstance(lib_info, dict): continue
                    lib_path = lib_info.get("path", "")
                    if nfs_path in lib_path and lib_iid != iid:
                        info = library.pop(lib_iid)
                        info["nfo_matched"] = True
                        library[iid] = info
                        updated = True
                # Match to TV show episodes by directory
                show_eps = library.get("_episodes", {})
                for ek, ep in show_eps.items():
                    if not isinstance(ep, dict): continue
                    ep_path = ep.get("path", "")
                    if dir_path.replace("\\", "/") in ep_path.replace("\\", "/"):
                        show_name = ep.get("showtitle", "")
                        if show_name and iid not in titles:
                            titles[iid] = {"title": show_name, "type": "tvSeries", "_from_nfo": True}
                            updated = True
                        break
            if updated: save_titles(titles)
        if updated:
            save_user_tmm(user, library)
            print(f"[tasks] Updated {user} library")
            break

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
    safe_json_save(TITLES_FILE, titles)

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
        cast = [c["name"] for c in (credits.get("cast") or [])[:8]]
        if cast: result["cast"] = ", ".join(cast)
        # Writers (Screenplay, Writer, Story)
        crew = credits.get("crew") or []
        writers = [c["name"] for c in crew if c.get("job") in ("Screenplay", "Writer", "Story", "Novel")][:3]
        if writers: result["writers"] = ", ".join(writers)
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
        result["keywords"] = [k["name"] for k in kw_list[:40]]
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


# ── Movie Scraper ─────────────────────────────────────────────────────
import re as _re

def parse_movie_filename(filename):
    """Extract title, year, quality, 3D format from a media filename."""
    name = os.path.splitext(os.path.basename(filename))[0]
    # 3D detection
    is_3d = None
    for pattern, fmt in [("hsbs", "HSBS"), ("sbs", "SBS"), ("htab", "HTAB"), ("tab", "TAB"), ("mvc", "MVC")]:
        if pattern in name.lower():
            is_3d = fmt
            name = _re.sub(pattern, "", name, flags=_re.IGNORECASE)
    # Quality
    quality = ""
    for q in ["2160p", "1080p", "720p", "480p", "4K", "UHD"]:
        if q.lower() in name.lower():
            quality = q; break
    # Year
    year_match = _re.search(r"[\.\s\-_\(]((?:19|20)\d{2})[\.\s\-_\)]", name)
    year = year_match.group(1) if year_match else ""
    # Title: everything before the year
    if year:
        title = name[:name.find(year)].strip()
    else:
        title = name
    title = _re.sub(r"[\.\-_]", " ", title).strip()
    title = _re.sub(r"\s+", " ", title).rstrip("( ")
    return {"title": title, "year": year, "quality": quality, "is_3d": is_3d, "filename": filename}

def identify_movie(parsed, imdb_cache=None):
    """Match a parsed filename to a title via IMDB dataset or TMDB search."""
    title, year = parsed["title"], parsed["year"]
    # Try IMDB dataset first (instant, no API call)
    if imdb_cache:
        for iid, t in imdb_cache.items():
            if t.get("title","").lower() == title.lower() and str(t.get("year","")) == year:
                return iid, t
    # Fallback to TMDB search
    if TMDB_KEY:
        q = urllib.parse.quote(title)
        url = f"https://api.themoviedb.org/3/search/movie?api_key={TMDB_KEY}&query={q}"
        if year: url += f"&year={year}"
        data = api_get(url)
        if data and data.get("results"):
            r = data["results"][0]
            ext = api_get(f"https://api.themoviedb.org/3/movie/{r['id']}/external_ids?api_key={TMDB_KEY}")
            if ext and ext.get("imdb_id"):
                return ext["imdb_id"], {"title": r.get("title",""), "year": (r.get("release_date") or "")[:4], "tmdb_id": r["id"]}
    return None, None

def generate_nfo(imdb_id, title_data, filepath):
    """Generate a Kodi-compatible NFO file for a movie."""
    nfo_path = os.path.splitext(filepath)[0] + ".nfo"
    t = title_data
    nfo = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<movie>
  <title>{t.get("title","")}</title>
  <year>{t.get("year","")}</year>
  <uniqueid type="imdb" default="true">{imdb_id}</uniqueid>
  <uniqueid type="tmdb">{t.get("tmdb_id","")}</uniqueid>
  <plot>{t.get("overview","")}</plot>
  <genre>{t.get("genres","")}</genre>
  <director>{t.get("directors","")}</director>
  <rating>{t.get("imdb_rating","")}</rating>
</movie>"""
    return nfo_path, nfo

def smart_rename(filepath, imdb_id, title_data, parsed):
    """Generate a clean filename: Title (Year) [Quality].ext"""
    ext = os.path.splitext(filepath)[1]
    title = _re.sub(r'[<>:"/\\|?*]', '', title_data.get("title", ""))
    year = title_data.get("year", parsed.get("year", ""))
    quality = parsed.get("quality", "")
    is_3d = parsed.get("is_3d", "")
    parts = [title]
    if year: parts[0] += f" ({year})"
    if quality: parts.append(f"[{quality}]")
    if is_3d: parts.append(f"[{is_3d}]")
    return " ".join(parts) + ext

def scrape_movie_batch(filepaths, jid=None):
    """Identify and scrape metadata for a batch of movie files.
    Returns list of {filepath, imdb_id, title_data, parsed, nfo, new_name, status}."""
    imdb_cache = load_imdb_cache() if os.path.exists(IMDB_DATASET_DIR) else None
    results = []
    for i, fp in enumerate(filepaths):
        parsed = parse_movie_filename(fp)
        iid, tdata = identify_movie(parsed, imdb_cache)
        status = "matched" if iid else "unmatched"
        nfo_path, nfo_content = generate_nfo(iid, tdata or {}, fp) if iid else (None, None)
        new_name = smart_rename(fp, iid, tdata or {}, parsed) if iid else None
        results.append({
            "filepath": fp, "imdb_id": iid, "title_data": tdata,
            "parsed": parsed, "nfo_path": nfo_path, "nfo_content": nfo_content,
            "new_name": new_name, "status": status
        })
        if jid and (i+1) % 10 == 0:
            job_progress(jid, i+1, len(filepaths), f"Identified {i+1}/{len(filepaths)}")
        time.sleep(0.1)
    return results

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

# ── Social + Alerts ───────────────────────────────────────────────────
def get_available_alerts(user):
    """Check if any watchlisted titles just became available on streaming."""
    wl = load_watchlist(user)
    titles = load_titles()
    provs = get_user_active_providers(user)
    alerts = []
    for iid in wl:
        t = titles.get(iid, {})
        tp = set(t.get("providers", []))
        if tp & provs:
            alerts.append({"id": iid, "title": t.get("title",""), "providers": list(tp & provs)})
    return alerts

def get_activity_feed(limit=20):
    """Recent activity across all users."""
    feed = []
    for user in list_users():
        ratings = load_user_ratings(user)
        titles = load_titles()
        for iid, r in ratings.items():
            if r.get("date"):
                t = titles.get(iid, {})
                feed.append({"user": user, "action": "rated", "title": t.get("title", iid),
                             "rating": r["rating"], "date": r["date"], "id": iid})
    feed.sort(key=lambda x: x["date"], reverse=True)
    return feed[:limit]

def taste_compatibility(u1, u2):
    """Calculate taste compatibility between two users (0-100%)."""
    r1, r2 = load_user_ratings(u1), load_user_ratings(u2)
    common = set(r1.keys()) & set(r2.keys())
    if len(common) < 5: return None
    diffs = [abs(r1[i]["rating"] - r2[i]["rating"]) for i in common]
    avg_diff = sum(diffs) / len(diffs)
    # 0 diff = 100%, 5 diff = 0%
    return max(0, round((1 - avg_diff / 5) * 100))

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
def build_taste_profile(user_ratings, titles, user=None):
    """Build weighted taste profile from highly-rated titles (6+).
    Fully-watched unrated TV shows count as implicit 7."""
    # Merge in fully-watched unrated shows as implicit 7
    ratings = dict(user_ratings)
    if user:
        library = load_user_tmm(user)
        eps = library.get("_episodes", {})
        show_stats = {}
        for ep in eps.values():
            if not isinstance(ep, dict): continue
            show = ep.get("showtitle", "")
            if not show: continue
            show_stats.setdefault(show, [0, 0])
            show_stats[show][0] += 1
            if ep.get("playcount", 0) > 0: show_stats[show][1] += 1
        # Map show titles to IMDB IDs via titles.json
        title_to_iid = {}
        for iid, t in titles.items():
            if t.get("type") in ("tvSeries", "tvMiniSeries", "tv"):
                title_to_iid[t.get("title", "").lower()] = iid
        for show_name, (total, watched) in show_stats.items():
            if total > 0 and watched == total:
                iid = title_to_iid.get(show_name.lower(), "")
                if iid and iid not in ratings:
                    ratings[iid] = {"rating": 7, "date": "", "implicit": True}
    keyword_scores, genre_scores, director_scores, actor_scores, writer_scores = {}, {}, {}, {}, {}
    for iid, r in ratings.items():
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
        for w in (t.get("writers") or "").split(","):
            w = w.strip()
            if w: writer_scores[w] = writer_scores.get(w, 0) + weight
    return {"keywords": keyword_scores, "genres": genre_scores,
            "directors": director_scores, "actors": actor_scores, "writers": writer_scores}

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
    # Keywords: full weight (themes, moods, plot elements)
    for kw in title.get("keywords", []):
        score += kw_prof.get(kw, 0)
    # Genres: half weight (broader, less specific)
    for g in (title.get("genres") or "").split(","):
        g = g.strip()
        if g: score += g_prof.get(g, 0) * 0.5
    # Directors: strong signal
    for d in (title.get("directors") or "").split(","):
        d = d.strip()
        if d: score += profile.get("directors", {}).get(d, 0) * 2.0
    # Actors: moderate signal
    for a in (title.get("cast") or "").split(","):
        a = a.strip()
        if a: score += profile.get("actors", {}).get(a, 0) * 1.5
    # Writers: moderate signal
    for w in (title.get("writers") or "").split(","):
        w = w.strip()
        if w: score += profile.get("writers", {}).get(w, 0) * 1.5
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

# ── Recommendations v2 ────────────────────────────────────────────────
def collaborative_recommendations(user, titles, n=20):
    """Find titles loved by users with similar taste (pseudo-collaborative).
    Uses TMDB 'similar' data as a proxy for collaborative filtering."""
    ratings = load_user_ratings(user)
    rated_ids = set(ratings.keys())
    # Collect similar titles from highly-rated movies
    similar_scores = {}
    for iid, r in ratings.items():
        if r["rating"] < 7: continue
        t = titles.get(iid, {})
        for sim_tmdb in t.get("similar_tmdb", []):
            # Find IMDB ID for this TMDB ID
            for sid, st in titles.items():
                if st.get("tmdb_id") == sim_tmdb and sid not in rated_ids:
                    similar_scores[sid] = similar_scores.get(sid, 0) + r["rating"] / 10
                    break
    return sorted(similar_scores.items(), key=lambda x: x[1], reverse=True)[:n]

def anti_recommendations(user, titles, n=10):
    """Titles you'd probably hate — high-rated titles in genres you consistently rate low."""
    ratings = load_user_ratings(user)
    genre_avg = {}
    genre_count = {}
    for iid, r in ratings.items():
        t = titles.get(iid, {})
        for g in (t.get("genres") or "").split(","):
            g = g.strip()
            if not g: continue
            genre_avg[g] = genre_avg.get(g, 0) + r["rating"]
            genre_count[g] = genre_count.get(g, 0) + 1
    # Find genres with low average (< 5) and enough samples (3+)
    hated_genres = set()
    for g, total in genre_avg.items():
        avg = total / genre_count[g]
        if avg < 5 and genre_count[g] >= 3:
            hated_genres.add(g)
    if not hated_genres: return []
    # Find highly-rated titles in those genres
    anti = []
    rated_ids = set(ratings.keys())
    for iid, t in titles.items():
        if iid in rated_ids: continue
        t_genres = set(g.strip() for g in (t.get("genres") or "").split(","))
        if t_genres & hated_genres and t.get("imdb_rating", 0) and t["imdb_rating"] > 7:
            anti.append((iid, t, t["imdb_rating"]))
    anti.sort(key=lambda x: x[2], reverse=True)
    return anti[:n]

def _richness(t):
    """Score how complete a title's metadata is (0-10). Used to prioritize re-enrichment."""
    score = 0
    if t.get("poster"): score += 2
    if t.get("overview") or t.get("plot"): score += 1
    if t.get("rotten_tomatoes"): score += 1
    if t.get("metacritic"): score += 1
    if t.get("tmdb_rating"): score += 1
    if t.get("providers"): score += 1
    if t.get("keywords"): score += 1
    if t.get("cast"): score += 1
    if t.get("writers"): score += 1
    return score

def enrich_titles(jid=None, fast=False):
    """Enrich all titles: unenriched first, then poorest metadata.
    Pulls from TMDB (poster, keywords, streaming), OMDB (RT, Metacritic), TVDB.
    Saves incrementally every 50 titles. Runs as background job."""
    titles = load_titles()
    never = [(k, v) for k, v in titles.items() if not v.get("_enriched")]
    # Priority: titles visible on recs/save-space that lack keywords or cast
    visible_ids = set()
    for user in list_users():
        ratings = load_user_ratings(user)
        profile = build_taste_profile(ratings, titles, user)
        library = load_user_tmm(user)
        for iid in library:
            if iid.startswith("_") or iid in ratings: continue
            visible_ids.add(iid)
        for iid, t in titles.items():
            if iid not in ratings and t.get("_enriched"):
                visible_ids.add(iid)
    visible = [(k, titles[k]) for k in visible_ids
               if k in titles and titles[k].get("_enriched") and (not titles[k].get("keywords") or not titles[k].get("cast"))]
    partial = sorted([(k, v) for k, v in titles.items() if v.get("_enriched") and _richness(v) < 5
                      and k not in {x[0] for x in visible}],
                     key=lambda x: _richness(x[1]))
    # FIFO: re-enrich the 50 oldest-enriched titles regardless of age
    stale = sorted([(k, v) for k, v in titles.items()
                    if v.get("_enriched") and v.get("_enriched_ts") and _richness(v) >= 5],
                   key=lambda x: x[1].get("_enriched_ts", ""))[:50]
    todo = never + visible + partial + stale
    print(f"  Enrich queue: {len(never)} new, {len(visible)} visible priority, {len(partial)} partial, {len(stale)} stale")
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

def import_streaming_history(user, service, text):
    """Import watch history from streaming services into user history."""
    history = load_user_history(user) or {}
    titles = load_titles()
    imported = 0
    
    if service == "netflix":
        # Netflix CSV: Title, Date
        for row in csv.DictReader(io.StringIO(text)):
            title = row.get("Title", "")
            date = row.get("Date", "")
            if not title: continue
            # Search TMDB for IMDB ID
            if TMDB_KEY:
                data = api_get(f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_KEY}&query={urllib.parse.quote(title[:60])}")
                if data and data.get("results"):
                    r = data["results"][0]
                    kind = r.get("media_type", "movie")
                    ext = api_get(f"https://api.themoviedb.org/3/{kind}/{r['id']}/external_ids?api_key={TMDB_KEY}")
                    if ext and ext.get("imdb_id"):
                        iid = ext["imdb_id"]
                        history[iid] = {"date": date, "source": "netflix", "title": title}
                        if iid not in titles:
                            titles[iid] = {"title": r.get("title") or r.get("name",""), "year": (r.get("release_date") or r.get("first_air_date",""))[:4], "type": kind}
                        imported += 1
                time.sleep(0.2)
    
    elif service == "prime":
        # Amazon CSV or JSON
        if text.strip().startswith("["):
            for item in json.loads(text):
                title = item.get("title", "")
                if title and TMDB_KEY:
                    data = api_get(f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_KEY}&query={urllib.parse.quote(title[:60])}")
                    if data and data.get("results"):
                        r = data["results"][0]
                        ext = api_get(f"https://api.themoviedb.org/3/{r.get('media_type','movie')}/{r['id']}/external_ids?api_key={TMDB_KEY}")
                        if ext and ext.get("imdb_id"):
                            history[ext["imdb_id"]] = {"date": item.get("date",""), "source": "prime", "title": title}
                            imported += 1
                    time.sleep(0.2)
        else:
            for row in csv.DictReader(io.StringIO(text)):
                title = row.get("Title", row.get("title", ""))
                if title:
                    history["_prime_" + title[:50]] = {"source": "prime", "title": title}
                    imported += 1
    
    elif service == "letterboxd":
        import_letterboxd(user, text)
        return 0
    
    else:
        # Generic: one title per line or CSV with Title column
        if "," in text.split("\n")[0]:
            for row in csv.DictReader(io.StringIO(text)):
                title = row.get("Title", row.get("title", row.get("Name", "")))
                if title:
                    history["_" + service + "_" + title[:50]] = {"source": service, "title": title}
                    imported += 1
        else:
            for line in text.strip().split("\n"):
                line = line.strip()
                if line:
                    history["_" + service + "_" + line[:50]] = {"source": service, "title": line}
                    imported += 1
    
    save_user_history(user, history)
    save_titles(titles)
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
def _render_agent_status():
    s = load_agent_status()
    if not s: return '<p style="color:#888">No agent connected</p>'
    version = s.get("agent_version", "?")
    uptime_s = s.get("uptime", 0)
    uptime = str(uptime_s // 3600) + "h " + str((uptime_s % 3600) // 60) + "m"
    last = s.get("last_activity", {})
    errors = s.get("consecutive_errors", 0)
    dot = "🟢" if errors == 0 else "🔴"
    logs = s.get("recent_logs", [])
    log_lines = "".join(l.rstrip() + "\n" for l in logs[-8:])
    log_html = '<pre style="font-size:.75em;max-height:120px;overflow-y:auto;background:#1a1a2e;padding:8px;border-radius:4px;margin-top:8px">' + log_lines + '</pre>' if logs else ""
    return dot + ' <b>Agent v' + version + '</b> · Seen: ' + s.get("last_seen","?") + ' · Up: ' + uptime + ' · Last: ' + last.get("task","idle") + ' at ' + last.get("time","?") + log_html

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
        vsource = detect_video_source(tmm.get(iid, {}).get("path", "")) if iid in tmm else ""
        vsource_icon = SOURCE_ICONS.get(vsource, "")
        local = ('💾' + vsource_icon + ' ' + sub_icon) if iid in tmm else ""
        tooltip = f' title="{t.get("overview","")[:200]}"' if t.get("overview") else ""
        awards_badge = " 🏆" if t.get("awards") and ("Oscar" in t.get("awards","") or "Won" in t.get("awards","")) else ""
        trailer_link = (' <a href="' + t.get("trailer","") + '" target="_blank" title="Trailer">▶️</a>') if t.get("trailer") else ""
        similar_link = ' <a href="' + BASE + '/similar/' + iid + '" title="Similar">🔗</a>'
        rows += f'<tr data-g="{t.get("genres","")}" data-r="{r["rating"]}" data-s="{" ".join(provs)}" data-d="{str(t.get("year",""))[:3]}0" data-vs="{vsource}"><td>{poster}</td><td><a href="https://www.imdb.com/title/{iid}/" target="_blank"{tooltip}>{t.get("title",iid)}</a>{awards_badge}{trailer_link}{similar_link}</td><td>{t.get("year","")}</td><td style="font-weight:bold;color:{c}">{r["rating"]}</td><td>{imdb}</td><td class="x">{" ".join(scores)}</td><td>{stream}</td><td class="x">{t.get("genres","")}</td><td class="x">{r.get("date","")}</td><td>{local}</td></tr>'
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
document.querySelectorAll('tbody tr').forEach(r=>r.style.display=(r.textContent.toLowerCase().includes(q)&&(!g||r.dataset.g.includes(g))&&(!mr||parseInt(r.dataset.r)>=parseInt(mr))&&(!st||r.dataset.s.includes(st))&&(!dec||r.dataset.d===dec)&&(!vs||r.dataset.vs===vs))?'':'none')}}
function sortTable(n){{const tb=document.querySelector('tbody'),rows=[...tb.rows],dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:'';
rows.sort((a,b)=>{{let x=a.cells[n].textContent,y=b.cells[n].textContent;return(typeof x==="number"&&typeof y==="number"?(x-y):(String(x)).localeCompare(String(y),undefined,{{numeric:true}}))*dir}});rows.forEach(r=>tb.appendChild(r))}}</script></head><body>
{job_banner}
<div style="display:flex;justify-content:space-between;align-items:center"><h2>🎬 {user}'s Ratings — {len(ratings)} titles</h2>{render_user_bar(user)}</div>
<div class="bar"><input id="s" onkeyup="f()" placeholder="Search..." style="width:220px">
<select id="g" onchange="f()"><option value="">All genres</option>{genre_opts}</select>
<select id="mr" onchange="f()"><option value="">Min ★</option>{''.join(f'<option value="{i}">{i}+</option>' for i in range(10,0,-1))}</select>
<select id="dec" onchange="f()"><option value="">All decades</option><option value="2020">2020s</option><option value="2010">2010s</option><option value="2000">2000s</option><option value="1990">1990s</option><option value="1980">1980s</option><option value="1970">1970s</option><option value="1960">1960s</option><option value="1950">1950s</option></select>
<select id="st" onchange="f()"><option value="">All streams</option>{"".join('<option value="' + p + '">' + PROVIDER_ICONS.get(p,"▪") + " " + p + '</option>' for p in sorted(user_provs))}</select>
<a href="{BASE}/tonight/{user}" class="btn">🎲 Tonight</a><a href="{BASE}/recs/{user}" class="btn">🎯 Recs</a><a href="{BASE}/catalog" class="btn">📺 Catalog</a><a href="{BASE}/library/{user}" class="btn">📚 Library</a><a href="{BASE}/tvshows/{user}" class="btn">📺 TV Shows</a><a href="{BASE}/stats/{user}" class="btn">📊 Stats</a><a href="{BASE}/new" class="btn">🆕 New</a><a href="{BASE}/random/{user}" class="btn">🎰 Random</a><a href="{BASE}/compare/" class="btn">👥 Compare</a><a href="{BASE}/enrich" class="btn">⚡ Enrich</a><a href="{BASE}/export/{user}" class="btn">⬇ Export</a><a href="{BASE}/rss/{user}" class="btn">📡 RSS</a><a href="{BASE}/setup/{user}" class="btn">⚙ Setup</a>
{f'<a href="{BASE}/trakt/sync/{user}">↕ Trakt</a>' if has_trakt else ""}
<button onclick="document.body.classList.toggle('light');localStorage.setItem('theme',document.body.classList.contains('light')?'light':'dark')" style="background:none;border:1px solid #444;border-radius:4px;cursor:pointer;padding:2px 8px;color:var(--fg)" title="Toggle dark/light theme">🌓</button> <span style="color:#666;font-size:.8em">{" ".join(services)}</span></div>
<div style="margin-bottom:10px;font-size:1.2em">Mood: <a href="{BASE}/mood/{user}/light" title="Light" style="text-decoration:none">☀️</a><a href="{BASE}/mood/{user}/intense" title="Intense" style="text-decoration:none">🔥</a><a href="{BASE}/mood/{user}/funny" title="Funny" style="text-decoration:none">😂</a><a href="{BASE}/mood/{user}/mind-bending" title="Mind-Bending" style="text-decoration:none">🌀</a><a href="{BASE}/mood/{user}/dark" title="Dark" style="text-decoration:none">🌑</a><a href="{BASE}/mood/{user}/epic" title="Epic" style="text-decoration:none">⚔️</a><a href="{BASE}/mood/{user}/romantic" title="Romantic" style="text-decoration:none">💕</a><a href="{BASE}/mood/{user}/scary" title="Scary" style="text-decoration:none">👻</a><a href="{BASE}/mood/{user}/inspiring" title="Inspiring" style="text-decoration:none">✨</a></div>
<table><thead><tr><th></th><th onclick="sortTable(1)">Title</th><th onclick="sortTable(2)">Year</th><th onclick="sortTable(3)">★</th><th onclick="sortTable(4)">IMDB</th><th>Scores</th><th>Stream</th><th onclick="sortTable(7)">Genres</th><th onclick="sortTable(8)">Rated</th><th>💾</th></tr></thead>
<tbody>{rows}</tbody></table></div></body></html>"""

def render_recs(user):
    titles = load_titles()
    cats, profile = get_5cat_recommendations(user, titles, n_per_cat=6)
    watchlist = set(load_watchlist(user))
    user_ratings = load_user_ratings(user)
    
    top_kw = sorted(profile["keywords"].items(), key=lambda x: x[1], reverse=True)[:12]
    top_g = sorted(profile["genres"].items(), key=lambda x: x[1], reverse=True)[:6]
    top_d = sorted(profile["directors"].items(), key=lambda x: x[1], reverse=True)[:5]
    top_a = sorted(profile["actors"].items(), key=lambda x: x[1], reverse=True)[:8]
    
    taste_kw = " ".join('<span style="background:#16213e;padding:2px 8px;border-radius:10px;font-size:.8em">' + k + '</span>' for k, v in top_kw)
    taste_g = " ".join('<span style="background:#1a3a5e;padding:2px 8px;border-radius:10px;font-size:.8em">' + k + '</span>' for k, v in top_g)
    taste_d = " ".join('<span style="background:#3a1a5e;padding:2px 8px;border-radius:10px;font-size:.8em">' + k + '</span>' for k, v in top_d)
    taste_a = " ".join('<span style="background:#1a3a3a;padding:2px 8px;border-radius:10px;font-size:.8em">' + k + '</span>' for k, v in top_a)
    
    cat_meta = [
        ("dna", "🧬 Deep Cuts for You", "Based on your keyword DNA — themes, moods, plot elements"),
        ("cast", "🎬 The Director\'s Chair", "From creators and actors you love"),
        ("community", "👥 Community Picks", "Loved by users with similar taste"),
        ("overlap", "✅ Unanimous Hits", "Highly rated across IMDB, TMDB, and critics"),
        ("rewatch", "💫 Blast from the Past", "Favorites you haven\'t seen in years"),
    ]
    
    # Build columns layout
    columns = ""
    for cat_key, cat_title, cat_desc in cat_meta:
        items = cats.get(cat_key, [])
        if not items: continue
        cards = ""
        for iid, t, score in items:
            poster = '<img src="' + t.get("poster","") + '" style="border-radius:4px;width:45px;height:65px;object-fit:cover" loading="lazy">' if t.get("poster") else ""
            provs = " ".join(PROVIDER_ICONS.get(p,"") for p in t.get("providers",[]) if p in get_user_active_providers(user))
            wl = '<a href="' + BASE + '/watchlist/add/' + iid + '">🤍</a>' if iid not in watchlist else '<a href="' + BASE + '/watchlist/rm/' + iid + '">❤️</a>'
            trailer = ' <a href="' + t.get("trailer","") + '" target="_blank">▶️</a>' if t.get("trailer") else ""
            prev_rating = user_ratings.get(iid, {}).get("rating", 0) if cat_key == "rewatch" else 0
            if prev_rating:
                stars = "".join('<a href="' + BASE + '/rate/' + user + '/' + iid + '/' + str(s) + '" style="text-decoration:none;color:' + ('#4fc3f7' if s <= prev_rating else '#444') + '">' + "★" + '</a>' for s in range(1, 11))
            else:
                stars = "".join('<a href="' + BASE + '/rate/' + user + '/' + iid + '/' + str(s) + '" style="text-decoration:none;color:gold">' + ("★" if s <= 5 else "☆") + '</a>' for s in range(1, 11))
            cards += '<div style="display:flex;gap:8px;padding:8px 0;border-bottom:1px solid var(--border,#333);align-items:center">'
            cards += poster
            cards += '<div style="flex:1;min-width:0;overflow:hidden">'
            cards += '<div style="white-space:nowrap;overflow:hidden;text-overflow:ellipsis"><b><a href="https://www.imdb.com/title/' + iid + '/" target="_blank" title="' + t.get("overview","")[:150] + '">' + t.get("title","?") + '</a></b> ' + wl + trailer + '</div>'
            cards += '<div style="color:#888;font-size:.8em">' + str(t.get("year","")) + ' · ' + provs + ' · ' + str(t.get("imdb_rating","")) + ' · <span style="color:var(--accent)">' + str(score) + '</span></div>'
            cards += '<div style="font-size:.75em">' + stars + '</div>'
            cards += '</div></div>'
        columns += '<div style="min-width:0"><h4 style="margin:0 0 5px;white-space:nowrap">' + cat_title + '</h4>'
        columns += '<p style="color:#888;font-size:.75em;margin:0 0 8px">' + cat_desc + '</p>'
        columns += cards + '</div>'
    sections = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:20px;align-items:start">' + columns + '</div>'
    
    user_bar = render_user_bar(user, "recs", False)
    html = page_head("Recommendations for " + user)
    html += nav_bar("discover", user)
    html += '<div class="page">'
    html += '<h2>🎯 Recommendations <span style="color:var(--muted);font-weight:normal;font-size:.6em">Movies & TV shows</span></h2>'
    html += '<details style="margin-bottom:20px"><summary style="cursor:pointer;color:#4fc3f7">Your taste profile</summary>'
    html += '<p><b>Keywords:</b> ' + taste_kw + '</p>'
    html += '<p><b>Genres:</b> ' + taste_g + '</p>'
    html += '<p><b>Directors:</b> ' + taste_d + '</p>'
    html += '<p><b>Actors:</b> ' + taste_a + '</p></details>'
    html += sections
    html += '<p style="margin-top:20px"><a href="' + BASE + '/tonight/' + user + '">🎲 Pick one for tonight</a> · '
    html += '<a href="' + BASE + '/u/' + user + '">← Ratings</a></p></div>' + page_foot()
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
    
    # Agent Status
    html += '<h3>Agent Status</h3>'
    html += _render_agent_status()
    html += '<hr>'
    
    # Streaming History Import
    html += '<h3>Import Streaming History</h3>'
    html += '<p><a href="' + BASE + '/import/streaming/' + user + '" class="btn btn-primary">Import Netflix / Prime / Disney+ / HBO history</a></p>'
    html += '<hr>'
    
    # Streaming Region
    html += '<h3>Streaming Region</h3>'
    html += '<p>Region: <b>' + WATCH_COUNTRY + '</b> | <a href="' + BASE + '/catalog">Browse catalog</a></p>'
    
    html += '</div></body></html>'
    return html



# ── TV Show Intelligence ──────────────────────────────────────────────

# ── Library Organization ──────────────────────────────────────────────
def analyze_library(user):
    """Analyze library for organization issues."""
    library = load_user_tmm(user)
    titles = load_titles()
    items = {k: v for k, v in library.items() if not k.startswith("_") and isinstance(v, dict)}
    
    # Orphans: in library but not in titles store (unmatched)
    orphans = [(iid, info) for iid, info in items.items() if iid not in titles]
    
    # Drive/folder summary
    from collections import defaultdict
    drives = defaultdict(lambda: {"count": 0, "size": 0})
    for iid, info in items.items():
        path = info.get("path", "")
        # Extract root (drive or first 2 path components)
        parts = path.replace("\\", "/").split("/")
        root = "/".join(parts[:4]) if len(parts) > 3 else "/".join(parts[:2])
        drives[root]["count"] += 1
        s = info.get("file_size") or info.get("size") or 0
        if isinstance(s, str):
            try: s = int(s)
            except: s = 0
        drives[root]["size"] += s
    
    # Duplicates: same IMDB ID appearing multiple times (shouldn't happen in dict, but same-size files)
    from collections import Counter
    size_counts = Counter()
    size_map = defaultdict(list)
    for iid, info in items.items():
        s = info.get("file_size") or info.get("size") or 0
        if s:
            size_counts[s] += 1
            size_map[s].append((iid, info))
    dupes = {s: entries for s, entries in size_map.items() if len(entries) > 1}
    
    # Quality distribution
    quality = defaultdict(int)
    codec = defaultdict(int)
    for iid, info in items.items():
        h = info.get("video_height") or info.get("quality") or ""
        if isinstance(h, int) or (isinstance(h, str) and h.isdigit()):
            h = int(h)
            if h >= 2160: quality["4K"] += 1
            elif h >= 1080: quality["1080p"] += 1
            elif h >= 720: quality["720p"] += 1
            elif h > 0: quality["SD"] += 1
        c = info.get("video_codec", "")
        if c: codec[c.lower()] += 1
    
    # Missing data
    no_size = sum(1 for info in items.values() if not (info.get("file_size") or info.get("size")))
    no_subs = sum(1 for info in items.values() if not info.get("subtitles"))
    
    return {
        "total": len(items),
        "orphans": len(orphans),
        "drives": dict(sorted(drives.items(), key=lambda x: x[1]["size"], reverse=True)),
        "duplicates": len(dupes),
        "dupe_details": {str(s): [(iid, info.get("path","")[:80]) for iid, info in entries] for s, entries in list(dupes.items())[:20]},
        "quality": dict(quality),
        "codec": dict(codec),
        "no_size": no_size,
        "no_subs": no_subs,
    }


# ── Title/Path Mismatch Detection ─────────────────────────────────────
import re as _re
import unicodedata as _ud


# ── Video Source Detection ────────────────────────────────────────────
SOURCE_ICONS = {"bluray": "💿", "dvd": "📀", "webrip": "🌐", "webdl": "🌐", "hdtv": "📡", "telesync": "📹", "cam": "📷", "remux": "💎"}

def detect_video_source(path):
    """Detect video source from filename/path."""
    p = path.lower()
    if "remux" in p: return "remux"
    if "blu-ray" in p or "bluray" in p or "brrip" in p or "bdmv" in p or "bdrip" in p: return "bluray"
    if "dvd" in p or "video_ts" in p: return "dvd"
    if "webrip" in p or "web-rip" in p: return "webrip"
    if "webdl" in p or "web-dl" in p or "web dl" in p: return "webdl"
    if "hdtv" in p: return "hdtv"
    if "telesync" in p or "ts" in p.split("_"): return "telesync"
    if "cam" in p.split("_") or "camrip" in p: return "cam"
    return ""

def _normalize(s):
    """Normalize a string for fuzzy comparison: lowercase, strip tags, accents, punctuation."""
    s = s.lower()
    # Remove common tags
    for tag in ["1080p","720p","480p","2160p","4k","uhd","hdr","bluray","blu-ray","webrip",
                "brrip","dvdrip","hdtv","aac","ac3","dts","x264","x265","hevc","h264",
                "mbps","telesync","remastered","extended","directors.cut","unrated"]:
        s = s.replace(tag.lower(), "")
    # Strip accents
    s = "".join(c for c in _ud.normalize("NFD", s) if _ud.category(c) != "Mn")
    # Remove punctuation, underscores, dots, parens, brackets, year
    s = _re.sub(r"[()\[\]{}_.,:;!?\x27\x22-]", " ", s)
    s = _re.sub(r"(19|20)\d{2}", "", s)  # remove years
    s = _re.sub(r"\s+", " ", s).strip()
    return s

def _extract_title_from_path(path):
    """Extract likely title from a file path."""
    parts = path.replace("\\", "/").split("/")
    # Skip generic filenames
    fname = parts[-1] if parts else ""
    if fname.lower() in ("video_ts.ifo","index.bdmv","movieobject.bdmv",""):
        # Use parent or grandparent directory
        fname = parts[-3] if len(parts) >= 3 else parts[-2] if len(parts) >= 2 else ""
    else:
        fname = fname.rsplit(".", 1)[0]  # remove extension
    return _normalize(fname)

def _fuzzy_match(a, b):
    """Simple fuzzy match: ratio of common words."""
    wa = set(a.split())
    wb = set(b.split())
    if not wa or not wb: return 0
    common = wa & wb
    return len(common) / max(len(wa), len(wb))

def find_mismatches(user, threshold=0.3):
    """Find titles where the IMDB title doesn't match the filename."""
    library = load_user_tmm(user)
    titles = load_titles()
    mismatches = []
    for iid, info in library.items():
        if iid.startswith("_") or not isinstance(info, dict): continue
        path = info.get("path", "")
        if not path: continue
        t = titles.get(iid, {})
        db_title = t.get("title", "")
        if not db_title: continue
        path_title = _extract_title_from_path(path)
        norm_db = _normalize(db_title)
        if not path_title or not norm_db: continue
        score = _fuzzy_match(norm_db, path_title)
        if score < threshold:
            mismatches.append({
                "iid": iid, "db_title": db_title, "year": t.get("year",""),
                "path_title": path_title, "path": path,
                "match": round(score, 2),
            })
    mismatches.sort(key=lambda x: x["match"])
    return mismatches

def analyze_show(show_name, seasons_data):
    """Analyze a TV show for gaps, quality issues, and completion."""
    analysis = {"gaps": [], "quality_issues": [], "completion": {}, "next_episode": None}
    all_watched = []
    
    for season_num in sorted(seasons_data.keys()):
        eps = seasons_data[season_num]
        if season_num == 0: continue  # Skip specials
        ep_nums = sorted(set(ep.get("episode", 0) for ep in eps))
        
        # Episode gaps: find missing numbers in sequence
        if ep_nums:
            expected = set(range(1, max(ep_nums) + 1))
            missing = sorted(expected - set(ep_nums))
            if missing:
                analysis["gaps"].append({"season": season_num, "missing": missing})
        
        # Season completion
        watched_in_season = sum(1 for ep in eps if ep.get("playcount", 0) > 0)
        analysis["completion"][season_num] = {
            "total": len(eps), "watched": watched_in_season,
            "pct": round(watched_in_season / len(eps) * 100) if eps else 0
        }
        
        # Quality consistency: flag mixed resolutions within a season
        heights = set(ep.get("video_height", 0) for ep in eps if ep.get("video_height"))
        if len(heights) > 1:
            analysis["quality_issues"].append({
                "season": season_num, "type": "mixed_resolution",
                "values": sorted(heights, reverse=True)
            })
        
        # Track watched episodes for next-episode prediction
        for ep in eps:
            if ep.get("playcount", 0) > 0:
                all_watched.append((season_num, ep.get("episode", 0)))
    
    # Next episode prediction: find the first unwatched after last watched
    if all_watched:
        last_s, last_e = max(all_watched)
        for season_num in sorted(seasons_data.keys()):
            if season_num < last_s: continue
            for ep in sorted(seasons_data[season_num], key=lambda x: x.get("episode", 0)):
                s, e = season_num, ep.get("episode", 0)
                if (s, e) > (last_s, last_e) and ep.get("playcount", 0) == 0:
                    analysis["next_episode"] = {"season": s, "episode": e, "title": ep.get("title", "")}
                    break
            if analysis["next_episode"]: break
    
    return analysis

def render_tvshows(user):
    """TV Shows screen: episodes grouped by show/season, quality, duplicates."""
    library = load_user_tmm(user)
    episodes = library.get("_episodes", {})
    if not episodes:
        return '<html><body style="background:#1a1a2e;color:#eee;font-family:sans-serif;padding:40px"><h2>No TV episodes synced</h2><p>Re-run the agent to fetch episodes from Kodi</p><a href="' + BASE + '/u/' + user + '" style="color:#4fc3f7">← Back</a></body></html>'

    from collections import defaultdict
    # Group by show -> season -> episode
    shows = defaultdict(lambda: defaultdict(list))
    for key, ep in episodes.items():
        if not isinstance(ep, dict): continue
        show = ep.get("showtitle", "Unknown")
        season = ep.get("season", 0)
        shows[show][season].append(ep)

    # Stats
    total_eps = len(episodes)
    total_shows = len(shows)
    watched = sum(1 for ep in episodes.values() if isinstance(ep, dict) and ep.get("playcount", 0) > 0)
    no_subs = sum(1 for ep in episodes.values() if isinstance(ep, dict) and not ep.get("subtitles"))

    # Quality breakdown
    codec_map = {"hevc": "x265", "h265": "x265", "h264": "x264", "avc": "x264", "mpeg2": "MPEG-2"}
    quality_dist = defaultdict(int)
    codec_dist = defaultdict(int)
    for ep in episodes.values():
        if not isinstance(ep, dict): continue
        h = ep.get("video_height", 0) or 0
        if h >= 2160: quality_dist["4K"] += 1
        elif h >= 1080: quality_dist["1080p"] += 1
        elif h >= 720: quality_dist["720p"] += 1
        elif h > 0: quality_dist["SD"] += 1
        c = ep.get("video_codec", "")
        if c: codec_dist[codec_map.get(c.lower(), c)] += 1

    # Detect duplicate episodes (same show+season+episode, multiple files)
    ep_groups = defaultdict(list)
    for key, ep in episodes.items():
        if not isinstance(ep, dict): continue
        gkey = ep.get("showtitle","") + "|" + str(ep.get("season",0)) + "|" + str(ep.get("episode",0))
        ep_groups[gkey].append(ep)
    dupes = {k: v for k, v in ep_groups.items() if len(v) > 1}

    # Build show list
    show_rows = ""
    for show_name in sorted(shows.keys()):
        seasons = shows[show_name]
        total_s = len(seasons)
        total_e = sum(len(eps) for eps in seasons.values())
        watched_e = sum(1 for eps in seasons.values() for ep in eps if ep.get("playcount", 0) > 0)
        # Quality summary for this show
        codecs = defaultdict(int)
        for eps in seasons.values():
            for ep in eps:
                c = ep.get("video_codec", "")
                if c: codecs[codec_map.get(c.lower(), c)] += 1
        codec_str = ", ".join(c + ":" + str(n) for c, n in sorted(codecs.items(), key=lambda x: x[1], reverse=True))
        res = set()
        for eps in seasons.values():
            for ep in eps:
                h = ep.get("video_height", 0)
                if h: res.add(str(h) + "p")
        res_str = "/".join(sorted(res, reverse=True))
        pct = round(watched_e / total_e * 100) if total_e else 0
        bar_color = "#2d7" if pct == 100 else "#f90" if pct > 0 else "#d72"
        show_rows += '<tr>'
        show_rows += '<td><b>' + show_name + '</b></td>'
        show_rows += '<td>' + str(total_s) + '</td>'
        show_rows += '<td>' + str(total_e) + '</td>'
        show_rows += '<td data-sort="' + str(pct) + '"><div style="display:flex;align-items:center;gap:6px"><div style="background:#333;border-radius:3px;width:80px;height:12px"><div style="background:' + bar_color + ';height:12px;width:' + str(pct * 0.8) + 'px;border-radius:3px"></div></div>' + str(pct) + '%</div></td>'
        show_rows += '<td>' + res_str + '</td>'
        show_rows += '<td style="font-size:.85em">' + codec_str + '</td>'
        # TV Intelligence
        analysis = analyze_show(show_name, seasons)
        badges = ""
        if analysis["gaps"]:
            gap_count = sum(len(g["missing"]) for g in analysis["gaps"])
            badges += '<span style="color:#d72" title="' + str(gap_count) + ' missing episodes">⚠' + str(gap_count) + ' gaps</span> '
        if analysis["quality_issues"]:
            badges += '<span style="color:#f90" title="Mixed quality in season">🔀 mixed</span> '
        if analysis["next_episode"]:
            ne = analysis["next_episode"]
            badges += '<span style="color:#4fc3f7" title="Next: S' + str(ne["season"]).zfill(2) + 'E' + str(ne["episode"]).zfill(2) + '">▶ S' + str(ne["season"]).zfill(2) + 'E' + str(ne["episode"]).zfill(2) + '</span>'
        show_rows += '<td>' + badges + '</td>'
        show_rows += '</tr>'

    # Quality bars
    max_q = max(quality_dist.values()) if quality_dist else 1
    q_bars = ""
    for q in ["4K", "1080p", "720p", "SD"]:
        c = quality_dist.get(q, 0)
        if c: q_bars += '<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:50px;text-align:right">' + q + '</span><div style="background:#4fc3f7;height:18px;width:' + str(min(c/max_q*300, 300)) + 'px;border-radius:3px"></div><span style="color:#888">' + str(c) + '</span></div>'

    html = page_head("TV Shows - " + user)
    html += nav_bar("library", user)
    html += render_library_nav(user, "tvshows")
    html += '<div class="page">'
    html += '<script>function f(){const q=document.getElementById("s").value.toLowerCase();document.querySelectorAll("tbody tr").forEach(r=>r.style.display=r.textContent.toLowerCase().includes(q)?"":"none")}</script>'
    html += '<script>function sortTable(n){const tb=document.querySelector("tbody"),rows=[...tb.rows],dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:"";rows.sort((a,b)=>{let x=a.cells[n].textContent,y=b.cells[n].textContent;return(typeof x==="number"&&typeof y==="number"?(x-y):(String(x)).localeCompare(String(y),undefined,{numeric:true}))*dir});rows.forEach(r=>tb.appendChild(r))}'
    html += 'function rate(el,user,iid,score){fetch("' + BASE + '/rate/"+user+"/"+iid+"/"+score).then(()=>{const row=el.closest("tr");const stars=row.querySelectorAll("a[href*=rate]");stars.forEach((s,i)=>{s.style.color=i<score?"#4fc3f7":"#444"});row.style.opacity="0.6"})}'
    html += '</script>'
    html += '</head><body>'
    html += '<h2>📺 TV Shows — ' + user + '</h2>'

    # Stats
    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card"><div style="font-size:2.5em">' + str(total_shows) + '</div>shows</div>'
    html += '<div class="card"><div style="font-size:2.5em">' + str(total_eps) + '</div>episodes</div>'
    html += '<div class="card"><div style="font-size:2.5em">' + str(watched) + '</div>watched</div>'
    html += '<div class="card"><div style="font-size:2.5em;color:#d72">' + str(no_subs) + '</div>no subs</div>'
    html += '<div class="card"><div style="font-size:2.5em;color:#f90">' + str(len(dupes)) + '</div>duplicate eps</div>'
    html += '</div>'

    # Quality breakdown
    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card"><h3>Resolution</h3>' + q_bars + '</div>'
    html += '</div>'

    # Show list
    html += '<div style="margin-bottom:10px"><input id="s" onkeyup="f()" placeholder="Search shows..."></div>'
    html += '<table><thead><tr><th onclick="sortTable(0)">Show</th><th onclick="sortTable(1)">Seasons</th><th onclick="sortTable(2)">Episodes</th><th onclick="sortTable(3)">Watched</th><th onclick="sortTable(4)">Quality</th><th>Codecs</th><th>Intel</th></tr></thead>'
    html += '<tbody>' + show_rows + '</tbody></table>'

    # Duplicate episodes
    if dupes:
        dupe_rows = ""
        for gkey, eps in sorted(dupes.items()):
            show, season, episode = gkey.split("|")
            dupe_rows += '<tr style="background:#1a3a5e"><td colspan="7"><b>' + show + '</b> S' + season.zfill(2) + 'E' + episode.zfill(2) + ' — ' + str(len(eps)) + ' copies</td></tr>'
            for ep in eps:
                h = ep.get("video_height", "")
                codec = ep.get("video_codec", "")
                codec_display = codec_map.get(codec.lower(), codec) if codec else ""
                audio = ep.get("audio", [])
                audio_str = ", ".join(a.get("codec","") + " " + str(a.get("channels","")) + "ch" for a in audio[:3]) if audio else "—"
                subs = ep.get("subtitles", [])
                sub_str = ", ".join(s.get("language","") for s in subs[:4]) if subs else "none"
                path = ep.get("path", "")
                dupe_rows += '<tr><td>' + str(h) + 'p</td><td>' + codec_display + '</td><td>' + audio_str + '</td><td>' + sub_str + '</td><td style="font-size:.7em;color:#888">' + path[-60:] + '</td></tr>'
        html += '<h3 style="margin-top:20px">🔍 Duplicate Episodes (' + str(len(dupes)) + ')</h3>'
        html += '<table><thead><tr><th>Res</th><th>Codec</th><th>Audio</th><th>Subs</th><th>Path</th></tr></thead>'
        html += '<tbody>' + dupe_rows + '</tbody></table>'

    html += '<p style="margin-top:20px"><a href="' + BASE + '/u/' + user + '">← Ratings</a> · <a href="' + BASE + '/library/' + user + '">📚 Library</a></p>'
    html += '</body></html>'
    return html

def render_library_nav(user, active="library"):
    return sub_nav([
        ("library", "📚 Library", f"{BASE}/library/{user}"),
        ("browse", "📖 Browse", f"{BASE}/library/browse/{user}"),
        ("tvshows", "📺 TV Shows", f"{BASE}/tvshows/{user}"),
        ("scraper", "🔍 Scraper", f"{BASE}/scraper/{user}"),
        ("org", "🗂 Organize", f"{BASE}/library/org/{user}"),
        ("confirm", "⚠ Confirm", f"{BASE}/confirm/{user}"),
    ], active)

def render_library(user):
    """Library page with sub-navigation."""
    """Library curation: duplicates, quality comparison, cleanup suggestions."""
    library = load_user_tmm(user)
    titles = load_titles()
    if not library:
        return '<html><body style="background:#1a1a2e;color:#eee;font-family:sans-serif;padding:40px"><h2>No local library synced</h2><a href="' + BASE + '/setup/' + user + '" style="color:#4fc3f7">Setup media servers</a></body></html>'

    # Detect duplicates two ways:
    from collections import defaultdict
    by_title_year = defaultdict(list)
    for iid, info in library.items():
        if iid == "_episodes": continue
        # Handle list entries (multiple files for same IMDB ID)
        entries = info if isinstance(info, list) else [info] if isinstance(info, dict) else []
        for entry in entries:
            if not isinstance(entry, dict): continue
            path = entry.get("path", "")
            if not path: continue
            t = (entry.get("title") or titles.get(iid, {}).get("title") or "").strip()
            y = str(entry.get("year") or titles.get(iid, {}).get("year") or "")
            key = (t.lower() + "|" + y) if t else iid
            by_title_year[key].append((iid, entry))
    by_title = {k: v for k, v in by_title_year.items() if len(v) > 1}

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
        if c:
            cmap = {"hevc": "x265/HEVC", "h265": "x265/HEVC", "h264": "x264/AVC", "avc": "x264/AVC",
                    "mpeg2": "MPEG-2", "mpeg2video": "MPEG-2", "av1": "AV1", "vp9": "VP9", "vc1": "VC-1"}
            codec_dist[cmap.get(c.lower(), c)] += 1

    # Find titles with multiple entries (potential duplicates)
    dupes = [(key, entries) for key, entries in by_title.items()]
    dupes.sort(key=lambda x: len(x[1]), reverse=True)
    dupes.sort(key=lambda x: len(x[1]), reverse=True)

    # Build duplicate cards (column layout)
    dupe_cards = ""
    codec_map = {"hevc": "x265/HEVC", "h265": "x265/HEVC", "h264": "x264/AVC", "avc": "x264/AVC",
                 "mpeg2": "MPEG-2", "mpeg2video": "MPEG-2", "av1": "AV1", "vp9": "VP9", "vc1": "VC-1"}
    for name, entries in dupes[:50]:
        iid_key = entries[0][0]
        t = titles.get(iid_key, {})
        proper_title = t.get("title") or entries[0][1].get("title") or iid_key
        year = t.get("year") or entries[0][1].get("year", "")
        imdb_r = t.get("imdb_rating", "")
        runtime = t.get("runtime", "")
        runtime_str = str(runtime) + " min" if runtime else ""
        best_h = max(int(e[1].get("video_height", 0) or 0) if isinstance(e[1], dict) else 0 for e in entries)

        # Group header
        dupe_cards += '<div style="background:#16213e;border-radius:10px;padding:15px;margin-bottom:15px">'
        dupe_cards += '<h4 style="margin:0 0 10px 0"><a href="https://www.imdb.com/title/' + iid_key + '/" target="_blank">' + proper_title + '</a> (' + str(year) + ') — IMDB ' + str(imdb_r) + ' — ' + runtime_str + ' — ' + str(len(entries)) + ' copies</h4>'

        # Columns for each copy
        dupe_cards += '<div style="display:grid;grid-template-columns:repeat(' + str(min(len(entries), 4)) + ',1fr);gap:12px">'
        for iid, info in entries:
            h = info.get("video_height") or info.get("quality", "")
            codec = info.get("video_codec", "")
            codec_display = codec_map.get(codec.lower(), codec) if codec else ""
            audio = info.get("audio", [])
            audio_str = "<br>".join(a.get("codec","") + " " + str(a.get("channels","")) + "ch " + a.get("language","") for a in audio[:3]) if audio else "—"
            subs = info.get("subtitles", [])
            sub_str = ", ".join(s.get("language","") for s in subs[:5]) if subs else "none"
            path = info.get("path", "")
            raw_size = info.get("file_size", 0) or 0
            if raw_size > 1073741824: size_str = str(round(raw_size / 1073741824, 1)) + " GB"
            elif raw_size > 1048576: size_str = str(round(raw_size / 1048576)) + " MB"
            elif raw_size: size_str = str(raw_size)
            else: size_str = "—"

            # Thumbnail
            thumb = info.get("thumbnail", "")
            thumb_html = '<img src="' + BASE + thumb + '" style="width:100%;border-radius:4px;margin-bottom:8px">' if thumb else '<div style="background:#333;height:80px;border-radius:4px;margin-bottom:8px;display:flex;align-items:center;justify-content:center;color:#666">no preview</div>'

            # Keep suggestion
            h_val = int(h or 0)
            my_size = raw_size
            my_codec = codec.lower()
            dominated = False
            for _, other in entries:
                if other is info: continue
                o_h = int(other.get("video_height", 0) or 0)
                o_size = other.get("file_size", 0) or 0
                o_codec = (other.get("video_codec") or "").lower()
                if o_h >= h_val and o_codec in ("hevc","h265","x265") and my_codec in ("h264","avc","x264"):
                    if o_size and my_size and o_size < my_size * 0.75: dominated = True
                if o_h >= h_val and o_codec == my_codec and o_size and my_size and o_size < my_size * 0.9: dominated = True
            is_best = h_val == best_h and best_h > 0 and not dominated
            border = "2px solid #2d7" if is_best else "2px solid #d72" if dominated else "1px solid #444"
            badge = '<div style="color:#2d7;font-weight:bold">✅ KEEP</div>' if is_best else '<div style="color:#d72;font-weight:bold">❌ REMOVE</div>' if dominated else '<div style="color:#f90">⚖️ REVIEW</div>'

            # Folder link
            folder = path.rsplit("/", 1)[0] if "/" in path else path
            smb_path = folder.replace("nfs://192.168.0.235/volume1", "//zeus")
            open_btn = '<a href="file:///' + smb_path.replace("\\", "/") + '" style="font-size:.8em">📂 Open folder</a>' if path else ""

            dupe_cards += '<div style="background:#1a1a2e;border-radius:8px;padding:10px;border:' + border + '">'
            dupe_cards += thumb_html
            dupe_cards += '<div style="font-size:1.2em;font-weight:bold;margin-bottom:4px">' + str(h) + 'p ' + codec_display + '</div>'
            dupe_cards += '<div style="font-size:.9em;color:#aaa">Size: ' + size_str + '</div>'
            dupe_cards += '<div style="font-size:.85em;color:#888;margin:4px 0">Audio: ' + audio_str + '</div>'
            dupe_cards += '<div style="font-size:.85em;color:#888">Subs: ' + sub_str + '</div>'
            dupe_cards += badge
            dupe_cards += '<div style="margin-top:6px">' + open_btn + '</div>'
            thumb_img = ''
            if entry.get("thumbnail"):
                thumb_img = '<img src="data:image/jpeg;base64,' + entry["thumbnail"] + '" style="max-width:280px;border-radius:4px;margin-top:6px">'
            dupe_cards += thumb_img
            dupe_cards += '<div style="font-size:.75em;color:#8ab;margin-top:4px;word-break:break-all;font-family:monospace">' + path.split("/")[-2] + '/' + path.split("/")[-1] + '</div>'
            dupe_cards += '</div>'

        dupe_cards += '</div></div>'

    # Quality bars
    q_bars = ""
    for q in ["4K", "1080p", "720p", "SD"]:
        c = quality_dist.get(q, 0)
        if c: q_bars += '<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:50px;text-align:right">' + q + '</span><div style="background:#4fc3f7;height:18px;width:' + str(min(c/max(quality_dist.values())*300, 300)) + 'px;border-radius:3px"></div><span style="color:#888">' + str(c) + '</span></div>'

    codec_bars = ""
    for c, n in sorted(codec_dist.items(), key=lambda x: x[1], reverse=True)[:8]:
        codec_bars += '<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:60px;text-align:right;font-size:.85em">' + c + '</span><div style="background:#4fc3f7;height:16px;width:' + str(min(n/max(codec_dist.values())*250, 250)) + 'px;border-radius:3px"></div><span style="color:#888">' + str(n) + '</span></div>'

    html = page_head(f"Library - {user}")
    html += nav_bar("library", user)
    html += render_library_nav(user, "library")
    html += '<div class="page">'
    html += '<h2>📚 Library Curation</h2>'

    # Comprehensive status
    eps = library.get("_episodes", {})
    ep_count = sum(1 for v in eps.values() if isinstance(v, dict))
    sized = sum(1 for iid, info in library.items() if isinstance(info, dict) and not iid.startswith("_") and (info.get("file_size") or info.get("size")))
    hashed = sum(1 for iid, info in library.items() if isinstance(info, dict) and not iid.startswith("_") and info.get("file_hash"))
    with_subs_count = sum(1 for iid, info in library.items() if isinstance(info, dict) and not iid.startswith("_") and info.get("subtitles"))
    scraped = sum(1 for iid in library if not iid.startswith("_") and isinstance(library[iid], dict) and iid in titles and titles[iid].get("title"))
    nfo_matched = sum(1 for iid, info in library.items() if isinstance(info, dict) and info.get("nfo_matched"))
    def pct(n, t): return f"{n*100//t}%" if t else "0%"
    def bar(n, t, color="#4fc3f7"):
        w = n*100//t if t else 0
        return f'<div style="background:#333;border-radius:3px;height:8px;margin-top:4px"><div style="background:{color};height:8px;width:{w}%;border-radius:3px"></div></div>'

    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.2em">' + str(total) + '</div>movies<br><small style="color:var(--muted)">' + str(ep_count) + ' TV episodes</small></div>'
    html += '<div class="card"><b>Scraped</b> ' + str(scraped) + '/' + str(total) + ' (' + pct(scraped,total) + ')' + bar(scraped,total) + '</div>'
    html += '<div class="card"><b>Sized</b> ' + str(sized) + '/' + str(total) + ' (' + pct(sized,total) + ')' + bar(sized,total,"#f90") + '</div>'
    html += '<div class="card"><b>Hashed</b> ' + str(hashed) + '/' + str(total) + ' (' + pct(hashed,total) + ')' + bar(hashed,total,"#a6e") + '</div>'
    html += '<div class="card"><b>Subtitles</b> ' + str(with_subs_count) + '/' + str(total) + ' (' + pct(with_subs_count,total) + ')' + bar(with_subs_count,total,"#4c8") + '</div>'
    html += '<div class="card"><b>Media info</b> ' + str(has_video) + '/' + str(total) + ' (' + pct(has_video,total) + ')' + bar(has_video,total,"#48f") + '</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.2em;color:#f90">' + str(len(dupes)) + '</div>duplicates</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.2em;color:#d72">' + str(no_subs) + '</div>missing subs</div>'
    html += '</div>'

    # Agent & task performance
    queue = load_task_queue()
    agent = load_agent_status()
    t_done = [t for t in queue if t["status"] == "done" and t.get("completed")]
    t_pending = [t for t in queue if t["status"] == "pending"]
    t_by_type = {}
    for t in t_pending: t_by_type[t["type"]] = t_by_type.get(t["type"], 0) + 1
    agent_ver = agent.get("agent_version", "?")
    agent_uptime = agent.get("uptime", 0)
    uptime_str = f"{agent_uptime//3600}h {(agent_uptime%3600)//60}m" if agent_uptime > 3600 else f"{agent_uptime//60}m"
    last_seen = agent.get("last_seen", "never")
    last_task = agent.get("last_activity", {}).get("task", "-")
    bg_task = agent.get("last_activity", {}).get("bg_task", agent.get("bg_task", ""))
    errors = agent.get("last_activity", {}).get("errors", 0)
    # ETA: avg time per done task
    eta_str = "-"
    if len(t_done) >= 2:
        times = sorted(t["completed"] for t in t_done)
        try:
            from datetime import datetime
            first = datetime.strptime(times[0], "%Y-%m-%d %H:%M:%S")
            last = datetime.strptime(times[-1], "%Y-%m-%d %H:%M:%S")
            elapsed = (last - first).total_seconds()
            if elapsed > 0 and len(t_done) > 1:
                rate = len(t_done) / elapsed  # tasks/sec
                if rate > 0 and t_pending:
                    eta_sec = len(t_pending) / rate
                    eta_str = f"{eta_sec//3600:.0f}h {(eta_sec%3600)//60:.0f}m" if eta_sec > 3600 else f"{eta_sec//60:.0f}m"
        except: pass

    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card"><b>🤖 Agent</b><br>'
    html += f'v{agent_ver} · up {uptime_str}<br>'
    html += f'<small style="color:var(--muted)">Last seen: {last_seen}</small><br>'
    html += f'<small>Current: {last_task}</small>'
    if bg_task: html += f'<br><small>Background: {bg_task}</small>'
    if errors: html += f'<br><small style="color:#d72">Errors: {errors}</small>'
    html += '</div>'
    html += '<div class="card"><b>📋 Task Queue</b><br>'
    html += f'<span style="font-size:1.5em">{len(t_done)}</span> done · <span style="font-size:1.5em">{len(t_pending)}</span> pending<br>'
    for ttype, cnt in sorted(t_by_type.items(), key=lambda x: x[1], reverse=True):
        html += f'<small>{ttype}: {cnt}</small><br>'
    html += f'<small style="color:var(--muted)">ETA: {eta_str}</small>'
    html += '</div>'
    html += '</div>'

    # Quality & codec breakdown
    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card"><h3>Resolution</h3>' + q_bars + '</div>'
    html += '<div class="card"><h3>Codecs</h3>' + codec_bars + '</div>'
    html += '</div>'

    # Duplicates table
    if dupes:
        html += '<h3>🔍 Potential Duplicates (' + str(len(dupes)) + ' titles)</h3>'
        html += '<p style="color:#888;font-size:.85em">✅ KEEP = best quality · ❌ REMOVE = dominated · ⚖️ REVIEW = manual decision</p>'
        html += dupe_cards

    # Save Space: titles furthest from taste profile
    ratings = load_user_ratings(user)
    profile = build_taste_profile(ratings, titles, user)
    space_candidates = []
    unenriched_count = 0
    for iid, info in library.items():
        if iid.startswith("_") or not isinstance(info, dict): continue
        t = titles.get(iid, {})
        if not t.get("title") or iid in ratings: continue
        # Only include titles with enough data to score meaningfully
        if not t.get("keywords") and not t.get("genres"):
            unenriched_count += 1
            continue
        size = info.get("file_size") or info.get("size") or 0
        score = score_title(t, profile)
        space_candidates.append((iid, t, info, score, size or 0))
    space_candidates.sort(key=lambda x: x[3])  # lowest score first
    if space_candidates:
        sized = [(x[4]) for x in space_candidates[:50] if x[4]]
        total_save = sum(sized) if sized else 0
        html += '<h3 style="margin-top:30px">💾 Save Space — furthest from your taste</h3>'
        if unenriched_count:
            html += '<p style="color:var(--warn);font-size:.85em">⚠ ' + str(unenriched_count) + ' titles lack keywords/genres — enrichment needed before scoring. These are excluded below.</p>'
        html += '<p style="color:#888;font-size:.85em">Unrated titles with the lowest taste match. Potential savings: <b>' + f"{total_save/1073741824:.1f}" + ' GB</b> from bottom 50.</p>'
        html += '<table><thead><tr><th>Title</th><th>Year</th><th>IMDB</th><th>Match</th><th>Size</th><th>Source</th></tr></thead><tbody>'
        for iid, t, info, score, size in space_candidates[:50]:
            size_str = f"{size/1073741824:.1f} GB" if size > 1073741824 else f"{size/1048576:.0f} MB"
            vsrc = detect_video_source(info.get("path", ""))
            vsrc_icon = SOURCE_ICONS.get(vsrc, "")
            html += f'<tr><td><a href="{BASE}/title/{iid}">{t.get("title","?")}</a></td><td>{t.get("year","")}</td><td>{t.get("imdb_rating","")}</td><td style="color:#d72">{score:.1f}</td><td>{size_str}</td><td>{vsrc_icon} {vsrc}</td></tr>'
        html += '</tbody></table>'

    html += '</div>' + page_foot()
    return html

def render_scraper(user):
    """Movie scraper: show unmatched files, allow manual matching."""
    library = load_user_tmm(user)
    titles = load_titles()
    
    # Find unmatched: in library but not in titles store
    unmatched = []
    matched_count = 0
    for iid, info in library.items():
        if iid.startswith("_") or not isinstance(info, dict): continue
        if iid in titles and titles[iid].get("title"):
            matched_count += 1
            continue
        parsed = parse_movie_filename(info.get("path", "")) if info.get("path") else {"title": "", "year": ""}
        unmatched.append((iid, info, parsed))
    # Add fully-watched TV shows not linked to any tt ID
    eps = library.get("_episodes", {})
    show_stats = {}
    for ep in eps.values():
        if not isinstance(ep, dict): continue
        show = ep.get("showtitle", "")
        if not show: continue
        show_stats.setdefault(show, [0, 0])
        show_stats[show][0] += 1
        if ep.get("playcount", 0) > 0: show_stats[show][1] += 1
    title_to_iid = {t.get("title", "").lower(): iid for iid, t in titles.items() if t.get("type") in ("tvSeries", "tvMiniSeries", "tv")}
    for show_name, (total, watched) in show_stats.items():
        if show_name.lower() not in title_to_iid:
            pct = watched * 100 // total if total else 0
            unmatched.append(("_show_" + show_name, {"path": show_name, "show": True}, {"title": show_name, "year": "", "quality": f"📺 {watched}/{total} ({pct}%)", "is_3d": None}))
    
    # Try auto-matching unmatched against IMDB dataset
    imdb_by_title = {}
    if hasattr(parse_movie_filename, '__self__') or True:
        for tid, t in (_imdb_cache or {}).items():
            key = (t.get("title","").lower(), str(t.get("year","")))
            imdb_by_title.setdefault(key, []).append((tid, t))

    rows = ""
    for iid, info, parsed in unmatched[:100]:
        path = info.get("path", "")
        short_path = path.split("/")[-1] if "/" in path else path.split("\\")[-1] if "\\" in path else path
        title_guess = parsed.get("title", "")
        year_guess = parsed.get("year", "")
        quality = parsed.get("quality", "")
        is_3d = parsed.get("is_3d", "")
        # Auto-proposal from IMDB dataset
        proposal = ""
        key = (title_guess.lower(), year_guess)
        candidates = imdb_by_title.get(key, [])
        if not candidates and not year_guess:
            candidates = [(tid, t) for tid, t in (_imdb_cache or {}).items() if t.get("title","").lower() == title_guess.lower() and t.get("type") in ("tvSeries", "tvMiniSeries")][:1]
        if candidates:
            best = candidates[0]
            proposal = f'<a href="{BASE}/scraper-apply/{user}/{iid}/{best[0]}/imdb" class="btn" style="background:#2a5" title="{best[0]}">✅ {best[1].get("title","")} ({best[1].get("year","")})</a>'
        rows += f'<tr><td class="x" style="max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="{path}">{short_path}</td>'
        rows += f'<td>{title_guess}</td><td>{year_guess}</td><td>{quality} {is_3d or ""}</td>'
        rows += f'<td>{proposal}</td>'
        rows += f'<td><form method="GET" action="{BASE}/scraper-match/{user}/{iid}" style="display:flex;gap:4px"><input name="q" value="{title_guess}" style="width:150px;padding:4px"><button type="submit" class="btn">🔍</button></form></td>'
        lookup_q = urllib.parse.quote(f'{title_guess} {year_guess} movie'.strip())
        rows += f'<td style="white-space:nowrap"><a href="https://www.imdb.com/find/?q={lookup_q}" target="_blank" title="IMDB">🔗</a> <a href="https://www.google.com/search?q={lookup_q}+site:imdb.com" target="_blank" title="Google">🌐</a></td></tr>'
    
    html = page_head(f"Scraper - {user}")
    html += nav_bar("library", user)
    html += sub_nav([
        ("library", "📚 Library", f"{BASE}/library/{user}"),
        ("browse", "📖 Browse", f"{BASE}/library/browse/{user}"),
        ("tvshows", "📺 TV Shows", f"{BASE}/tvshows/{user}"),
        ("scraper", "🔍 Scraper", f"{BASE}/scraper/{user}"),
    ], "scraper")
    html += '<div class="page">'
    html += f'<div class="grid"><div class="card card-stat"><div class="num">{matched_count}</div>matched</div>'
    html += f'<div class="card card-stat"><div class="num" style="color:var(--warn)">{len(unmatched)}</div>unmatched</div></div>'
    html += '<table><thead><tr><th onclick="sortTable(0)">File</th><th onclick="sortTable(1)">Title (guess)</th><th onclick="sortTable(2)">Year</th><th onclick="sortTable(3)">Quality</th><th>Proposal</th><th>Search</th><th>Lookup</th></tr></thead>'
    html += '<tbody>' + rows + '</tbody></table>'
    html += '</div>' + page_foot()
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
{leaving_html}<table><thead><tr><th></th><th onclick="sortTable(1)">Title</th><th onclick="sortTable(2)">Year</th><th onclick="sortTable(3)">TMDB</th><th>On</th><th onclick="sortTable(5)">Type</th></tr></thead>
<tbody>{rows}</tbody></table></div></body></html>"""

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

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except Exception as e:
            print(f"[REQUEST ERROR] {e}")
            import traceback; traceback.print_exc()

    def do_GET(self):
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
        p = "/" + "/".join(parts)
        user = self._user(parts)

        # API routes first (agent communication)
        if p.startswith('/api/agent_status'):
            import base64
            encoded = qs.get('s', [''])[0]
            if encoded:
                try: save_agent_status(json.loads(base64.b64decode(encoded).decode()))
                except: pass
            self._json({'status': 'ok'})
            return
        if p.startswith('/api/tasks/complete/'):
            import base64
            task_id = parts[-1]
            encoded = qs.get('r', [''])[0]
            result = None
            if encoded:
                try: result = json.loads(base64.b64decode(encoded).decode())
                except: pass
            complete_task(task_id, result)
            self._json({'status': 'ok'})
            return
        if p == '/api/exec_results':
            self._json(_exec_results)
            return
        if p == '/api/tasks':
            self._json({'tasks': get_pending_tasks()})
            return
        if p == '/api':
            self._json({'titles': len(load_titles()), 'users': {u: len(load_user_ratings(u)) for u in list_users()}})
            return


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
        elif p.startswith("/subs/request/"):
            # Human clicked find-subs on a specific title
            iid = parts[-1]
            u = self._user(parts)
            request_file_hash(u, iid)
            request_subs(u, iid)
            self._redirect(f"{BASE}/subs/{iid}")
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
            self._page(render_new_on_streaming(), "discover", user)
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
                self._page(render_compare(u1, u2), "social")
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

        elif p == "/feed":
            feed = get_activity_feed()
            titles = load_titles()
            rows = ""
            for f in feed:
                poster = titles.get(f["id"],{}).get("poster","")
                img = '<img src="' + poster + '" height="40" style="border-radius:4px">' if poster else ""
                rows += '<tr><td>' + img + '</td><td><b>' + f["user"] + '</b> rated <a href="https://www.imdb.com/title/' + f["id"] + '/" target="_blank">' + f["title"] + '</a></td><td style="font-weight:bold">' + str(f["rating"]) + '/10</td><td style="color:#888">' + f["date"] + '</td></tr>'
            self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Activity Feed</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}}table{{border-collapse:collapse;width:100%}}
td{{padding:8px;border-bottom:1px solid #333}}a{{color:#4fc3f7;text-decoration:none}}</style></head>
<body><h2>📡 Activity Feed</h2><table>{rows}</table>
<p style="margin-top:15px"><a href="{BASE}/">← Back</a></p></body></html>""")
            return
        elif p.startswith("/alerts/"):
            u = parts[-1]
            alerts = get_available_alerts(u)
            rows = ""
            for a in alerts:
                provs = " ".join(PROVIDER_ICONS.get(p,"") for p in a["providers"])
                rows += '<tr><td><a href="https://www.imdb.com/title/' + a["id"] + '/" target="_blank">' + a["title"] + '</a></td><td>' + provs + '</td></tr>'
            self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Alerts</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}}table{{border-collapse:collapse;width:100%}}
td{{padding:8px;border-bottom:1px solid #333}}a{{color:#4fc3f7;text-decoration:none}}</style></head>
<body><h2>🔔 Watchlist Alerts — {u}</h2>
<p style="color:#888">Titles from your watchlist now available on your streaming services</p>
<table>{rows if rows else "<tr><td>No watchlisted titles currently available</td></tr>"}</table>
<p style="margin-top:15px"><a href="{BASE}/u/{u}">← Back</a></p></body></html>""")
            return
        elif p.startswith("/ai-friend/"):
            u = parts[-1]
            titles = load_titles()
            ratings = load_user_ratings(u)
            library = load_user_tmm(u)
            profile = build_taste_profile(ratings, titles, u)

            # Hidden gems: in library, unrated, high taste score
            gems = []
            # Why do I have this: low taste + low IMDB
            why = []
            # Guilty pleasures: you loved it, critics hated it
            guilty = []
            # Blind spots: top profile directors/actors with few titles
            for iid, info in library.items():
                if iid.startswith("_") or not isinstance(info, dict): continue
                t = titles.get(iid, {})
                if not t.get("title"): continue
                score = score_title(t, profile) if (t.get("keywords") or t.get("genres")) else 0
                imdb_r = t.get("imdb_rating", 0) or 0
                if iid not in ratings and score > 50 and imdb_r >= 6.5:
                    gems.append((iid, t, score, imdb_r))
                if iid not in ratings and score < 2 and imdb_r and imdb_r < 5.5:
                    why.append((iid, t, score, imdb_r))
                if iid in ratings:
                    my_r = ratings[iid].get("rating", 0)
                    rt = t.get("rotten_tomatoes", "")
                    rt_val = int(str(rt).replace("%","")) if rt and "%" in str(rt) else 0
                    if my_r >= 8 and rt_val > 0 and rt_val < 35:
                        guilty.append((iid, t, my_r, rt_val))

            gems.sort(key=lambda x: x[2], reverse=True)
            why.sort(key=lambda x: x[3])
            guilty.sort(key=lambda x: x[3])

            # Top directors you love but have < 3 titles from
            dir_counts = {}
            for iid, r in ratings.items():
                if r["rating"] >= 8:
                    for d in (titles.get(iid, {}).get("directors") or "").split(","):
                        d = d.strip()
                        if d: dir_counts[d] = dir_counts.get(d, 0) + 1
            blind_dirs = [(d, c) for d, c in sorted(dir_counts.items(), key=lambda x: x[1], reverse=True) if c >= 3][:10]

            html = page_head(f"AI Friend - {u}")
            html += nav_bar("discover", u)
            html += '<div class="page">'
            html += '<h2>🤖 My AI Friend Recommends</h2>'
            html += '<p style="color:var(--muted)">Based on your ' + str(len(ratings)) + ' ratings and ' + str(len(profile["keywords"])) + ' taste keywords.</p>'

            # Hidden Gems
            html += '<h3>💎 Hidden Gems in Your Library</h3>'
            html += '<p style="color:var(--muted);font-size:.85em">You own these but have not rated them. Your taste profile says you will love them.</p>'
            html += '<div style="display:grid;grid-template-columns:repeat(auto-fill,minmax(160px,1fr));gap:12px">'
            for iid, t, score, imdb_r in gems[:20]:
                poster = f'<img src="{t.get("poster","")}" style="width:100%;border-radius:6px">' if t.get("poster") else ""
                html += f'<a href="{BASE}/title/{iid}" style="text-decoration:none;color:var(--fg)"><div class="card" style="padding:8px;text-align:center">{poster}<div style="font-size:.85em;margin-top:4px">{t["title"]}</div><div style="font-size:.75em;color:var(--muted)">{t.get("year","")} · IMDB {imdb_r}</div><div style="font-size:.75em;color:#4c8">Match: {score:.0f}</div></div></a>'
            html += '</div>'

            # Why do I have this
            if why:
                html += '<h3 style="margin-top:30px">🤔 Why Do I Have This?</h3>'
                html += '<p style="color:var(--muted);font-size:.85em">Low taste match AND low IMDB rating. Prime candidates for cleanup.</p>'
                html += '<table><thead><tr><th>Title</th><th>Year</th><th>IMDB</th><th>Match</th></tr></thead><tbody>'
                for iid, t, score, imdb_r in why[:20]:
                    html += f'<tr><td><a href="{BASE}/title/{iid}">{t["title"]}</a></td><td>{t.get("year","")}</td><td style="color:#d72">{imdb_r}</td><td>{score:.1f}</td></tr>'
                html += '</tbody></table>'

            # Guilty Pleasures
            if guilty:
                html += '<h3 style="margin-top:30px">😈 Guilty Pleasures</h3>'
                html += '<p style="color:var(--muted);font-size:.85em">You rated 8+ but critics gave < 35% on Rotten Tomatoes. Own it!</p>'
                html += '<table><thead><tr><th>Title</th><th>Your ★</th><th>🍅</th></tr></thead><tbody>'
                for iid, t, my_r, rt_val in guilty[:15]:
                    html += f'<tr><td><a href="{BASE}/title/{iid}">{t["title"]}</a></td><td style="color:#4c8">{my_r}</td><td style="color:#d72">{rt_val}%</td></tr>'
                html += '</tbody></table>'

            # Directors you love
            if blind_dirs:
                html += '<h3 style="margin-top:30px">🎬 Directors You Love</h3>'
                html += '<p style="color:var(--muted);font-size:.85em">Directors with 3+ titles you rated 8+.</p>'
                html += '<div style="display:flex;flex-wrap:wrap;gap:8px">'
                for d, c in blind_dirs:
                    html += f'<span class="card" style="padding:6px 12px;font-size:.9em">{d} <b>({c})</b></span>'
                html += '</div>'

            html += '</div>' + page_foot()
            self._page(html, "discover", u)
            return
        elif p.startswith("/library/browse/"):
            u = parts[-1]
            library = load_user_tmm(u)
            titles = load_titles()
            ratings = load_user_ratings(u)
            page_num = int(qs.get("p", ["1"])[0])
            search = qs.get("q", [""])[0].lower()
            sort_by = qs.get("sort", ["title"])[0]
            items = []
            for iid, info in library.items():
                if iid.startswith("_") or not isinstance(info, dict): continue
                t = titles.get(iid, {})
                title = t.get("title", "") or info.get("title", "")
                if search and search not in title.lower() and search not in info.get("path","").lower(): continue
                size = info.get("file_size") or info.get("size") or 0
                vsrc = detect_video_source(info.get("path", ""))
                r = ratings.get(iid, {}).get("rating", "")
                items.append({"iid": iid, "title": title, "year": t.get("year",""),
                    "quality": info.get("quality","") or (str(info.get("video_height",""))+"p" if info.get("video_height") else ""),
                    "codec": info.get("video_codec",""), "size": size,
                    "size_str": f"{size/1073741824:.1f}GB" if size > 1e9 else f"{size//1048576}MB" if size else "-",
                    "vsrc": SOURCE_ICONS.get(vsrc,""), "rating": r,
                    "subs": "yes" if info.get("subtitles") else "no",
                    "path": info.get("path","").split("/")[-1]})
            if sort_by == "size": items.sort(key=lambda x: x["size"], reverse=True)
            elif sort_by == "year": items.sort(key=lambda x: str(x["year"]), reverse=True)
            elif sort_by == "rating": items.sort(key=lambda x: x["rating"] or 0, reverse=True)
            else: items.sort(key=lambda x: x["title"].lower())
            per_page = 100
            total_pages = max(1, (len(items) + per_page - 1) // per_page)
            page_items = items[(page_num-1)*per_page : page_num*per_page]
            rows = ""
            for it in page_items:
                rc = f' style="color:#4a4"' if it["rating"] else ""
                rows += f'<tr><td><a href="{BASE}/title/{it["iid"]}">{it["title"]}</a></td><td>{it["year"]}</td><td>{it["quality"]}</td><td>{it["codec"]}</td><td>{it["size_str"]}</td><td>{it["vsrc"]}</td><td{rc}>{it["rating"] or "-"}</td><td>{it["subs"]}</td></tr>'
            pager = ""
            if total_pages > 1:
                for pg in range(1, total_pages+1):
                    if pg == page_num: pager += f' <b>[{pg}]</b>'
                    else: pager += f' <a href="{BASE}/library/browse/{u}?p={pg}&q={search}&sort={sort_by}">{pg}</a>'
            html = page_head(f"Browse Library - {u}")
            html += nav_bar("library", u)
            html += render_library_nav(u, "browse")
            html += '<div class="page">'
            html += f'<h2>📖 Browse Library — {len(items)} titles</h2>'
            html += f'<div style="display:flex;gap:10px;margin-bottom:15px;flex-wrap:wrap"><form><input name="q" value="{search}" placeholder="Search..." style="padding:6px;border-radius:4px;border:1px solid var(--border);background:var(--card);color:var(--fg)"><button class="btn">🔍</button></form>'
            html += f'<span style="color:var(--muted)">Sort: <a href="{BASE}/library/browse/{u}?sort=title&q={search}">Title</a> · <a href="{BASE}/library/browse/{u}?sort=size&q={search}">Size</a> · <a href="{BASE}/library/browse/{u}?sort=year&q={search}">Year</a> · <a href="{BASE}/library/browse/{u}?sort=rating&q={search}">Rating</a></span></div>'
            html += '<table><thead><tr><th onclick="sortTable(0)">Title</th><th onclick="sortTable(1)">Year</th><th>Quality</th><th>Codec</th><th onclick="sortTable(4)">Size</th><th>Src</th><th>★</th><th>Subs</th></tr></thead>'
            html += '<tbody>' + rows + '</tbody></table>'
            html += '<div style="margin-top:15px;text-align:center">' + pager + '</div>'
            html += '</div>' + page_foot()
            self._page(html, "library", u)
            return
        elif p.startswith("/library/org/"):
            u = parts[-1]
            analysis = analyze_library(u)
            # Build drive table
            drive_rows = ""
            for path, info in analysis["drives"].items():
                size_gb = info["size"] / (1024**3) if info["size"] else 0
                drive_rows += "<tr><td>" + path[:60] + "</td><td>" + str(info["count"]) + "</td><td>" + f"{size_gb:.1f} GB" + "</td></tr>"
            # Quality bars
            max_q = max(analysis["quality"].values()) if analysis["quality"] else 1
            q_bars = ""
            for q in ["4K", "1080p", "720p", "SD"]:
                c = analysis["quality"].get(q, 0)
                if c: q_bars += '<div style="display:flex;gap:8px;align-items:center;margin:2px"><span style="width:50px;text-align:right">' + q + '</span><div style="background:var(--accent);height:16px;width:' + str(min(int(c/max_q*200),200)) + 'px;border-radius:3px"></div> ' + str(c) + '</div>'
            html = page_head("Library Organization")
            html += nav_bar("library", u)
            html += render_library_nav(u, "org")
            html += '<div class="page">'
            html += '<div class="grid">'
            html += '<div class="card card-stat"><div class="num">' + str(analysis["total"]) + '</div>titles</div>'
            html += '<div class="card card-stat"><div class="num" style="color:var(--warn)">' + str(analysis["orphans"]) + '</div>unmatched</div>'
            html += '<div class="card card-stat"><div class="num" style="color:var(--warn)">' + str(analysis["duplicates"]) + '</div>duplicate sizes</div>'
            html += '<div class="card card-stat"><div class="num">' + str(analysis["no_size"]) + '</div>unsized</div>'
            html += '<div class="card card-stat"><div class="num">' + str(analysis["no_subs"]) + '</div>no subs</div>'
            html += '</div>'
            html += '<div class="grid"><div class="card"><h3>Quality</h3>' + q_bars + '</div>'
            html += '<div class="card"><h3>Drives</h3><table><tr><th>Path</th><th>Files</th><th>Size</th></tr>' + drive_rows + '</table></div></div>'
            if analysis["dupe_details"]:
                dupe_rows = ""
                for size, entries in analysis["dupe_details"].items():
                    for iid, path in entries:
                        dupe_rows += "<tr><td>" + iid + "</td><td>" + path + "</td><td>" + str(int(int(size)/(1024*1024))) + " MB</td></tr>"
                html += '<div class="card" style="margin-top:15px"><h3>Potential Duplicates (same file size)</h3><table><tr><th>ID</th><th>Path</th><th>Size</th></tr>' + dupe_rows + '</table></div>'
            html += '</div>' + page_foot()
            self._page(html, "library", u)
            return
        elif p.startswith("/import/streaming/"):
            u = parts[-1]
            html = page_head(f"Import Streaming History - {u}")
            html += nav_bar("setup", u)
            html += '<div class="page"><h2>Import Streaming History</h2>'
            html += '<div class="grid">'
            for svc, desc, fmt in [
                ("netflix", "Netflix", "Download from netflix.com/account → Profile → Viewing Activity → Download all"),
                ("prime", "Amazon Prime", "Download from amazon.com/gp/video/settings → Download watch history"),
                ("disney", "Disney+", "Request data export from Disney+ account settings"),
                ("hbo", "HBO Max", "Request data export from HBO Max account settings"),
                ("letterboxd", "Letterboxd", "Export from letterboxd.com/settings/data/"),
            ]:
                html += f'<div class="card"><h3>{svc.title()}</h3><p style="color:var(--muted);font-size:.85em">{desc}</p>'
                html += f'<form method="POST" action="{BASE}/import/streaming/{u}/{svc}" enctype="multipart/form-data">'
                html += f'<input type="file" name="file" accept=".csv,.json,.txt" style="margin:8px 0">'
                html += f'<button type="submit" class="btn btn-primary">Import</button></form></div>'
            html += '</div></div>' + page_foot()
            self._page(html, "setup", u)
            return
        elif p.startswith("/scraper/"):
            u = parts[-1]
            self._page(render_scraper(u), "library", u)
            return
        elif p.startswith("/scraper-match/"):
            # /scraper-match/<user>/<imdb_id>?q=search+term
            u = parts[-2] if len(parts) >= 3 else self._user(parts)
            iid = parts[-1]
            query = qs.get("q", [""])[0]
            if query and TMDB_KEY:
                results = api_get(f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_KEY}&query={urllib.parse.quote(query)}")
                matches = []
                for r in (results or {}).get("results", [])[:8]:
                    title = r.get("title") or r.get("name", "")
                    year = (r.get("release_date") or r.get("first_air_date") or "")[:4]
                    poster = f"https://image.tmdb.org/t/p/w92{r['poster_path']}" if r.get("poster_path") else ""
                    matches.append(f'<a href="{BASE}/scraper-apply/{u}/{iid}/{r["id"]}/{r.get("media_type","movie")}" style="display:flex;gap:8px;align-items:center;padding:8px;background:var(--card);border-radius:6px;margin:4px 0;text-decoration:none;color:var(--fg)"><img src="{poster}" height="60">{title} ({year})</a>')
                self._page(page_head("Match") + nav_bar("library", u) + '<div class="page"><h3>Select match</h3>' + "".join(matches) + f'<p><a href="{BASE}/scraper/{u}">← Back</a></p></div>' + page_foot(), "library", u)
            else:
                self._redirect(f"{BASE}/scraper/{u}")
            return
        elif p.startswith("/scraper-apply/"):
            # /scraper-apply/<user>/<library_imdb_id>/<tmdb_id>/<type>
            u = parts[-4]
            old_iid = parts[-3]
            new_id = parts[-2]
            kind = parts[-1]
            # Direct IMDB match (from dataset proposal)
            if kind == "imdb":
                library = load_user_tmm(u)
                if old_iid in library:
                    info = library.pop(old_iid)
                    info["matched"] = True
                    library[new_id] = info
                    save_user_tmm(u, library)
                self._redirect(f"{BASE}/scraper/{u}")
                return
            tmdb_id = int(new_id)
            # Look up IMDB ID from TMDB
            ext = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/external_ids?api_key={TMDB_KEY}")
            if ext and ext.get("imdb_id"):
                new_iid = ext["imdb_id"]
                library = load_user_tmm(u)
                if old_iid in library:
                    info = library.pop(old_iid)
                    info["matched"] = True
                    info["tmdb_id"] = tmdb_id
                    library[new_iid] = info
                    save_user_tmm(u, library)
                    # Also add to titles store
                    titles = load_titles()
                    if new_iid not in titles:
                        detail = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}?api_key={TMDB_KEY}")
                        if detail:
                            genres = ", ".join(g["name"] for g in detail.get("genres", []))
                            titles[new_iid] = {"title": detail.get("title") or detail.get("name",""),
                                "year": (detail.get("release_date") or detail.get("first_air_date",""))[:4],
                                "tmdb_id": tmdb_id, "genres": genres, "type": kind,
                                "overview": detail.get("overview",""),
                                "poster": f"https://image.tmdb.org/t/p/w185{detail['poster_path']}" if detail.get("poster_path") else ""}
                            save_titles(titles)
            self._redirect(f"{BASE}/scraper/{u}")
            return
        elif p.startswith("/unrated/"):
            u = parts[-1]
            titles = load_titles()
            ratings = load_user_ratings(u)
            library = load_user_tmm(u)
            history = load_user_history(u) if hasattr(__import__('builtins'), '__builtins__') else {}
            # Combine watched sources
            watched_ids = set()
            for iid, info in library.items():
                if iid.startswith("_") or not isinstance(info, dict): continue
                if info.get("playcount", 0) > 0 or info.get("watched"): watched_ids.add(iid)
            for iid in (history or {}):
                watched_ids.add(iid)
            unrated = watched_ids - set(ratings.keys())
            # Build rows
            rows = ""
            for iid in sorted(unrated, key=lambda x: titles.get(x, {}).get("title", x)):
                t = titles.get(iid, {})
                lib_info = library.get(iid, {})
                # Fallback: use library title if titles store is empty
                if not t.get("title") and lib_info.get("title"):
                    t = {"title": lib_info["title"], "year": lib_info.get("year", "")}
                poster = f'<img src="{t["poster"]}" height="60" loading="lazy">' if t.get("poster") else ""
                imdb = str(t.get("imdb_rating", "")) if t.get("imdb_rating") else ""
                source = lib_info.get("source", "")
                vsource = detect_video_source(lib_info.get("path", ""))
                vsource_icon = SOURCE_ICONS.get(vsource, "")
                plays = lib_info.get("playcount", "")
                # Inline rating stars
                stars = "".join('<a href="#" onclick="rate(this,\'' + u + '\',\'' + iid + '\',' + str(s) + ');return false" style="text-decoration:none;color:gold" title="' + str(s) + '">' + ("★" if s <= 5 else "☆") + '</a>' for s in range(1, 11))
                rows += f'<tr><td>{poster}</td><td><a href="https://www.imdb.com/title/{iid}/" target="_blank">{t.get("title",iid)}</a></td><td>{t.get("year","")}</td><td>{imdb}</td><td>{source} {vsource_icon}</td><td>{plays}</td><td>{stars}</td></tr>'
            html = page_head(f"Unrated - {u}")
            html += nav_bar("ratings", u)
            html += '<div class="page">'
            html += f'<h2>Watched but unrated - {len(unrated)} titles</h2>'
            html += '<script>function sortTable(n){const tb=document.querySelector("tbody"),rows=[...tb.rows],dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:"";rows.sort((a,b)=>{let x=a.cells[n].textContent,y=b.cells[n].textContent;return(typeof x==="number"&&typeof y==="number"?(x-y):(String(x)).localeCompare(String(y),undefined,{numeric:true}))*dir});rows.forEach(r=>tb.appendChild(r))}'
            html += 'function rate(el,user,iid,score){fetch("' + BASE + '/rate/"+user+"/"+iid+"/"+score).then(()=>{const row=el.closest("tr");const stars=row.querySelectorAll("a[href*=rate]");stars.forEach((s,i)=>{s.style.color=i<score?"#4fc3f7":"#444"});row.style.opacity="0.6"})}'
            html += '</script>'
            html += '<p style="color:var(--muted)">Movies you watched in Kodi or Trakt but never rated. Click stars to rate.</p>'
            html += '<table><thead><tr><th></th><th onclick="sortTable(1)">Title</th><th onclick="sortTable(2)">Year</th><th onclick="sortTable(3)">IMDB</th><th onclick="sortTable(4)">Source</th><th onclick="sortTable(5)">Plays</th><th>Rate</th></tr></thead>'
            html += '<tbody>' + rows + '</tbody></table>'
            html += '</div>' + page_foot()
            self._page(html, "ratings", u)
            return
        elif p.startswith("/stats/"):
            u = parts[-1]
            self._page(render_stats(u), "ratings", u)
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
        elif p.startswith("/confirm/"):
            u = parts[-1]
            mismatches = find_mismatches(u)
            rows = ""
            for m in mismatches[:200]:
                short_path = m["path"].split("/")[-1] if "/" in m["path"] else m["path"].split(chr(92))[-1]
                if short_path.lower() in ("video_ts.ifo","index.bdmv"):
                    parts_p = m["path"].replace(chr(92),"/").split("/")
                    short_path = "/".join(parts_p[-3:]) if len(parts_p)>=3 else short_path
                match_color = "#d72" if m["match"] < 0.1 else "#f90" if m["match"] < 0.2 else "#aaa"
                rows += '<tr><td><a href="' + BASE + '/title/' + m["iid"] + '">' + m["db_title"] + '</a> (' + str(m.get('year','')) + ')</td>'
                rows += '<td style="font-size:.85em;color:var(--muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + m["path"] + '">' + short_path + '</td>'
                rows += '<td style="color:' + match_color + '">' + str(int(m["match"]*100)) + '%</td>'
                rows += '<td><a href="' + BASE + '/scraper-match/' + u + '/' + m["iid"] + '?q=' + m["db_title"].replace(" ","+") + '" class="btn">🔍 Re-match</a></td></tr>'
            html = page_head(f"To Be Confirmed - {u}")
            html += nav_bar("library", u)
            html += '<div class="page">'
            html += f'<h2>⚠ To Be Confirmed — {len(mismatches)} mismatches</h2>'
            html += '<p style="color:var(--muted)">Titles where the IMDB match doesn\'t match the filename. Low % = likely wrong match.</p>'
            html += '<table><thead><tr><th onclick="sortTable(0)">IMDB Title</th><th>Filename</th><th onclick="sortTable(2)">Match</th><th></th></tr></thead>'
            html += '<tbody>' + rows + '</tbody></table>'
            html += '<script>function sortTable(n){const tb=document.querySelector("tbody"),rows=[...tb.rows],dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:"";rows.sort((a,b)=>{let x=a.cells[n].textContent,y=b.cells[n].textContent;return(typeof x==="number"&&typeof y==="number"?(x-y):(String(x)).localeCompare(String(y),undefined,{numeric:true}))*dir});rows.forEach(r=>tb.appendChild(r))}</script>'
            html += '</div>' + page_foot()
            self._page(html, "library", u)
            return
        elif p.startswith("/title/"):
            iid = parts[-1]
            titles = load_titles()
            t = titles.get(iid, {})
            ratings = load_user_ratings(user)
            r = ratings.get(iid, {})
            library = load_user_tmm(user)
            lib_info = library.get(iid, {})
            poster = f'<img src="{t.get("poster","")}" style="border-radius:8px;max-height:350px;float:left;margin-right:20px">' if t.get("poster") else ""
            provs = " ".join(PROVIDER_ICONS.get(p,"") + " " + p for p in t.get("providers", []))
            watch_link = f'<a href="{t.get("watch_link","")}" target="_blank" class="btn btn-primary">Watch now</a>' if t.get("watch_link") else ""
            trailer = f'<a href="{t.get("trailer","")}" target="_blank" class="btn">▶️ Trailer</a>' if t.get("trailer") else ""
            # Rating history
            rating_html = ""
            if r:
                prev = r.get("rating", 0)
                stars = "".join('<a href="' + BASE + '/rate/' + user + '/' + iid + '/' + str(s) + '" style="text-decoration:none;color:' + ('#4fc3f7' if s <= prev else '#444') + ';font-size:1.5em">' + "★" + '</a>' for s in range(1, 11))
                rating_html = f'<div style="margin:15px 0"><b>Your rating:</b> {prev}/10 on {r.get("date","?")}<br>{stars}</div>'
            else:
                stars = "".join('<a href="' + BASE + '/rate/' + user + '/' + iid + '/' + str(s) + '" style="text-decoration:none;color:gold;font-size:1.5em">' + ("★" if s <= 5 else "☆") + '</a>' for s in range(1, 11))
                rating_html = f'<div style="margin:15px 0"><b>Rate this:</b><br>{stars}</div>'
            # Scores
            scores = []
            if t.get("imdb_rating"): scores.append(f'IMDB: {t["imdb_rating"]}')
            if t.get("tmdb_rating"): scores.append(f'TMDB: {t["tmdb_rating"]}')
            if t.get("rotten_tomatoes"): scores.append(f'🍅 {t["rotten_tomatoes"]}')
            if t.get("metacritic"): scores.append(f'Metacritic: {t["metacritic"]}')
            # Local info
            local_html = ""
            if lib_info:
                local_html = '<div class="card" style="margin-top:15px"><h4>💾 Local Library</h4>'
                vsrc = detect_video_source(lib_info.get("path",""))
                vsrc_icon = SOURCE_ICONS.get(vsrc, "")
                local_html += f'<p>Source: {lib_info.get("source","")} {vsrc_icon} {vsrc}'
                if lib_info.get("quality") or lib_info.get("video_height"): local_html += f' · {lib_info.get("video_height") or lib_info.get("quality","")}p'
                if lib_info.get("video_codec"): local_html += f' · {lib_info.get("video_codec","")}'
                if lib_info.get("file_size"): local_html += f' · {int(lib_info["file_size"])/(1024**3):.1f} GB'
                local_html += '</p>'
                if lib_info.get("audio"):
                    audio = lib_info["audio"]
                    if isinstance(audio, list):
                        local_html += '<p>Audio: ' + ", ".join(f'{a.get("codec","")} {a.get("channels","")}ch {a.get("language","")}' for a in audio[:4]) + '</p>'
                if lib_info.get("subtitles"):
                    subs = lib_info["subtitles"]
                    if isinstance(subs, list):
                        local_html += '<p>Subs: ' + ", ".join(s.get("language","") for s in subs[:6]) + '</p>'
                local_html += '</div>'
            html = page_head(t.get("title", iid))
            html += nav_bar("ratings", user)
            html += '<div class="page">'
            html += f'<div style="overflow:hidden">{poster}<div>'
            html += f'<h1>{t.get("title","?")} <span style="color:var(--muted);font-weight:normal">({t.get("year","")})</span></h1>'
            html += f'<p style="color:var(--muted)">{t.get("genres","")}</p>'
            html += f'<p>{t.get("overview","") or t.get("plot","")}</p>'
            # Compute match score for this title
            profile = build_taste_profile(ratings, titles, user)
            
            match = round(score_title(t, profile), 1) if profile.get("keywords") else 0
            if match: scores.append(f'Match: <span style="color:var(--accent)">{match}</span>')
            html += f'<p style="font-size:1.1em">{" · ".join(scores)}</p>\n' 
            if t.get("awards"): html += f'<p>🏆 {t["awards"]}</p>'
            if t.get("directors"): html += f'<p><b>Director:</b> {t["directors"]}</p>'
            if t.get("cast"): html += f'<p><b>Cast:</b> {t["cast"]}</p>'
            if t.get("writers"): html += f'<p><b>Writers:</b> {t["writers"]}</p>'
            html += f'<p>{provs}</p>'
            html += f'<div style="display:flex;gap:8px;margin:10px 0">{watch_link} {trailer} <a href="{BASE}/similar/{iid}" class="btn">🔗 Similar</a> <a href="https://www.imdb.com/title/{iid}/" target="_blank" class="btn">IMDB</a></div>'
            html += rating_html
            html += '</div></div>'
            html += local_html
            if t.get("keywords"):
                html += '<p style="margin-top:15px;color:var(--muted);font-size:.85em">Keywords: ' + ", ".join(t["keywords"][:15]) + '</p>'
            html += '</div>' + page_foot()
            self._page(html, "ratings", user)
            return
        elif p.startswith("/similar/"):
            iid = parts[-1]
            titles = load_titles()
            t = titles.get(iid, {})
            results = tastedive_similar(iid, t.get("title", ""))
            rows = "".join("<tr><td><b>" + r["title"] + "</b></td><td>" + r.get("description","")[:200] + "</td></tr>" for r in results)
            self._html(f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>Similar</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>body{{font-family:sans-serif;background:#1a1a2e;color:#eee;margin:20px}}table{{border-collapse:collapse;width:100%}}
td{{padding:8px;border-bottom:1px solid #333}}a{{color:#4fc3f7}}</style></head>
<body><h2>Similar to {t.get("title","?")}</h2>
<table>{rows}</table><p><a href="{BASE}/">← Back</a></p></body></html>""")
            return
        elif p.startswith("/rate/"):
            u = parts[-3] if len(parts) >= 3 else self._user(parts)
            iid = parts[-2]
            score = int(parts[-1])
            if 1 <= score <= 10:
                ratings = load_user_ratings(u)
                ratings[iid] = {"rating": score, "date": time.strftime("%Y-%m-%d")}
                save_user_ratings(u, ratings)
            # AJAX call: return JSON. Browser nav: redirect.
            accept = self.headers.get("Accept", "")
            if "application/json" in accept or "fetch" in self.headers.get("Sec-Fetch-Mode", ""):
                self._json({"status": "ok", "rating": score})
            else:
                self._redirect(self.headers.get("Referer", f"{BASE}/u/{u}"))
            return
        elif p == "/datasets/download":
            if not active_job()[1]:
                start_job("imdb_datasets", lambda jid: (download_imdb_datasets(jid), seed_from_imdb_dataset(jid)))
            self._redirect(f"{BASE}/")
            return
        elif p.startswith("/tvshows/"):
            u = parts[-1] if len(parts) > 1 else self._user(parts)
            self._page(render_tvshows(u), "library", u)
            return
        elif p.startswith("/library/"):
            u = parts[-1] if len(parts) > 1 else self._user(parts)
            self._page(render_library(u), "library", u)
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
            self._page(render_recs(u), "discover", u)
        elif p == "/catalog":
            self._page(render_catalog(), "discover", user)
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
                self._page(render_setup(u), "setup", u)
        elif p == "/jobs":
            self._json(get_jobs())
        elif p.startswith("/thumbnails/"):
            fname = parts[-1]
            thumb_path = os.path.join(DATA_DIR, "thumbnails", fname)
            if os.path.exists(thumb_path):
                self.send_response(200)
                self.send_header("Content-Type", "image/jpeg")
                self.send_header("Cache-Control", "max-age=86400")
                self.end_headers()
                with open(thumb_path, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(404)
                self.end_headers()
            return
        elif p.startswith("/api/agent_status"):
            # Agent reports status via GET with base64-encoded JSON in query
            import base64
            encoded = qs.get("s", [""])[0]
            if encoded:
                try:
                    status = json.loads(base64.b64decode(encoded).decode())
                    save_agent_status(status)
                except: pass
            self._json({"status": "ok"})
            return
        elif p.startswith("/api/tasks/complete/"):
            task_id = parts[-1]
            import base64
            encoded = qs.get("r", [""])[0]
            result = None
            if encoded:
                try: result = json.loads(base64.b64decode(encoded).decode())
                except: pass
            complete_task(task_id, result)
            self._json({"status": "ok"})
            return
        elif p.startswith("/api/agent_status"):
            import base64
            encoded = qs.get("s", [""])[0]
            if encoded:
                try: save_agent_status(json.loads(base64.b64decode(encoded).decode()))
                except: pass
            self._json({"status": "ok"})
            return
        elif p == "/api/tasks":
            # Agent polls this for pending tasks
            self._json({"tasks": get_pending_tasks()})
            return
        elif p.startswith("/api/tasks/complete/"):
            # Agent reports task completion (via POST below)
            pass
        elif p == "/api":
            self._json({"titles": len(load_titles()), "users": {u: len(load_user_ratings(u)) for u in list_users()}})
        elif p.startswith("/u/"):
            u = parts[-1]
            self._html(render_ratings(u))
        else:
            self._html(render_ratings(user))

    def do_POST(self):
        try:
            self._do_POST()
        except Exception as e:
            print(f"[POST ERROR] {self.path}: {e}")
            import traceback; traceback.print_exc()
            try:
                self.send_response(500)
                self.end_headers()
            except: pass

    def _do_POST(self):
        global TMDB_KEY, OMDB_KEY, TVDB_KEY, AGENT_TOKEN
        cl = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(cl) if cl > 0 else b""
        parts = [p for p in self.path.split("?")[0].split("/") if p]
        # Agent status - handle early
        if self.path.startswith("/api/agent_status"):
            if body:
                try: save_agent_status(json.loads(body.decode()))
                except: pass
            self._json({"status": "ok"})
            return
        # Task completion - handle early
        if self.path.startswith("/api/tasks/complete/"):
            task_id = parts[-1]
            try: complete_task(task_id, json.loads(body.decode()).get("result") if body else None)
            except: pass
            self._json({"status": "ok"})
            return
        # Library push - handle early
        if self.path.startswith("/api/library/"):
            user = parts[-1]
            try:
                data = json.loads(body.decode())
                library = load_user_tmm(user)
                # Merge: keep existing fields (file_size, file_hash, nfo_matched, etc.)
                for iid, info in data.get("library", {}).items():
                    if iid in library and isinstance(library[iid], dict) and isinstance(info, dict):
                        library[iid].update(info)
                    else:
                        library[iid] = info
                save_user_tmm(user, library)
                task_count = generate_tasks_for_library(user)
                self._json({"status": "ok", "count": len(library), "tasks_generated": task_count})
            except Exception as e:
                self._json({"error": str(e)})
            return
        # API endpoints first (before multipart parsing)
        if self.path == "/api/tasks":
            if body:
                try: save_agent_status(json.loads(body.decode()))
                except: pass
            self._json({"tasks": get_pending_tasks()})
            return
        if self.path.startswith("/api/tasks/complete/"):
            task_id = parts[-1]
            data = json.loads(body.decode()) if body else {}
            complete_task(task_id, data.get("result"))
            self._json({"status": "ok"})
            return
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
        elif self.path.startswith("/api/thumbnail/"):
            user = parts[-1]
            data = json.loads(body.decode())
            iid = data.get("imdb_id", "")
            thumb_b64 = data.get("thumbnail", "")
            if iid and thumb_b64:
                import base64
                thumb_dir = os.path.join(DATA_DIR, "thumbnails")
                os.makedirs(thumb_dir, exist_ok=True)
                thumb_path = os.path.join(thumb_dir, iid.replace("/","_") + ".jpg")
                with open(thumb_path, "wb") as f:
                    f.write(base64.b64decode(thumb_b64))
                # Update library entry
                library = load_user_tmm(user)
                if iid in library and isinstance(library[iid], dict):
                    library[iid]["thumbnail"] = "/thumbnails/" + iid.replace("/","_") + ".jpg"
                    save_user_tmm(user, library)
            self._json({"status": "ok"})
            return
        elif self.path.startswith("/api/agent_status"):
            if body:
                try: save_agent_status(json.loads(body.decode()))
                except: pass
            self._json({"status": "ok"})
            return
        elif self.path.startswith("/api/tasks/complete/"):
            task_id = parts[-1]
            data = json.loads(body.decode()) if body else {}
            complete_task(task_id, data.get("result"))
            self._json({"status": "ok"})
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
            incoming = data.get("library", {})
            for iid, info in incoming.items():
                if iid in library:
                    existing = library[iid]
                    if isinstance(info, list):
                        if isinstance(existing, list):
                            # Merge by path to avoid exact duplicates
                            existing_paths = {e.get("path","") for e in existing if isinstance(e, dict)}
                            for item in info:
                                if isinstance(item, dict) and item.get("path","") not in existing_paths:
                                    existing.append(item)
                        else:
                            existing_paths = {existing.get("path","")} if isinstance(existing, dict) else set()
                            new_items = [existing] if isinstance(existing, dict) else []
                            for item in info:
                                if isinstance(item, dict) and item.get("path","") not in existing_paths:
                                    new_items.append(item)
                            library[iid] = new_items if len(new_items) > 1 else new_items[0] if new_items else info
                    elif isinstance(info, dict):
                        if isinstance(existing, list):
                            if info.get("path","") not in {e.get("path","") for e in existing if isinstance(e, dict)}:
                                existing.append(info)
                        elif isinstance(existing, dict):
                            if info.get("path","") != existing.get("path",""):
                                library[iid] = [existing, info]
                            else:
                                existing.update(info)
                    else:
                        library[iid] = info
                else:
                    library[iid] = info
            save_user_tmm(user, library)
            # Auto-generate tasks for the agent
            task_count = generate_tasks_for_library(user)
            self._json({"status": "ok", "count": len(library), "tasks_generated": task_count})
            return
        elif self.path.startswith("/import/streaming/"):
            # /import/streaming/<user>/<service>
            user = parts[-2]
            service = parts[-1]
            boundary = self.headers["Content-Type"].split("boundary=")[1].encode()
            raw = b""
            for part in body.split(b"--" + boundary):
                if b'name="file"' in part:
                    raw = part.split(b"\r\n\r\n", 1)[1].rsplit(b"\r\n", 1)[0]
            if raw:
                text = raw.decode("utf-8-sig")
                imported = import_streaming_history(user, service, text)
                print(f"Imported {imported} from {service} for {user}")
            self._redirect(f"{BASE}/unrated/{user}")
            return
        elif self.path.startswith("/keys"):
            params = urllib.parse.parse_qs(body.decode())
            existing = json.load(open(KEYS_FILE)) if os.path.exists(KEYS_FILE) else {}
            for k in ("tmdb", "omdb", "tvdb", "opensubs", "agent_token"):
                v = params.get(k, [""])[0]
                if v: existing[k] = v
            keys = existing
            json.dump(keys, open(KEYS_FILE, "w"))
            TMDB_KEY, OMDB_KEY, TVDB_KEY = keys["tmdb"], keys["omdb"], keys["tvdb"]
            AGENT_TOKEN = keys.get("agent_token", "")
            self._redirect(f"{BASE}/")
        else:
            self.send_response(404)
            self.end_headers()

    def _page(self, body, section="ratings", user=""):
        self._html(wrap_page(body, section, user))
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
    def log_message(self, format, *args):
        if args and "error" in str(args).lower():
            print(f"[http] {format % args}")
    def log_error(self, format, *args):
        print(f"[http-error] {format % args}")

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

def _resolve_from_imdb_dataset():
    """Resolve library titles without metadata from IMDB dataset."""
    titles = load_titles()
    for u in list_users():
        lib = load_user_tmm(u)
        items = {k: v for k, v in lib.items() if not k.startswith("_") and isinstance(v, dict)}
        unresolved = [k for k in items if k not in titles]
        if not unresolved: continue
        unresolved_set = set(unresolved)
        resolved = 0
        basics = os.path.join(IMDB_DATASET_DIR, "title.basics.tsv")
        if os.path.exists(basics):
            with open(basics, encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter="\t")
                for row in reader:
                    iid = row.get("tconst", "")
                    if iid in unresolved_set:
                        titles[iid] = {"title": row.get("primaryTitle",""), "year": row.get("startYear","") if row.get("startYear") != "\\N" else "",
                            "type": row.get("titleType",""), "genres": row.get("genres","").replace(",",", ") if row.get("genres") != "\\N" else ""}
                        resolved += 1
                        unresolved_set.discard(iid)
        # Fallback: library title
        for iid in list(unresolved_set):
            info = items.get(iid, {})
            if info.get("title"):
                titles[iid] = {"title": info["title"], "year": info.get("year","")}
                resolved += 1
        if resolved:
            save_titles(titles)
            print(f"Resolved {resolved} titles for {u}")

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
    class ThreadedServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True
    ThreadedServer(("0.0.0.0", PORT), H).serve_forever()
