"""CineCross — Data layer: DB, JSON stores, config constants, task queue."""
import csv, json, os, time, threading
import threading as _threading
import sqlite3
import html as _html_escape

TMDB_KEY = os.environ.get("TMDB_KEY", "")
OMDB_KEY = os.environ.get("OMDB_KEY", "")
TVDB_KEY = os.environ.get("TVDB_KEY", "")
TRAKT_ID = os.environ.get("TRAKT_ID", "")
TRAKT_SECRET = os.environ.get("TRAKT_SECRET", "")
TRAKT_REDIRECT = os.environ.get("TRAKT_REDIRECT", "https://your-domain.com/trakt/callback")
SIMKL_ID = os.environ.get("SIMKL_ID", "")
SIMKL_SECRET = os.environ.get("SIMKL_SECRET", "")
SIMKL_REDIRECT = os.environ.get("SIMKL_REDIRECT", "https://your-domain.com/simkl/callback")
WATCH_COUNTRY = os.environ.get("WATCH_COUNTRY", "LU")  # ISO 3166-1 for streaming availability
DEFAULT_PROVIDERS = {"Netflix", "Amazon Prime Video", "Disney Plus", "Max"}  # Defaults for new users  # User's subscriptions
DATA_DIR = "/data"


VERIFY_VERSION = 1  # Bump this to re-run verification on all files
_db_local = threading.local()

def esc(s):
    """Escape HTML to prevent XSS."""
    return _html_escape.escape(str(s)) if s else ""

def get_db():
    if not hasattr(_db_local, "conn") or _db_local.conn is None:
        _db_local.conn = sqlite3.connect(os.path.join(DATA_DIR, "cinecross.db"), timeout=10)
        _db_local.conn.execute("PRAGMA journal_mode=WAL")
        _db_local.conn.execute("PRAGMA busy_timeout=5000")
        _db_local.conn.row_factory = sqlite3.Row
    return _db_local.conn

def init_fts():
    """Initialize FTS5 full-text search index on titles."""
    db = get_db()
    db.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS title_fts USING fts5(
        imdb_id, title, plot, genres, directors, actors, keywords,
        tokenize='porter unicode61')""")
    db.commit()

def rebuild_fts():
    """Rebuild FTS index from titles.json."""
    db = get_db()
    db.execute("DELETE FROM title_fts")
    titles = load_titles()
    for iid, t in titles.items():
        kw = " ".join(t.get("keywords") or []) if isinstance(t.get("keywords"), list) else ""
        dirs = " ".join(t.get("directors") or []) if isinstance(t.get("directors"), list) else str(t.get("directors",""))
        acts = " ".join(t.get("actors") or []) if isinstance(t.get("actors"), list) else str(t.get("actors",""))
        db.execute("INSERT INTO title_fts VALUES (?,?,?,?,?,?,?)",
            (iid, t.get("title",""), (t.get("overview") or t.get("plot") or "")[:500],
             t.get("genres",""), dirs, acts, kw))
    db.commit()
    print(f"FTS: indexed {len(titles)} titles")

def search_fts(query, limit=50):
    """Full-text search across titles, plots, cast, keywords."""
    db = get_db()
    try:
        rows = db.execute("SELECT imdb_id, rank FROM title_fts WHERE title_fts MATCH ? ORDER BY rank LIMIT ?",
            (query, limit)).fetchall()
        return [r["imdb_id"] for r in rows]
    except: return []

def init_db():
    db = get_db()
    db.executescript("""
        CREATE TABLE IF NOT EXISTS task_queue (
            id TEXT PRIMARY KEY, type TEXT, params TEXT DEFAULT '{}',
            priority INTEGER DEFAULT 0, status TEXT DEFAULT 'pending',
            created TEXT, completed TEXT, result TEXT);
        CREATE TABLE IF NOT EXISTS agent_data (
            user TEXT, imdb_id TEXT, field TEXT, value TEXT, updated_at TEXT,
            PRIMARY KEY (user, imdb_id, field));
        CREATE TABLE IF NOT EXISTS enrichment_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, imdb_id TEXT, title TEXT,
            year TEXT, ts TEXT, changes TEXT);
        CREATE TABLE IF NOT EXISTS incoming (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user TEXT, path TEXT UNIQUE,
            filename TEXT, size INTEGER, title_guess TEXT, year_guess TEXT,
            quality TEXT, tmdb_match TEXT, status TEXT DEFAULT 'pending', destination TEXT);
        CREATE TABLE IF NOT EXISTS verification (
            path TEXT, step TEXT, imdb_id TEXT,
            status TEXT, result TEXT, version INTEGER DEFAULT 1,
            ts TEXT);
    """)
    db.commit()

_task_seq = [0]

def db_enqueue_task(task_type, params=None, priority=0):
    db = get_db()
    _task_seq[0] = _task_seq[0] + 1
    tid = f"task_{int(time.time()*1000)}_{_task_seq[0]}"
    db.execute("INSERT INTO task_queue (id,type,params,priority,status,created) VALUES (?,?,?,?,?,?)",
        (tid, task_type, json.dumps(params or {}), priority, "pending", time.strftime("%Y-%m-%d %H:%M:%S")))
    db.commit()
    return tid

TASK_LANES = {
    "disk": ["validate_match", "hash_files", "size_files", "check_quality", "integrity_check", "scan_extra", "quality_score", "compare_files", "diag"],
    "cpu": ["contact_sheet", "ssim_compare", "merge_audio", "strip_audio", "generate_thumb", "transcode_dvd", "sync_subs", "verify_stills"],
    "api": ["identify_movie", "download_subs", "search_upgrade"],
}
LANE_FOR_TYPE = {t: lane for lane, types in TASK_LANES.items() for t in types}

def db_get_pending_tasks(limit=5):
    db = get_db()
    # Return one task per lane — enables true parallel processing by bottleneck
    results = []
    seen_ids = set()
    for lane, types in TASK_LANES.items():
        placeholders = ",".join(f"'{t}'" for t in types)
        rows = db.execute(f"SELECT * FROM task_queue WHERE status='pending' AND type IN ({placeholders}) ORDER BY priority, created LIMIT 2").fetchall()
        for r in rows:
            if r["id"] not in seen_ids:
                results.append(r)
                seen_ids.add(r["id"])
    # Also grab any uncategorized tasks
    if len(results) < limit:
        rows = db.execute("SELECT * FROM task_queue WHERE status='pending' ORDER BY priority, created LIMIT ?", (limit,)).fetchall()
        for r in rows:
            if r["id"] not in seen_ids:
                results.append(r)
                seen_ids.add(r["id"])
                if len(results) >= limit: break
    return [{"id":r["id"],"type":r["type"],"params":json.loads(r["params"] or "{}"),"priority":r["priority"],"status":r["status"]} for r in results[:limit]]

def db_complete_task(task_id, result=None):
    db = get_db()
    db.execute("UPDATE task_queue SET status='done',completed=?,result=? WHERE id=?",
        (time.strftime("%Y-%m-%d %H:%M:%S"), json.dumps(result) if result else None, task_id))
    if db.execute("SELECT changes()").fetchone()[0] == 0:
        db.execute("INSERT OR IGNORE INTO task_queue (id,type,status,completed,result) VALUES (?,?,?,?,?)",
            (task_id, "batch", "done", time.strftime("%Y-%m-%d %H:%M:%S"), json.dumps(result) if result else None))
    db.commit()

def db_trim_done(keep=500):
    db = get_db()
    db.execute("DELETE FROM task_queue WHERE status='done' AND id NOT IN (SELECT id FROM task_queue WHERE status='done' ORDER BY completed DESC LIMIT ?)", (keep,))
    db.commit()

def db_get_agent_data(user, imdb_id=None):
    db = get_db()
    if imdb_id:
        rows = db.execute("SELECT field, value FROM agent_data WHERE user=? AND imdb_id=?", (user, imdb_id)).fetchall()
        return {r["field"]: r["value"] for r in rows}
    rows = db.execute("SELECT imdb_id, field, value FROM agent_data WHERE user=?", (user,)).fetchall()
    result = {}
    for r in rows:
        result.setdefault(r["imdb_id"], {})[r["field"]] = r["value"]
    return result

def db_clear_auto_tasks():
    db = get_db()
    db.execute("DELETE FROM task_queue WHERE status='pending' AND priority > -1 AND type NOT IN ('exec_code','update_agent')")
    db.commit()

def db_set_agent_data(user, imdb_id, field, value):
    get_db().execute("INSERT OR REPLACE INTO agent_data (user,imdb_id,field,value,updated_at) VALUES (?,?,?,?,?)",
        (user, imdb_id, field, str(value), time.strftime("%Y-%m-%d %H:%M:%S")))
    get_db().commit()

def db_get_agent_field_count(user, field):
    return get_db().execute("SELECT COUNT(*) FROM agent_data WHERE user=? AND field=?", (user, field)).fetchone()[0]

def db_log_enrichment(imdb_id, title, year, changes):
    get_db().execute("INSERT INTO enrichment_log (imdb_id,title,year,ts,changes) VALUES (?,?,?,?,?)",
        (imdb_id, title, year, time.strftime("%Y-%m-%dT%H:%M:%S"), json.dumps(changes)))
    get_db().commit()

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
            try:
                return json.load(open(path))
            except (json.JSONDecodeError, ValueError):
                print(f"Warning: corrupt JSON in {path}")
                return None
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

PROVIDER_ICONS = {"Netflix": "🟥", "Amazon Prime Video": "📦", "Disney Plus": "🏰", "Max": "🟪", "Apple TV Plus": "🍎"}
LU_PROVIDER_IDS = {"Netflix": 8, "Amazon Prime Video": 119, "Disney Plus": 337}  # TMDB provider IDs per country


TASK_QUEUE_FILE = f"{DATA_DIR}/task_queue.json"
AGENT_STATUS_FILE = f"{DATA_DIR}/agent_status.json"

def load_task_queue():
    return safe_json_load(TASK_QUEUE_FILE) or []

def save_task_queue(queue):
    safe_json_save(TASK_QUEUE_FILE, queue)


def save_agent_status(status):
    status["last_seen"] = time.strftime("%Y-%m-%d %H:%M:%S")
    json.dump(status, open(AGENT_STATUS_FILE, "w"))

def load_agent_status():
    if os.path.exists(AGENT_STATUS_FILE): return json.load(open(AGENT_STATUS_FILE))
    return {}

PRIORITY_HUMAN = -1   # User clicked something — do it NOW
PRIORITY_DUPES = 0    # Duplicate detection
PRIORITY_QUALITY = 1  # Sizing, quality checks
PRIORITY_SUBS = 2     # Subtitle search (API rate limited)


def enqueue_task(task_type, params=None, priority=0):
    """Add a task to the queue (SQLite)."""
    return db_enqueue_task(task_type, params, priority)

def get_pending_tasks(limit=5):
    """Get next pending tasks (SQLite)."""
    return db_get_pending_tasks(limit)

_exec_results = {}  # Persistent store for exec_code results

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
DEFAULT_USER = "ecb"

def user_dir(user):
    if not user or user != DEFAULT_USER:
        user = DEFAULT_USER
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



def load_user_trakt_token(user):
    f = f"{user_dir(user)}/trakt_token.json"
    if os.path.exists(f): return json.load(open(f))
    return None

def save_user_trakt_token(user, token):
    json.dump(token, open(f"{user_dir(user)}/trakt_token.json", "w"))

def load_user_simkl_token(user):
    f = f"{user_dir(user)}/simkl_token.json"
    if os.path.exists(f): return json.load(open(f))
    return None

def save_user_simkl_token(user, token):
    json.dump(token, open(f"{user_dir(user)}/simkl_token.json", "w"))

def load_agent_data(user):
    path = os.path.join(DATA_DIR, "users", user, "agent_data.json")
    return safe_json_load(path) or {}

def save_agent_data(user, data):
    path = os.path.join(DATA_DIR, "users", user, "agent_data.json")
    safe_json_save(path, data)

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

def save_user_tmm(user, lib):
    json.dump(lib, open(f"{user_dir(user)}/tmm_library.json", "w"))


def _load_key(name):
    if os.path.exists(KEYS_FILE):
        return json.load(open(KEYS_FILE)).get(name, "")
    return ""

def load_collections(user):
    f = f"{user_dir(user)}/collections.json"
    return safe_json_load(f) or {}

def save_collections(user, data):
    safe_json_save(f"{user_dir(user)}/collections.json", data)

def load_watchlist(user):
    f = user_dir(user) + "/watchlist.json"
    if os.path.exists(f): return json.load(open(f))
    return []

def save_watchlist(user, wl):
    json.dump(wl, open(user_dir(user) + "/watchlist.json", "w"))

IMDB_DATASET_DIR = f"{DATA_DIR}/imdb_datasets"
IMDB_BASICS = f"{IMDB_DATASET_DIR}/title.basics.tsv"
IMDB_RATINGS_DS = f"{IMDB_DATASET_DIR}/title.ratings.tsv"
_imdb_cache = {}  # {imdb_id: (title, year, type, genres, runtime, imdb_rating, votes)}


def _merge_agent_data(library, user):
    agent = db_get_agent_data(user) if get_db() else (load_agent_data(user) or {})
    for iid, adata in agent.items():
        if iid not in library: continue
        if isinstance(adata, dict):
            entries = library[iid] if isinstance(library[iid], list) else [library[iid]] if isinstance(library[iid], dict) else []
            for e in entries:
                if not isinstance(e, dict): continue
                for k, v in adata.items():
                    if not v: continue
                    try:
                        if k in ("file_size",) and v: v = int(v)
                    except (ValueError, TypeError): pass
                    if not e.get(k): e[k] = v
    return library
