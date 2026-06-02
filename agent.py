#!/usr/bin/env python3
"""
CinephileCrossroads LAN Agent - runs on your local network, syncs media servers.

Usage:
    python3 agent.py --server https://tools.ecb.pm/imdb --user ecb

Configure your media servers in agent.json (created on first run).
Run via cron for automatic sync: */30 * * * * python3 /path/to/agent.py --server URL --user USER
"""
import json, os, sys, time, threading, urllib.request, urllib.parse, argparse, subprocess, base64

AGENT_VERSION = "2.1.04141657"
CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent.json")
LOG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent.log")
_last_activity = {"task": "starting", "time": "", "errors": 0}
BUFFER_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent_buffer.json")

def buffer_result(task_id, result):
    """Save completed task result locally when server is unreachable."""
    buf = []
    if os.path.exists(BUFFER_FILE):
        try: buf = json.load(open(BUFFER_FILE))
        except: pass
    buf.append({"task_id": task_id, "result": result, "time": time.strftime("%Y-%m-%d %H:%M:%S")})
    json.dump(buf, open(BUFFER_FILE, "w"))

def flush_buffer(base_url, headers):
    """Send buffered results to server."""
    if not os.path.exists(BUFFER_FILE): return 0
    try: buf = json.load(open(BUFFER_FILE))
    except: return 0
    if not buf: return 0
    flushed = 0
    remaining = []
    for item in buf:
        try:
            import base64
            payload = base64.b64encode(json.dumps(item["result"]).encode()).decode()
            req = urllib.request.Request(
                f"{base_url}/api/tasks/complete/{item['task_id']}?r={payload}",
                headers=headers)
            urllib.request.urlopen(req, timeout=5)
            flushed += 1
        except:
            remaining.append(item)
    json.dump(remaining, open(BUFFER_FILE, "w"))
    if flushed: log(f"[task] Flushed {flushed} buffered results")
    return flushed

DEFAULT_CONFIG = {
    "_path_mappings": {
        "/Movies": "\\\\zeus\\Movies",
        "/TVShows": "\\\\zeus\\TVShows"
    },

    "plex": {"enabled": False, "url": "http://192.168.1.x:32400", "token": ""},
    "jellyfin": {"enabled": False, "url": "http://192.168.1.x:8096", "token": ""},
    "emby": {"enabled": False, "url": "http://192.168.1.x:8096", "token": ""},
    "kodi": {"enabled": False, "url": "http://192.168.1.x:8080/jsonrpc", "user": "kodi", "password": ""},
    "radarr": {"enabled": False, "url": "http://192.168.1.x:7878", "token": ""},
    "sonarr": {"enabled": False, "url": "http://192.168.1.x:8989", "token": ""},
    "_agent_token": "paste-your-token-here",
    "tmm": {"enabled": False, "path": "/path/to/tmm/movies", "url": "http://192.168.1.x:7878", "token": "", "template": "ListExampleCSV"},
}

def log(msg):
    """Print and append to log file."""
    line = time.strftime("%Y-%m-%d %H:%M:%S") + " " + msg
    print(line)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
        # Keep log file under 50KB
        if os.path.getsize(LOG_FILE) > 50000:
            lines = open(LOG_FILE).readlines()
            open(LOG_FILE, "w").writelines(lines[-200:])
    except: pass

def get_recent_logs(n=20):
    """Get last N lines from the log file."""
    try:
        if os.path.exists(LOG_FILE):
            return open(LOG_FILE).readlines()[-n:]
    except: pass
    return []

def api_get(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "CinephileAgent/1.0"}), timeout=10) as r:
            return json.loads(r.read())
    except Exception as e:
        print(f"  Error: {url[:60]} - {e}")
        return None

def api_post(url, data):
    req = urllib.request.Request(url, data=json.dumps(data).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "CinephileAgent/1.0"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())

def fetch_plex(cfg):
    lib = {}
    sections = api_get(f"{cfg['url']}/library/sections?X-Plex-Token={cfg['token']}")
    if not sections: return lib
    for d in sections.get("MediaContainer", {}).get("Directory", []):
        if d.get("type") not in ("movie", "show"): continue
        items = api_get(f"{cfg['url']}/library/sections/{d['key']}/all?X-Plex-Token={cfg['token']}")
        if not items: continue
        for item in items.get("MediaContainer", {}).get("Metadata", []):
            for guid in item.get("Guid", []):
                if "imdb://" in guid.get("id", ""):
                    iid = guid["id"].replace("imdb://", "")
                    media = item.get("Media", [{}])[0]
                    lib[iid] = {"source": "plex", "quality": media.get("videoResolution", ""),
                                "path": (media.get("Part", [{}])[0]).get("file", "")}
    return lib

def fetch_jellyfin(cfg):
    lib = {}
    users = api_get(f"{cfg['url']}/Users?api_key={cfg['token']}")
    if not users: return lib
    uid = users[0].get("Id", "")
    items = api_get(f"{cfg['url']}/Users/{uid}/Items?api_key={cfg['token']}&Recursive=true&IncludeItemTypes=Movie,Series&Fields=ProviderIds,Path")
    if not items: return lib
    for item in items.get("Items", []):
        iid = item.get("ProviderIds", {}).get("Imdb", "")
        if iid: lib[iid] = {"source": "jellyfin", "path": item.get("Path", "")}
    return lib

def fetch_kodi(cfg):
    lib = {}
    payload = {"jsonrpc": "2.0", "method": "VideoLibrary.GetMovies", "id": 1,
               "params": {"properties": ["imdbnumber", "file", "streamdetails", "runtime", "title", "year", "playcount", "lastplayed"]}}
    headers = {"Content-Type": "application/json"}
    if cfg.get("user") and cfg.get("password"):
        import base64
        cred = base64.b64encode((cfg["user"] + ":" + cfg["password"]).encode()).decode()
        headers["Authorization"] = "Basic " + cred
    req = urllib.request.Request(cfg["url"], data=json.dumps(payload).encode(), headers=headers)
    try:
        resp = urllib.request.urlopen(req, timeout=120)
        data = json.loads(resp.read())
        for m in data.get("result", {}).get("movies", []):
            iid = m.get("imdbnumber", "")
            if not iid or not iid.startswith("tt"): continue
            sd = m.get("streamdetails", {})
            vs = sd.get("video", [])
            aus = sd.get("audio", [])
            subs = sd.get("subtitle", [])
            v = vs[0] if vs else {}
            lib[iid] = {
                "source": "kodi", "path": m.get("file", ""),
                "title": m.get("title", ""), "year": m.get("year", ""),
                "runtime": m.get("runtime", 0),
                "quality": str(v.get("height", "")),
                "video_codec": v.get("codec", ""),
                "video_width": v.get("width", 0),
                "video_height": v.get("height", 0),
                "audio": [{"codec": a.get("codec",""), "channels": a.get("channels",0), "language": a.get("language","")} for a in aus],
                "subtitles": [{"language": s.get("language","")} for s in subs],
                "playcount": m.get("playcount", 0),
                "lastplayed": m.get("lastplayed", ""),
            }
    except Exception as e:
        print(f"  Kodi movies error: {e}")
    # TV Shows
    payload2 = {"jsonrpc": "2.0", "method": "VideoLibrary.GetTVShows", "id": 2,
                "params": {"properties": ["imdbnumber", "title", "year"]}}
    req2 = urllib.request.Request(cfg["url"], data=json.dumps(payload2).encode(), headers=headers)
    try:
        data2 = json.loads(urllib.request.urlopen(req2, timeout=30).read())
        for s in data2.get("result", {}).get("tvshows", []):
            iid = s.get("imdbnumber", "")
            if iid and iid.startswith("tt") and iid not in lib:
                lib[iid] = {"source": "kodi", "title": s.get("title",""), "year": s.get("year",""), "type": "tvshow"}
    except Exception as e:
        print(f"  Kodi tvshows error: {e}")
    # Episodes with full details
    payload3 = {"jsonrpc": "2.0", "method": "VideoLibrary.GetEpisodes", "id": 3,
                "params": {"properties": ["showtitle", "season", "episode", "title", "file",
                           "streamdetails", "runtime", "playcount", "lastplayed", "uniqueid"]}}
    req3 = urllib.request.Request(cfg["url"], data=json.dumps(payload3).encode(), headers=headers)
    try:
        data3 = json.loads(urllib.request.urlopen(req3, timeout=60).read())
        episodes = data3.get("result", {}).get("episodes", [])
        ep_lib = {}
        for ep in episodes:
            sd = ep.get("streamdetails", {})
            vs = sd.get("video", [])
            aus = sd.get("audio", [])
            subs = sd.get("subtitle", [])
            v = vs[0] if vs else {}
            uid = ep.get("uniqueid", {})
            ep_key = ep.get("showtitle", "") + "|S" + str(ep.get("season", 0)).zfill(2) + "E" + str(ep.get("episode", 0)).zfill(2)
            ep_lib[ep_key] = {
                "source": "kodi", "type": "episode",
                "showtitle": ep.get("showtitle", ""),
                "season": ep.get("season", 0),
                "episode": ep.get("episode", 0),
                "title": ep.get("title", ""),
                "path": ep.get("file", ""),
                "runtime": ep.get("runtime", 0),
                "playcount": ep.get("playcount", 0),
                "lastplayed": ep.get("lastplayed", ""),
                "quality": str(v.get("height", "")),
                "video_codec": v.get("codec", ""),
                "video_height": v.get("height", 0),
                "audio": [{"codec": a.get("codec",""), "channels": a.get("channels",0), "language": a.get("language","")} for a in aus],
                "subtitles": [{"language": s.get("language","")} for s in subs],
                "imdb_id": uid.get("imdb", ""),
                "tvdb_id": uid.get("tvdb", ""),
            }
        # Push episodes separately
        print(f"  Kodi episodes: {len(ep_lib)}")
        lib["_episodes"] = ep_lib
    except Exception as e:
        print(f"  Kodi episodes error: {e}")
    return lib

def fetch_radarr(cfg):
    lib = {}
    movies = api_get(f"{cfg['url']}/api/v3/movie?apiKey={cfg['token']}")
    if not movies: return lib
    for m in movies:
        iid = m.get("imdbId", "")
        if iid: lib[iid] = {"source": "radarr", "downloaded": m.get("hasFile", False),
                            "path": m.get("path", "")}
    return lib

def fetch_sonarr(cfg):
    lib = {}
    shows = api_get(f"{cfg['url']}/api/v3/series?apiKey={cfg['token']}")
    if not shows: return lib
    for s in shows:
        iid = s.get("imdbId", "")
        if iid: lib[iid] = {"source": "sonarr", "downloaded": s.get("statistics", {}).get("percentOfEpisodes", 0) > 0,
                            "path": s.get("path", "")}
    return lib


def opensubtitles_hash(filepath):
    """Compute OpenSubtitles hash: file size + checksum of first and last 64KB."""
    import struct
    block_size = 65536
    file_size = os.path.getsize(filepath)
    if file_size < block_size * 2:
        return None
    hash_val = file_size
    with open(filepath, "rb") as f:
        for _ in range(block_size // 8):
            hash_val += struct.unpack("<Q", f.read(8))[0]
            hash_val &= 0xFFFFFFFFFFFFFFFF
        f.seek(-block_size, 2)
        for _ in range(block_size // 8):
            hash_val += struct.unpack("<Q", f.read(8))[0]
            hash_val &= 0xFFFFFFFFFFFFFFFF
    return format(hash_val, "016x")

def compute_hashes(library):
    """Add file hashes to library entries that have local paths."""
    for iid, info in library.items():
        path = info.get("path", "")
        if path and os.path.isfile(path):
            h = opensubtitles_hash(path)
            if h:
                info["file_hash"] = h
                info["file_size"] = os.path.getsize(path)
    return library

def find_missing_subs(library):
    """Return IMDB IDs of titles with no subtitle streams detected."""
    return [iid for iid, info in library.items()
            if not info.get("subtitles") and info.get("path")]

def fetch_tmm(cfg):
    """Fetch library from tinyMediaManager. Two modes:
    - 'path' mode: scan NFO files in TMM's data directory (no API needed)
    - 'url' mode: use TMM HTTP API to trigger update (API key required)
    """
    import re
    lib = {}
    # Mode 1: scan local NFO files (preferred - works offline)
    scan_paths = cfg.get("paths", [])
    if not scan_paths:
        p = cfg.get("path", "")
        if p: scan_paths = [p]
    scanned = 0
    for scan_path in scan_paths:
      if scan_path and os.path.isdir(scan_path):
        print(f"  Scanning NFO files in {scan_path}...")
        for root, dirs, files in os.walk(scan_path):
            for f in files:
                if not f.endswith(".nfo"): continue
                try:
                    content = open(os.path.join(root, f), encoding="utf-8", errors="ignore").read()
                    # Extract IMDB ID from NFO
                    match = re.search(r"(tt\d{7,})", content)
                    if match:
                        iid = match.group(1)
                        info = {"path": root, "source": "tmm"}
                        # Extract media info from NFO <fileinfo> block
                        vm = re.search(r"<height>(\d+)</height>", content)
                        if vm: info["video_height"] = int(vm.group(1))
                        vm = re.search(r"<width>(\d+)</width>", content)
                        if vm: info["video_width"] = int(vm.group(1))
                        vm = re.search(r"<codec>(\w+)</codec>", content)
                        if vm: info["video_codec"] = vm.group(1)
                        vm = re.search(r"<durationinseconds>(\d+)</durationinseconds>", content)
                        if vm: info["runtime"] = int(vm.group(1)) // 60
                        # Audio streams
                        audio = []
                        for am in re.finditer(r"<audio>(.*?)</audio>", content, re.DOTALL):
                            a = {}
                            ac = re.search(r"<codec>(\w+)</codec>", am.group(1))
                            if ac: a["codec"] = ac.group(1)
                            ch = re.search(r"<channels>(\d+)</channels>", am.group(1))
                            if ch: a["channels"] = int(ch.group(1))
                            lg = re.search(r"<language>(\w+)</language>", am.group(1))
                            if lg: a["language"] = lg.group(1)
                            if a: audio.append(a)
                        if audio: info["audio"] = audio
                        # Subtitles
                        subs = []
                        for sm in re.finditer(r"<subtitle>(.*?)</subtitle>", content, re.DOTALL):
                            lg = re.search(r"<language>(\w+)</language>", sm.group(1))
                            if lg: subs.append({"language": lg.group(1)})
                        if subs: info["subtitles"] = subs
                        # Detect external subtitle files (.srt, .sub, .ass, .ssa)
                        ext_subs = []
                        for sf in os.listdir(root):
                            if sf.lower().endswith((".srt", ".sub", ".ass", ".ssa", ".vtt")):
                                # Extract language from filename: movie.en.srt, movie.french.srt
                                parts_s = sf.rsplit(".", 2)
                                lang = parts_s[-2] if len(parts_s) >= 3 else ""
                                ext_subs.append({"language": lang, "file": sf, "external": True})
                        if ext_subs:
                            info.setdefault("subtitles", [])
                            info["subtitles"].extend(ext_subs)
                        info["quality"] = str(info.get("video_height", ""))
                        if iid in lib:
                            # Duplicate! Convert to list
                            existing = lib[iid]
                            if isinstance(existing, list):
                                existing.append(info)
                            else:
                                lib[iid] = [existing, info]
                        else:
                            lib[iid] = info
                        scanned += 1
                        if scanned % 500 == 0:
                            print(f"    Scanned {scanned} NFO files, found {len(lib)} titles...")
                except: pass
    if scan_paths and lib:
        return lib
    # Mode 2: TMM HTTP API - export to temp dir on same machine, then read
    url = cfg.get("url", "").rstrip("/")
    key = cfg.get("token", "")
    if not url or not key:
        print("  TMM: set either 'path' (NFO scan) or 'url'+'token' (HTTP API)")
        return lib
    import tempfile, time, csv, glob, shutil
    export_path = cfg.get("export_path", os.path.join(tempfile.gettempdir(), "tmm_export"))
    os.makedirs(export_path, exist_ok=True)
    headers = {"Content-Type": "application/json", "api-key": key}
    template = cfg.get("template", "ListExampleCSV")
    for kind, endpoint in [("movie", "/api/movie"), ("tvshow", "/api/tvshow")]:
        payload = json.dumps([
            {"action": "update", "scope": {"name": "all"}},
            {"action": "export", "scope": {"name": "all"},
             "args": {"template": template, "exportPath": export_path}}
        ]).encode()
        try:
            req = urllib.request.Request(url + endpoint, data=payload, headers=headers)
            urllib.request.urlopen(req, timeout=120)
            print(f"  TMM {kind}: export triggered, waiting...")
            time.sleep(10)  # Wait for TMM to finish
        except Exception as e:
            print(f"  TMM {kind} error: {e}")
    # Read exported CSV files
    for csvfile in glob.glob(os.path.join(export_path, "*.csv")) + glob.glob(os.path.join(export_path, "*.html")):
        try:
            with open(csvfile, encoding="utf-8", errors="ignore") as f:
                content = f.read()
                # Try CSV first
                for row in csv.DictReader(content.splitlines()):
                    iid = ""
                    for col in ("IMDb Id", "imdbId", "IMDB ID", "imdb_id"):
                        if row.get(col): iid = row[col]; break
                    if iid:
                        lib[iid] = {"path": row.get("Path", row.get("path", "")),
                                    "quality": row.get("Video Resolution", ""), "source": "tmm"}
        except: pass
    # Also scan for IMDB IDs in any text/html export
    import re
    for f in glob.glob(os.path.join(export_path, "*")):
        try:
            content = open(f, encoding="utf-8", errors="ignore").read()
            for iid in re.findall(r"(tt\d{7,})", content):
                if iid not in lib:
                    lib[iid] = {"source": "tmm"}
        except: pass
    shutil.rmtree(export_path, ignore_errors=True)
    return lib

def unmap_path(local_path, config):
    """Convert local/SMB path back to NFS path."""
    p = local_path.replace(os.sep, "/")
    for nfs, local in config.get("_path_mappings", {}).items():
        local_norm = local.replace(os.sep, "/")
        if p.startswith(local_norm):
            return nfs + p[len(local_norm):]
    return p

def map_path(path, config):
    """Map remote paths (e.g. Kodi NFS, nfs:// URLs) to local paths.
    Handles stack:// URLs by extracting the first file path."""
    # Handle stack:// (Kodi multi-part files)
    if path.startswith("stack://"):
        path = path.replace("stack://", "").split(" , ")[0].strip()
    # Handle nfs:// URLs: nfs://192.168.0.235/volume1/Movies/... -> /mnt/zeus/Movies/...
    if path.startswith("nfs://"):
        # Strip nfs://host/volume1/ prefix
        import re
        m = re.match(r"nfs://[^/]+(/volume1)?(/.*)", path)
        if m:
            path = m.group(2)
    mappings = config.get("_path_mappings", {})
    # Handle normalized relative paths: Movies/... -> /mnt/zeus/Movies/...
    if path.startswith("Movies/") or path.startswith("TVShows/"):
        return "/mnt/zeus/" + path
    for remote, local in mappings.items():
        if path.startswith(remote):
            mapped = local + path[len(remote):]
            if os.name == "nt":
                mapped = mapped.replace("/", "\\")
            else:
                mapped = mapped.replace("\\", "/")
            return mapped
    return path

FETCHERS = {"plex": fetch_plex, "jellyfin": fetch_jellyfin, "emby": fetch_jellyfin,
            "kodi": fetch_kodi, "radarr": fetch_radarr, "sonarr": fetch_sonarr, "tmm": fetch_tmm}

# --- NFS Mount Health ---
SCAN_CACHE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scan_cache.json")

def get_buffer_path(config):
    """Get the local SSD buffer path for heavy I/O operations."""
    return config.get("_buffer_path", "/tmp")

def buffer_copy(src, config):
    """Copy a file to the local SSD buffer for fast processing. Returns local path."""
    import shutil
    buf = get_buffer_path(config)
    dst = os.path.join(buf, os.path.basename(src))
    if os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(src):
        return dst  # Already buffered
    shutil.copy2(src, dst)
    return dst

def buffer_cleanup(*paths):
    """Remove buffered files after processing."""
    for p in paths:
        if p and os.path.exists(p) and "/mnt/buffer" in p:
            os.remove(p)


def check_mounts(config):
    """Check NFS mount health, auto-remount dropped mounts, return list of healthy paths."""
    import threading
    healthy = []
    mounts = config.get("_mounts", [])
    if not mounts:
        tmm_cfg = config.get("tmm", {})
        paths = tmm_cfg.get("paths", [])
        if not paths and tmm_cfg.get("path"):
            paths = [tmm_cfg["path"]]
        mounts = []
        try:
            with open("/proc/mounts") as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3 and parts[2] in ("nfs", "nfs4") and parts[1].startswith("/mnt/"):
                        mounts.append(parts[1])
        except: pass
        if not mounts:
            mounts = list({os.path.dirname(p.rstrip("/")) for p in paths if p.startswith("/mnt/")})

    for mp in mounts:
        result = [False]
        def _check(m=mp):
            try:
                if os.path.isdir(m) and os.listdir(m):
                    result[0] = True
            except: pass
        t = threading.Thread(target=_check, daemon=True)
        t.start()
        t.join(timeout=3)
        if result[0]:
            healthy.append(mp)
        elif t.is_alive():
            log(f"[nfs] {mp} stale (timeout)")
            os.system(f"umount -l {mp} 2>/dev/null; mount {mp} 2>/dev/null &")
        else:
            log(f"[nfs] {mp} empty/unmounted, remounting...")
            os.system(f"mount {mp} 2>/dev/null")
            time.sleep(1)
            try:
                if os.listdir(mp):
                    healthy.append(mp)
                    log(f"[nfs] {mp} remounted OK")
            except: pass
    return healthy


# --- Incremental NFO Scan ---
def load_scan_cache():
    try:
        return json.load(open(SCAN_CACHE_FILE))
    except: return {}

def save_scan_cache(cache):
    json.dump(cache, open(SCAN_CACHE_FILE, "w"))

def incremental_nfo_scan(scan_paths):
    """Only re-scan directories whose mtime changed since last scan."""
    import re
    cache = load_scan_cache()
    lib = {}
    scanned = 0
    skipped = 0

    for scan_path in scan_paths:
        if not os.path.isdir(scan_path):
            continue
        print(f"  Scanning NFO files in {scan_path}...")
        try:
          for entry in os.scandir(scan_path):
            if not entry.is_dir(): continue
            # Year directories - check year mtime first
            year_key = entry.path + "/_year"
            try:
                year_mtime = entry.stat().st_mtime
            except: continue
            cached_year = cache.get(year_key)
            if cached_year and cached_year.get("mtime") == year_mtime:
                # Year unchanged - use all cached entries from this year
                for dir_key, cached in cache.items():
                    if dir_key.startswith(entry.path + "/") and dir_key != year_key and cached.get("iid"):
                        iid = cached["iid"]
                        info = cached["info"]
                        if iid in lib:
                            existing = lib[iid]
                            if isinstance(existing, list): existing.append(info)
                            else: lib[iid] = [existing, info]
                        else:
                            lib[iid] = info
                        skipped += 1
                continue
            try:
              for movie_dir in os.scandir(entry.path):
                if not movie_dir.is_dir(): continue
                dir_key = movie_dir.path
                try:
                    mtime = movie_dir.stat().st_mtime
                except: continue
                cached = cache.get(dir_key)
                if cached and cached.get("mtime") == mtime:
                    # Use cached result
                    if cached.get("iid"):
                        iid = cached["iid"]
                        info = cached["info"]
                        if iid in lib:
                            existing = lib[iid]
                            if isinstance(existing, list): existing.append(info)
                            else: lib[iid] = [existing, info]
                        else:
                            lib[iid] = info
                    skipped += 1
                    continue
                # Scan this directory
                nfo_found = False
                for f in os.listdir(movie_dir.path):
                    if not f.endswith(".nfo"): continue
                    try:
                        content = open(os.path.join(movie_dir.path, f), encoding="utf-8", errors="ignore").read()
                        match = re.search(r"(tt\d{7,})", content)
                        if match:
                            iid = match.group(1)
                            info = {"path": movie_dir.path, "source": "tmm"}
                            vm = re.search(r"<height>(\d+)</height>", content)
                            if vm: info["video_height"] = int(vm.group(1))
                            vm = re.search(r"<width>(\d+)</width>", content)
                            if vm: info["video_width"] = int(vm.group(1))
                            vm = re.search(r"<codec>(\w+)</codec>", content)
                            if vm: info["video_codec"] = vm.group(1)
                            vm = re.search(r"<durationinseconds>(\d+)</durationinseconds>", content)
                            if vm: info["runtime"] = int(vm.group(1)) // 60
                            info["quality"] = str(info.get("video_height", ""))
                            if iid in lib:
                                existing = lib[iid]
                                if isinstance(existing, list): existing.append(info)
                                else: lib[iid] = [existing, info]
                            else:
                                lib[iid] = info
                            cache[dir_key] = {"mtime": mtime, "iid": iid, "info": info}
                            nfo_found = True
                            scanned += 1
                            break
                    except: pass
                if not nfo_found:
                    cache[dir_key] = {"mtime": mtime, "iid": None, "info": None}
                if scanned and scanned % 500 == 0:
                    print(f"    Scanned {scanned}, skipped {skipped} (cached)...")
            except PermissionError: pass
            except OSError: pass
            # Save year mtime so we can skip entire year next time
            cache[year_key] = {"mtime": year_mtime}
        except PermissionError: pass
        except OSError: pass

    save_scan_cache(cache)
    log(f"[scan] {scanned} scanned, {skipped} cached, {len(lib)} titles")
    return lib


# --- Full Mediainfo/ffprobe Extraction ---
def extract_mediainfo(filepath):
    """Run ffprobe and return rich metadata dict."""
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-print_format", "json", "-show_format", "-show_streams", filepath],
            capture_output=True, timeout=30, text=True)
        if r.returncode != 0:
            return {"error": f"ffprobe exit {r.returncode}"}
        data = json.loads(r.stdout)
        fmt = data.get("format", {})
        streams = data.get("streams", [])
        video = next((s for s in streams if s.get("codec_type") == "video"), {})
        audios = [s for s in streams if s.get("codec_type") == "audio"]
        subs = [s for s in streams if s.get("codec_type") == "subtitle"]
        result = {
            "container": fmt.get("format_name", ""),
            "duration": int(float(fmt.get("duration", 0))),
            "size": int(fmt.get("size", 0)),
            "bitrate": int(fmt.get("bit_rate", 0)) // 1000,  # kbps
        }
        if video:
            result["video"] = {
                "codec": video.get("codec_name", ""),
                "profile": video.get("profile", ""),
                "width": video.get("width", 0),
                "height": video.get("height", 0),
                "bitrate": int(video.get("bit_rate", 0)) // 1000 if video.get("bit_rate") else 0,
                "fps": eval(video["r_frame_rate"]) if video.get("r_frame_rate") and "/" in str(video.get("r_frame_rate", "")) else 0,
                "pix_fmt": video.get("pix_fmt", ""),
                "hdr": "yes" if video.get("color_transfer") in ("smpte2084", "arib-std-b67") else "no",
                "color_space": video.get("color_space", ""),
            }
            # HDR10+ / Dolby Vision detection from side_data
            side_data = video.get("side_data_list", [])
            for sd in side_data:
                if "mastering" in sd.get("side_data_type", "").lower():
                    result["video"]["hdr_type"] = "HDR10"
                if "dolby" in sd.get("side_data_type", "").lower():
                    result["video"]["hdr_type"] = "DolbyVision"
        if audios:
            result["audio"] = []
            for a in audios:
                result["audio"].append({
                    "codec": a.get("codec_name", ""),
                    "channels": a.get("channels", 0),
                    "language": a.get("tags", {}).get("language", ""),
                    "bitrate": int(a.get("bit_rate", 0)) // 1000 if a.get("bit_rate") else 0,
                    "title": a.get("tags", {}).get("title", ""),
                })
        if subs:
            result["subtitles"] = [{"language": s.get("tags", {}).get("language", ""), "codec": s.get("codec_name", "")} for s in subs]
        return result
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}


# --- Quality Scoring (BPP + genre-aware) ---
# Genre BPP thresholds: minimum acceptable bits-per-pixel
GENRE_BPP = {
    "animation": 0.03, "anime": 0.03,
    "documentary": 0.05, "drama": 0.05, "romance": 0.05, "comedy": 0.05,
    "horror": 0.06, "thriller": 0.06, "mystery": 0.06,
    "action": 0.10, "adventure": 0.09, "sci-fi": 0.10, "war": 0.10,
    "fantasy": 0.09, "music": 0.05,
}

def compute_quality_score(mediainfo, genres=None):
    """Compute quality score from mediainfo dict. Returns dict with BPP, verdict, effective_quality.
    
    genres: comma-separated string like "Action, Sci-Fi" or list
    """
    video = mediainfo.get("video", {})
    if not video or not video.get("width"):
        return {"error": "no video info"}
    
    w = video.get("width", 0)
    h = video.get("height", 0)
    fps = video.get("fps", 24) or 24
    # Get bitrate: prefer video bitrate, fall back to total - audio estimate
    v_bitrate = video.get("bitrate", 0) * 1000  # was in kbps
    if not v_bitrate:
        total_br = mediainfo.get("bitrate", 0) * 1000
        # Estimate audio bitrate
        audio_br = sum(a.get("bitrate", 0) * 1000 for a in mediainfo.get("audio", []))
        if not audio_br:
            audio_br = 256000 * len(mediainfo.get("audio", [{}]))  # assume 256kbps per track
        v_bitrate = total_br - audio_br
    
    if w == 0 or h == 0 or v_bitrate <= 0:
        return {"error": "missing dimensions or bitrate"}
    
    pixels = w * h
    bpp = v_bitrate / (pixels * fps)
    
    # Determine genre threshold
    threshold = 0.07  # default
    if genres:
        if isinstance(genres, str):
            genres = [g.strip().lower() for g in genres.split(",")]
        else:
            genres = [g.lower() for g in genres]
        # Use the highest threshold among the genres (most demanding wins)
        genre_thresholds = [GENRE_BPP.get(g, 0.07) for g in genres]
        if genre_thresholds:
            threshold = max(genre_thresholds)
    
    # Codec efficiency factor: x265/HEVC is ~40% more efficient than x264
    codec = video.get("codec", "").lower()
    codec_factor = 1.0
    if codec in ("hevc", "h265", "x265"):
        codec_factor = 1.4  # x265 at 0.05 BPP ≈ x264 at 0.07 BPP
    elif codec in ("av1",):
        codec_factor = 1.6
    
    effective_bpp = bpp * codec_factor
    
    # Compute effective resolution (what quality this bitrate actually delivers)
    # If BPP is too low for this resolution, compute what resolution it SHOULD be
    if effective_bpp < threshold * 0.5:
        # Severely starved: effective quality is much lower than labeled
        effective_height = int(h * (effective_bpp / threshold) ** 0.5)
        verdict = "fake_hd"
    elif effective_bpp < threshold:
        effective_height = int(h * (effective_bpp / threshold) ** 0.7)
        verdict = "starved"
    elif effective_bpp < threshold * 1.5:
        effective_height = h
        verdict = "acceptable"
    else:
        effective_height = h
        verdict = "good"
    
    return {
        "bpp": round(bpp, 4),
        "effective_bpp": round(effective_bpp, 4),
        "threshold": round(threshold, 4),
        "codec_factor": codec_factor,
        "resolution": f"{w}x{h}",
        "video_bitrate_kbps": round(v_bitrate / 1000),
        "verdict": verdict,  # fake_hd, starved, acceptable, good
        "effective_height": effective_height,
        "labeled_height": h,
        "genres_used": genres or [],
    }


def compare_quality(file_a_info, file_b_info, genres=None):
    """Compare two files and recommend which is better quality."""
    score_a = compute_quality_score(file_a_info, genres)
    score_b = compute_quality_score(file_b_info, genres)
    if "error" in score_a or "error" in score_b:
        return {"error": "cannot compare", "a": score_a, "b": score_b}
    
    # Verdict ranking: good > acceptable > starved > fake_hd
    verdict_rank = {"good": 4, "acceptable": 3, "starved": 2, "fake_hd": 1}
    rank_a = verdict_rank.get(score_a["verdict"], 0)
    rank_b = verdict_rank.get(score_b["verdict"], 0)
    
    if rank_a > rank_b:
        winner = "a"
        reason = f"A is '{score_a['verdict']}' (BPP {score_a['bpp']:.3f}) vs B '{score_b['verdict']}' (BPP {score_b['bpp']:.3f})"
    elif rank_b > rank_a:
        winner = "b"
        reason = f"B is '{score_b['verdict']}' (BPP {score_b['bpp']:.3f}) vs A '{score_a['verdict']}' (BPP {score_a['bpp']:.3f})"
    else:
        # Same verdict tier: compare effective resolution, then efficiency
        eff_a = score_a["effective_height"]
        eff_b = score_b["effective_height"]
        if eff_a > eff_b * 1.1:
            winner = "a"
            reason = f"Same tier, A has higher effective resolution ({eff_a}p vs {eff_b}p)"
        elif eff_b > eff_a * 1.1:
            winner = "b"
            reason = f"Same tier, B has higher effective resolution ({eff_b}p vs {eff_a}p)"
        else:
            size_a = file_a_info.get("size", 0)
            size_b = file_b_info.get("size", 0)
            winner = "a" if size_a <= size_b else "b"
            reason = f"Same quality tier and resolution, {'A' if winner == 'a' else 'B'} is smaller"
    
    return {"winner": winner, "reason": reason, "a": score_a, "b": score_b}


# --- Non-TMM Directory Scanner ---
def scan_non_tmm(config):
    """Scan directories not managed by TMM: DVD_TS, language folders, etc."""
    import re
    results = []
    tmm_cfg = config.get("tmm", {})
    base_paths = tmm_cfg.get("scan_extra", [])
    if not base_paths:
        # Auto-detect from TMM paths - look for sibling directories
        tmm_paths = tmm_cfg.get("paths", [])
        if not tmm_paths and tmm_cfg.get("path"):
            tmm_paths = [tmm_cfg["path"]]
        for tp in tmm_paths:
            parent = os.path.dirname(tp)
            if os.path.isdir(parent):
                for d in os.listdir(parent):
                    full = os.path.join(parent, d)
                    if os.path.isdir(full) and d != "TMM" and not d.startswith("."):
                        base_paths.append(full)
        # Also check for DVD_TS at Movies root
        movies_root = os.path.dirname(os.path.dirname(tmm_paths[0])) if tmm_paths else ""
        dvd_ts = os.path.join(movies_root, "DVD_TS")
        if os.path.isdir(dvd_ts):
            base_paths.append(dvd_ts)

    vexts = (".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".ts", ".m2ts", ".iso")
    # Pattern: Title (Year) or Title.Year or Title_Year
    year_re = re.compile(r"[\(\[_\.\s]*((?:19|20)\d{2})[\)\]_\.\s]*")
    imdb_re = re.compile(r"(tt\d{7,})")

    for base in base_paths:
        if not os.path.isdir(base):
            continue
        for root, dirs, files in os.walk(base):
            # Check for NFO with IMDB ID first
            nfo_iid = None
            for f in files:
                if f.endswith(".nfo"):
                    try:
                        content = open(os.path.join(root, f), encoding="utf-8", errors="ignore").read()
                        m = imdb_re.search(content)
                        if m: nfo_iid = m.group(1)
                    except: pass
            # Find video files
            vfiles = [f for f in files if f.lower().endswith(vexts)]
            if not vfiles:
                continue
            # Get largest video file
            largest = max(vfiles, key=lambda f: os.path.getsize(os.path.join(root, f)) if os.path.isfile(os.path.join(root, f)) else 0)
            fpath = os.path.join(root, largest)
            try: fsize = os.path.getsize(fpath)
            except: fsize = 0
            if fsize < 50_000_000:  # Skip < 50MB
                continue
            # Extract title/year from directory name or filename
            dirname = os.path.basename(root)
            name_source = dirname if dirname not in ("VIDEO_TS", "BDMV") else os.path.basename(os.path.dirname(root))
            year_match = year_re.search(name_source)
            year = year_match.group(1) if year_match else ""
            # Clean title: remove year, codec info, resolution
            title = year_re.sub(" ", name_source)
            title = re.sub(r"[\._]", " ", title)
            title = re.sub(r"\b(720p|1080p|2160p|4k|bluray|blu-ray|dvdrip|webrip|web-dl|hdtv|x264|x265|hevc|aac|ac3|dts)\b", "", title, flags=re.I)
            title = re.sub(r"\s+", " ", title).strip()

            results.append({
                "path": unmap_path(fpath, config),
                "dir_path": unmap_path(root, config),
                "title": title,
                "year": year,
                "imdb_id": nfo_iid,
                "size": fsize,
                "filename": largest,
            })
    log(f"[scan_extra] Found {len(results)} non-TMM titles in {len(base_paths)} dirs")
    return results


# --- Contact Sheet Generation ---
def cleanup_dir(dirpath, dry_run=True):
    """After deleting a video file, check if remaining files are junk and remove the directory.
    
    Auto-delete: .txt <10KB, .nfo, .srt, .sub, .ass, .ssa, .smi, .vtt,
                 .jpg, .png, .url, .html <1KB, .mp4 <5MB, thumbs.db,
                 SYNOINDEX_MEDIA_INFO, .db <1MB
    Keep (abort cleanup): .rar, .pdf, .epub, .mp3, .flac, .m4b, .iso,
                          large .mp4 (>5MB that aren't RARBG-style),
                          any video file
    """
    if not os.path.isdir(dirpath):
        return {"action": "skip", "reason": "not a directory"}
    
    vexts = (".mkv", ".mp4", ".avi", ".m4v", ".ts", ".m2ts", ".wmv")
    junk_exts = (".nfo", ".srt", ".sub", ".ass", ".ssa", ".smi", ".vtt",
                 ".jpg", ".png", ".url")
    keep_exts = (".rar", ".pdf", ".epub", ".mp3", ".flac", ".m4b", ".iso",
                 ".m4a", ".ogg", ".wav", ".zip", ".7z")
    
    files = os.listdir(dirpath)
    to_delete = []
    has_video = False
    has_keeper = False
    
    for f in files:
        fp = os.path.join(dirpath, f)
        if not os.path.isfile(fp):
            continue
        fl = f.lower()
        sz = os.path.getsize(fp)
        
        # Video files still present = don't touch this dir
        if fl.endswith(vexts) and sz > 5_000_000:
            has_video = True
            break
        # Small mp4 (<5MB) = RARBG promo, junk
        elif fl.endswith(".mp4") and sz <= 5_000_000:
            to_delete.append(fp)
        # Known junk extensions
        elif fl.endswith(junk_exts):
            to_delete.append(fp)
        # Small txt/html = tracker spam
        elif fl.endswith(".txt") and sz < 10_000:
            to_delete.append(fp)
        elif fl.endswith(".html") and sz < 1_000:
            to_delete.append(fp)
        # Synology/thumbs cache
        elif fl in ("thumbs.db", ".ds_store") or "synoindex" in fl.lower():
            to_delete.append(fp)
        elif fl.endswith(".db") and sz < 1_000_000:
            to_delete.append(fp)
        # Keepers = abort
        elif fl.endswith(keep_exts) or sz > 5_000_000:
            has_keeper = True
    
    if has_video:
        return {"action": "skip", "reason": "video files still present"}
    if has_keeper:
        return {"action": "skip", "reason": "contains non-junk files (pdf/rar/audio/large)", "kept": [f for f in files if os.path.isfile(os.path.join(dirpath, f))]}
    
    if dry_run:
        return {"action": "dry_run", "would_delete": len(to_delete), "files": [os.path.basename(f) for f in to_delete]}
    
    for fp in to_delete:
        os.remove(fp)
    # Remove any remaining empty subdirs then the dir itself
    for root, dirs, fls in os.walk(dirpath, topdown=False):
        for d in dirs:
            try: os.rmdir(os.path.join(root, d))
            except: pass
    try:
        if not os.listdir(dirpath):
            os.rmdir(dirpath)
            return {"action": "deleted", "removed": len(to_delete)}
    except: pass
    return {"action": "cleaned", "removed": len(to_delete), "remaining": os.listdir(dirpath) if os.path.isdir(dirpath) else []}


def generate_contact_sheet(filepath, output_path, cols=4, rows=4, width=1920):
    """Generate a contact sheet (thumbnail grid) using fast keyframe seeking per frame."""
    import tempfile, glob
    try:
        r = subprocess.run(
            ["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", filepath],
            capture_output=True, timeout=15, text=True)
        duration = float(r.stdout.strip()) if r.stdout.strip() else 0
        if duration < 60:
            return {"error": "video too short"}
        n_frames = cols * rows
        interval = duration / (n_frames + 1)
        thumb_w = width // cols
        tmpdir = tempfile.mkdtemp(prefix="cs_")
        # Extract individual frames with fast seeking
        for i in range(n_frames):
            seek = int((i + 1) * interval)
            subprocess.run([
                "ffmpeg", "-y", "-ss", str(seek), "-i", filepath,
                "-vframes", "1", "-q:v", "5", "-vf", f"scale={thumb_w}:-1",
                os.path.join(tmpdir, f"f{i:02d}.jpg")
            ], capture_output=True, timeout=30)
        # Tile frames using ffmpeg
        frames = sorted(glob.glob(os.path.join(tmpdir, "f*.jpg")))
        if len(frames) < 2:
            import shutil; shutil.rmtree(tmpdir, ignore_errors=True)
            return {"error": f"only {len(frames)} frames extracted"}
        # Build ffmpeg concat filter
        args = ["ffmpeg", "-y"]
        for f in frames:
            args += ["-i", f]
        filter_str = ""
        for i in range(len(frames)):
            filter_str += f"[{i}:v]"
        filter_str += f"xstack=inputs={len(frames)}:layout="
        layouts = []
        h = int(thumb_w * 9 / 16)  # Approximate height
        for i in range(len(frames)):
            layouts.append(f"{(i%cols)*thumb_w}_{(i//cols)*h}")
        filter_str += "|".join(layouts)
        args += ["-filter_complex", filter_str, "-q:v", "5", output_path]
        subprocess.run(args, capture_output=True, timeout=30)
        import shutil; shutil.rmtree(tmpdir, ignore_errors=True)
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            return {"path": output_path, "size": os.path.getsize(output_path), "frames": len(frames)}
        return {"error": "tiling failed"}
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    except Exception as e:
        return {"error": str(e)}


def check_prerequisites():
    """Check system prerequisites and guide user through setup."""
    import shutil
    issues = []

    # Check ffmpeg (try both name and .exe, also try running it)
    ffmpeg_ok = shutil.which("ffmpeg") or shutil.which("ffmpeg.exe")
    if not ffmpeg_ok:
        try:
            import subprocess
            subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
            ffmpeg_ok = True
        except: pass
    if not ffmpeg_ok:
        issues.append(("ffmpeg", "Optional - needed for video thumbnails"))

    if issues:
        print("\n!! Missing components:")
        for name, reason in issues:
            print(f"  FAIL {name} - {reason}")
        if os.name == "nt":
            print("\nRun the setup script (it will request admin privileges automatically):")
            print("  powershell -ExecutionPolicy Bypass -File setup-windows.ps1")
            resp = input("\nContinue without them? [y/N] ").strip().lower()
            if resp != "y":
                sys.exit(0)
        else:
            print("\nInstall with: sudo apt install ffmpeg nfs-common  (or equivalent)")
        print("")

def check_path_access(config, library):
    """Check file paths by discovering unique roots and testing each."""
    roots = {}
    for iid, val in list(library.items()):
        if iid == "_episodes": continue
        entries = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
        for info in entries:
            if not isinstance(info, dict): continue
            path = info.get("path", "")
            if not path: continue
            # Strip stack:// prefix
            if path.startswith("stack://"):
                path = path.replace("stack://", "").split(" , ")[0].strip()
            parts = path.replace("\\", "/").split("/")
            root = "/".join(parts[:4]) if len(parts) >= 4 else path
            if root not in roots:
                roots[root] = []
            if len(roots[root]) < 2:
                roots[root].append(path)
    if not roots:
        return
    print(f"  Path roots found: {len(roots)}")
    all_ok = True
    for root, samples in roots.items():
        ok = 0
        for s in samples:
            mapped = map_path(s, config)
            if os.name == "nt":
                mapped = mapped.replace("/", "\\")
            if os.path.exists(mapped):
                ok += 1
        icon = "+" if ok == len(samples) else "-"
        mapped_root = map_path(root, config)
        print(f"    {icon} {root} -> {mapped_root} ({ok}/{len(samples)} accessible)")
        if ok < len(samples):
            all_ok = False
            for s in samples:
                mapped = map_path(s, config)
                if os.name == "nt":
                    mapped = mapped.replace("/", "\\")
                print(f"      {s[:70]}")
                print(f"      -> {mapped[:70]} (exists: {os.path.exists(mapped)})")
    if all_ok:
        print("  All roots accessible")
        return
    resp = input("  Fix a path mapping? [y/N] ").strip().lower()
    while resp == "y":
        remote = input("    Kodi path prefix: ").strip()
        local = input("    Local path prefix: ").strip()
        if remote and local:
            mappings = config.get("_path_mappings", {})
            mappings[remote] = local
            config["_path_mappings"] = mappings
            json.dump(config, open(CONFIG_FILE, "w"), indent=2)
            print(f"    Saved: {remote} -> {local}")
        resp = input("  Fix another? [y/N] ").strip().lower()
    print("")


def daemon_mode(args, config):
    """Run as daemon: Kodi sync + server task polling in parallel."""
    import threading
    
    base_url = args.server.rstrip("/")
    config["_server"] = base_url
    headers = {"Content-Type": "application/json", "User-Agent": "CinephileAgent/2.0"}
    
    def sync_loop():
        """Periodic Kodi/media server sync with NFS health check and incremental scan."""
        while True:
            try:
                # NFS health check first
                healthy = check_mounts(config)
                if healthy:
                    log(f"[nfs] {len(healthy)} mounts healthy")
                else:
                    log("[nfs] No healthy mounts! Skipping sync.")
                    time.sleep(300)
                    continue

                library = {}
                tmm_cfg = config.get("tmm", {})
                if tmm_cfg.get("enabled"):
                    # Use incremental scan
                    scan_paths = tmm_cfg.get("paths", [])
                    if not scan_paths and tmm_cfg.get("path"):
                        scan_paths = [tmm_cfg["path"]]
                    # Filter to only paths on healthy mounts
                    scan_paths = [p for p in scan_paths if any(p.startswith(h) for h in healthy)]
                    if scan_paths:
                        items = incremental_nfo_scan(scan_paths)
                        library.update(items)
                        log(f"[sync] tmm: {len(items)} titles")

                # Other fetchers (Plex, Kodi, etc.)
                for name, cfg in config.items():
                    if name == "tmm": continue
                    if not isinstance(cfg, dict) or not cfg.get("enabled"): continue
                    if name not in FETCHERS: continue
                    try:
                        items = FETCHERS[name](cfg)
                        library.update(items)
                        log(f"[sync] {name}: {len(items)} titles")
                    except Exception as e:
                        log(f"[sync] {name} error: {e}")

                if library:
                    api_post(f"{base_url}/api/library/{args.user}", {"library": library})
                    log(f"[sync] Pushed {len(library)} titles")

                # Also scan non-TMM directories and report
                try:
                    extra = scan_non_tmm(config)
                    if extra:
                        api_post(f"{base_url}/api/library/{args.user}/incoming", {"files": extra})
                        log(f"[sync] Reported {len(extra)} non-TMM titles")
                except Exception as e:
                    log(f"[sync] non-TMM scan error: {e}")

            except Exception as e:
                log(f"[sync] Error: {e}")
                time.sleep(60)
                continue
            time.sleep(14400)  # Every 4 hours
    
    _start_time = time.time()

    _bg_task = {"running": False, "id": None, "cancel": False, "thread": None}

    def report_result(tid, result):
        """Report task completion to server."""
        try:
            req = urllib.request.Request(
                f"{base_url}/api/tasks/complete/{tid}",
                data=json.dumps({"result": result}).encode(), headers=headers)
            urllib.request.urlopen(req, timeout=30)
            log(f"[task] Done: {tid}")
        except Exception as e:
            log(f"[task] Report failed ({tid}): {e}")
            buffer_result(tid, result)

    def run_bg_task(tid, ttype, params):
        """Run a long task (exec_code) in background thread."""
        _bg_task["running"] = True
        _bg_task["id"] = tid
        _bg_task["cancel"] = False
        log(f"[bg] Starting {ttype} ({tid})")
        t0 = time.time()
        try:
            result = run_task(ttype, params, config)
            elapsed = time.time() - t0
            log(f"[bg] Done in {elapsed:.0f}s ({tid})")
            report_result(tid, result)
        except Exception as e:
            log(f"[bg] Error: {e}")
            report_result(tid, {"error": str(e)})
        _bg_task["running"] = False
        _bg_task["id"] = None
        log(f"[bg] Finished {tid}")

    def task_loop():
        consecutive_errors = 0
        """Poll server for tasks and execute them."""
        while True:
            try:
                # Handshake: report status
                try:
                    import base64
                    status = json.dumps({
                        "agent_version": AGENT_VERSION,
                        "last_activity": _last_activity,
                        "recent_logs": get_recent_logs(10),
                        "uptime": int(time.time() - _start_time),
                        "consecutive_errors": consecutive_errors,
                        "bg_task": _bg_task.get("id"),
                    })
                    encoded = base64.b64encode(status.encode()).decode()
                    sreq = urllib.request.Request(f"{base_url}/api/agent_status?s={encoded}", headers=headers)
                    urllib.request.urlopen(sreq, timeout=5)
                except: pass
                flush_buffer(base_url, headers)
                # Fetch tasks
                req = urllib.request.Request(f"{base_url}/api/tasks", headers=headers)
                resp = urllib.request.urlopen(req, timeout=10)
                tasks = json.loads(resp.read()).get("tasks", [])

                for task in tasks:
                    tid, ttype = task["id"], task["type"]
                    params = task.get("params", {})
                    # Skip if this task is already running in background
                    if tid == _bg_task.get("id"):
                        continue
                    # Long-running tasks: run in background thread
                    if ttype == "exec_code":
                        if _bg_task["running"]:
                            if tid == _bg_task["id"]:
                                continue  # same task, skip
                            # New exec_code: cancel the old one
                            log(f"[task] Cancelling bg task {_bg_task['id']} for {tid}")
                            _bg_task["cancel"] = True
                            if _bg_task.get("thread"):
                                _bg_task["thread"].join(timeout=5)
                            _bg_task["running"] = False
                        t = threading.Thread(target=run_bg_task, args=(tid, ttype, params), daemon=True)
                        _bg_task["thread"] = t
                        t.start()
                        continue
                    # Normal tasks: run in parallel (up to 3 concurrent)
                    log(f"[task] {ttype} ({tid})")
                    _last_activity["task"] = ttype
                    _last_activity["time"] = time.strftime("%H:%M:%S")
                    t0 = time.time()
                    result = run_task(ttype, params, config)
                    elapsed = time.time() - t0
                    log(f"[task] Done {ttype} in {elapsed:.1f}s ({tid})")
                    report_result(tid, result)
                    if result.get("_restart"):
                        sys.exit(42)
                    # Process all available tasks per cycle
            except Exception as e:
                if consecutive_errors == 0:
                    log(f"[task] Connection lost: {e}")
                consecutive_errors += 1
                _last_activity["errors"] = consecutive_errors
                wait = min(15 * (2 ** min(consecutive_errors - 1, 2)), 60)
                if consecutive_errors % 4 == 0:
                    log(f"[task] Still retrying... ({consecutive_errors} attempts)")
                time.sleep(wait)
                continue
            if consecutive_errors > 0:
                log(f"[task] Reconnected after {consecutive_errors} errors")
            consecutive_errors = 0
            _last_activity["errors"] = 0
            time.sleep(5)  # Poll every 5s when idle
    
    log(f"Agent daemon - server: {base_url}, user: {args.user}")
    log(f"  Sync thread: every 4 hours")
    log(f"  Task thread: every 15 sec")
    
    threading.Thread(target=sync_loop, daemon=True).start()
    task_loop()  # Run task loop in main thread

def _safe_stat(path, timeout=5):
    """os.path.getsize with timeout to avoid SMB hangs. Resolves dirs to largest video file."""
    import threading
    result = [None]
    def _do():
        try:
            if os.path.isdir(path):
                vexts = (".mkv", ".mp4", ".avi", ".m4v", ".ts", ".vob", ".iso")
                best = 0
                for f in os.listdir(path):
                    if f.lower().endswith(vexts):
                        sz = os.path.getsize(os.path.join(path, f))
                        if sz > best: best = sz
                # Also check VIDEO_TS subfolder
                vts = os.path.join(path, "VIDEO_TS")
                if os.path.isdir(vts):
                    for f in os.listdir(vts):
                        if f.lower().endswith(".vob"):
                            best += os.path.getsize(os.path.join(vts, f))
                result[0] = best if best > 0 else None
            else:
                result[0] = os.path.getsize(path)
        except: pass
    t = threading.Thread(target=_do, daemon=True)
    t.start()
    t.join(timeout)
    return result[0]

def run_task(ttype, params, config):
    """Execute a single task from the server."""
    try:
        if ttype == "size_files":
            paths = params.get("paths", [])
            mapped = [(p, map_path(p, config)) for p in paths]
            data = {}
            if os.name == "nt" and mapped:
                # Batch via PowerShell Get-Item - one process for all files
                mp_list = [mp for _, mp in mapped]
                try:
                    # Build PowerShell command
                    ps_paths = ",".join("'" + m.replace("'", "''") + "'" for m in mp_list)
                    cmd = ["powershell", "-c",
                        "Get-Item -LiteralPath " + ps_paths +
                        " -ErrorAction SilentlyContinue | Select-Object FullName,Length | ConvertTo-Json -Compress"]
                    out = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                    if out.stdout.strip():
                        import json as _j
                        results = _j.loads(out.stdout)
                        if isinstance(results, dict): results = [results]
                        size_map = {}
                        for r in results:
                            if r.get("Length"):
                                size_map[r["FullName"]] = r["Length"]
                        for orig, mp in mapped:
                            # Try exact match and normalized match
                            sz = size_map.get(mp) or size_map.get(mp.replace("/", os.sep))
                            if sz: data[orig] = sz
                    log(f"[size] PowerShell batch: {len(data)}/{len(paths)} sized")
                except Exception as e:
                    log(f"[size] PowerShell failed: {e}, falling back")
                    for orig, mp in mapped:
                        sz = _safe_stat(mp)
                        if sz: data[orig] = sz
            else:
                for orig, mp in mapped:
                    sz = _safe_stat(mp)
                    if sz: data[orig] = sz
            return {"sized": len(data), "data": data}
        elif ttype == "hash_files":
            paths = params.get("paths", [])
            data = {}
            for p in paths:
                mp = map_path(p, config)
                try:
                    h = opensubtitles_hash(mp)
                    if h:
                        sz = _safe_stat(mp)
                        if sz: data[p] = {"hash": h, "size": sz}
                except: pass
            return {"hashed": len(data), "data": data}
        
        elif ttype == "download_subs":
            imdb_id = params.get("imdb_id", "")
            path = params.get("path", "")
            languages = params.get("languages", ["eng", "fre"])
            if isinstance(languages, str): languages = [languages]
            if not imdb_id or not path:
                return {"error": "missing imdb_id or path"}
            mp = map_path(path, config)
            # Resolve dir to video file
            if os.path.isdir(mp):
                vexts = (".mkv", ".mp4", ".avi", ".m4v")
                vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                          for f in os.listdir(mp) if f.lower().endswith(vexts)]
                if vfiles:
                    vfiles.sort(reverse=True)
                    mp = vfiles[0][1]
            if not os.path.isfile(mp):
                return {"error": f"file not found: {mp[:60]}"}
            file_hash = opensubtitles_hash(mp)
            file_size = os.path.getsize(mp)
            video_base = mp.rsplit(".", 1)[0]  # /path/to/Movie_Name
            os_user = params.get("os_user", "") or config.get("_opensubs_user", "")
            os_pass = params.get("os_pass", "") or config.get("_opensubs_pass", "")
            os_key = params.get("os_key", "") or config.get("_opensubs_key", "")
            downloaded = []
            for language in languages:
                # Check if sub already exists
                lang_short = {"eng": "en", "fre": "fr", "fra": "fr", "ger": "de", "deu": "de", "spa": "es"}.get(language, language[:2])
                srt_path = f"{video_base}.{lang_short}.srt"
                if os.path.exists(srt_path):
                    downloaded.append({"lang": language, "status": "exists", "path": srt_path})
                    continue
                subs_found = []
                try:
                    import xmlrpc.client
                    server = xmlrpc.client.ServerProxy("https://api.opensubtitles.org/xml-rpc")
                    login = server.LogIn(os_user, os_pass, language, "CinephileCrossroads v2.1")
                    if login.get("status", "").startswith("200"):
                        token = login["token"]
                        search_params = []
                        if file_hash: search_params.append({"moviehash": file_hash, "moviebytesize": str(file_size), "sublanguageid": language})
                        search_params.append({"imdbid": imdb_id.replace("tt",""), "sublanguageid": language})
                        results = server.SearchSubtitles(token, search_params)
                        if results.get("data"):
                            # Pick best: prefer hash match, then highest rating
                            best = sorted(results["data"], key=lambda s: (s.get("MatchedBy","") == "moviehash", float(s.get("SubRating","0"))), reverse=True)[0]
                            # Download
                            import gzip
                            sub_url = best.get("SubDownloadLink", "")
                            if sub_url:
                                req = urllib.request.Request(sub_url, headers={"User-Agent": "CinephileAgent/2.0"})
                                resp = urllib.request.urlopen(req, timeout=15)
                                sub_data = gzip.decompress(resp.read()).decode("utf-8", errors="replace")
                                with open(srt_path, "w", encoding="utf-8") as f:
                                    f.write(sub_data)
                                downloaded.append({"lang": language, "status": "downloaded", "path": srt_path, "matched_by": best.get("MatchedBy","")})
                                log(f"[subs] {os.path.basename(srt_path)} ({best.get('MatchedBy','')})")
                            else:
                                downloaded.append({"lang": language, "status": "no_url"})
                        else:
                            downloaded.append({"lang": language, "status": "not_found"})
                        server.LogOut(token)
                    else:
                        downloaded.append({"lang": language, "status": f"login_failed: {login.get('status','')}"})
                except Exception as e:
                    downloaded.append({"lang": language, "status": f"error: {str(e)[:50]}"})
            return {"imdb_id": imdb_id, "path": path, "results": downloaded}
        
        elif ttype == "sync_subs":
            # Sync subtitle timing to video using alass
            # Skips if subtitle is already well-synced (<500ms offset)
            path = params.get("path", "")
            sub_path = params.get("sub_path", "")
            mp = map_path(path, config)
            if os.path.isdir(mp):
                vexts = (".mkv", ".mp4", ".avi", ".m4v")
                vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                          for f in os.listdir(mp) if f.lower().endswith(vexts)]
                if vfiles:
                    vfiles.sort(reverse=True)
                    mp = vfiles[0][1]
            if not os.path.isfile(mp):
                return {"error": f"video not found: {mp[:60]}"}
            # Find srt files to sync
            video_base = mp.rsplit(".", 1)[0]
            if sub_path:
                srt_files = [map_path(sub_path, config)]
            else:
                srt_files = [os.path.join(os.path.dirname(mp), f)
                             for f in os.listdir(os.path.dirname(mp))
                             if f.endswith(".srt") and f.startswith(os.path.basename(video_base))]
            synced = []
            skipped = []
            for srt in srt_files:
                if not os.path.isfile(srt): continue
                out = srt + ".tmp_synced"
                r = subprocess.run(["alass", mp, srt, out], capture_output=True, timeout=120, text=True)
                if r.returncode != 0 or not os.path.exists(out):
                    continue
                # Check if sync made a meaningful difference
                # Compare timestamps at start, middle, end to detect drift/splits
                def get_timestamps(path):
                    import re
                    ts = []
                    with open(path, "r", encoding="utf-8", errors="replace") as f:
                        for line in f:
                            m = re.match(r"(\d{2}):(\d{2}):(\d{2}),(\d{3})", line)
                            if m:
                                ts.append(int(m.group(1))*3600000 + int(m.group(2))*60000 + int(m.group(3))*1000 + int(m.group(4)))
                    return ts
                orig_ts = get_timestamps(srt)
                new_ts = get_timestamps(out)
                if not orig_ts or not new_ts or len(orig_ts) != len(new_ts):
                    # Structure changed (splits added/removed) — use synced version
                    os.replace(out, srt)
                    synced.append({"file": os.path.basename(srt), "reason": "structure_changed"})
                    log(f"[sync] {os.path.basename(srt)} — structure changed")
                    continue
                # Sample shifts at 10%, 50%, 90% through the file
                samples = [int(len(orig_ts) * p) for p in (0.1, 0.5, 0.9)]
                shifts = [abs(orig_ts[i] - new_ts[i]) for i in samples if i < len(orig_ts)]
                max_shift = max(shifts) if shifts else 0
                drift = abs(shifts[-1] - shifts[0]) if len(shifts) >= 2 else 0
                if max_shift < 500 and drift < 200:
                    # Already well-synced everywhere, no drift
                    os.remove(out)
                    skipped.append(os.path.basename(srt))
                else:
                    os.replace(out, srt)
                    synced.append({"file": os.path.basename(srt), "max_shift_ms": max_shift, "drift_ms": drift})
                    log(f"[sync] {os.path.basename(srt)} shift={max_shift}ms drift={drift}ms")
            return {"synced": len(synced), "skipped": len(skipped), "details": synced, "already_ok": skipped}

        elif ttype == "search_upgrade":
            # Add to Radarr/Sonarr and trigger search for better quality
            is_tv = params.get("is_tv", False)
            tmdb_id = params.get("tmdb_id", 0)
            tvdb_id = params.get("tvdb_id", 0)
            title = params.get("title", "")
            year = params.get("year", "")
            if is_tv:
                url = config.get("sonarr", {}).get("url", "http://localhost:8989")
                key = config.get("sonarr", {}).get("token", "")
                if not key or not tvdb_id:
                    return {"error": "sonarr not configured or no tvdb_id"}
                payload = json.dumps({"tvdbId": int(tvdb_id), "title": title, "qualityProfileId": 4,
                    "rootFolderPath": "/tv", "monitored": True, "addOptions": {"searchForMissingEpisodes": True}})
            else:
                url = config.get("radarr", {}).get("url", "http://localhost:7878")
                key = config.get("radarr", {}).get("token", "")
                if not key or not tmdb_id:
                    return {"error": "radarr not configured or no tmdb_id"}
                payload = json.dumps({"tmdbId": int(tmdb_id), "title": title, "year": int(year) if year else 0,
                    "qualityProfileId": 4, "rootFolderPath": "/movies", "monitored": True,
                    "addOptions": {"searchForMovie": True}})
            endpoint = f"{url}/api/v3/{'series' if is_tv else 'movie'}"
            req = urllib.request.Request(endpoint, data=payload.encode(),
                headers={"X-Api-Key": key, "Content-Type": "application/json"})
            try:
                resp = urllib.request.urlopen(req, timeout=15)
                result = json.loads(resp.read())
                log(f"[upgrade] {'Sonarr' if is_tv else 'Radarr'}: added {title} ({year})")
                return {"status": "added", "title": title, "id": result.get("id")}
            except urllib.error.HTTPError as e:
                body = e.read().decode()
                if "already been added" in body.lower() or "exists" in body.lower():
                    # Already in Radarr/Sonarr — trigger manual search instead
                    log(f"[upgrade] {title} already in {'Sonarr' if is_tv else 'Radarr'}, triggering search")
                    return {"status": "already_exists", "title": title}
                return {"error": f"HTTP {e.code}: {body[:100]}"}

        elif ttype == "verify_stills":
            # Extract a frame from the video and compare against TMDB stills via perceptual hash
            # params: path, stills (list of TMDB still URLs), imdb_id
            path = params.get("path", "")
            stills = params.get("stills", [])
            imdb_id = params.get("imdb_id", "")
            mp = map_path(path, config)
            if os.path.isdir(mp):
                vexts = (".mkv", ".mp4", ".avi", ".m4v")
                vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                          for f in os.listdir(mp) if f.lower().endswith(vexts)]
                if vfiles:
                    vfiles.sort(reverse=True)
                    mp = vfiles[0][1]
            if not os.path.isfile(mp):
                return {"error": "file not found"}
            if not stills:
                return {"error": "no stills to compare"}
            # Extract frame at 30% of duration
            r = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", mp],
                capture_output=True, timeout=15, text=True)
            duration = float(r.stdout.strip()) if r.stdout.strip() else 0
            seek = int(duration * 0.3) if duration > 60 else 30
            import tempfile, hashlib
            tmp = tempfile.mktemp(suffix=".jpg")
            subprocess.run(["ffmpeg", "-y", "-ss", str(seek), "-i", mp, "-vframes", "1", "-vf", "scale=160:-1", "-q:v", "5", tmp],
                capture_output=True, timeout=15)
            if not os.path.exists(tmp):
                return {"error": "frame extraction failed"}
            # Simple perceptual hash: resize to 8x8, grayscale, compare average
            def phash_file(filepath):
                r = subprocess.run(["ffmpeg", "-y", "-i", filepath, "-vf", "scale=8:8,format=gray", "-f", "rawvideo", "-"],
                    capture_output=True, timeout=10)
                if len(r.stdout) < 64: return 0
                pixels = list(r.stdout[:64])
                avg = sum(pixels) / 64
                return int("".join("1" if p > avg else "0" for p in pixels), 2)
            local_hash = phash_file(tmp)
            os.remove(tmp)
            # Compare against TMDB stills
            best_match = 0
            best_still = ""
            for still_url in stills[:3]:
                try:
                    tmp_still = tempfile.mktemp(suffix=".jpg")
                    urllib.request.urlretrieve(still_url, tmp_still)
                    still_hash = phash_file(tmp_still)
                    os.remove(tmp_still)
                    # Hamming distance
                    xor = local_hash ^ still_hash
                    distance = bin(xor).count("1")
                    similarity = max(0, 100 - distance * 100 // 64)
                    if similarity > best_match:
                        best_match = similarity
                        best_still = still_url
                except:
                    continue
            return {"imdb_id": imdb_id, "similarity": best_match, "best_still": best_still,
                    "confirmed": best_match > 60, "path": path}

        elif ttype == "check_quality":
            paths = params.get("paths", [])
            genres = params.get("genres", "")
            data = {}
            for p in paths:
                mp = map_path(p, config)
                if os.path.isdir(mp):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".ts", ".m2ts")
                    vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                              for f in os.listdir(mp) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        mp = vfiles[0][1]
                if os.path.isfile(mp):
                    mi = extract_mediainfo(mp)
                    if "error" not in mi:
                        score = compute_quality_score(mi, genres)
                        mi["bpp"] = score.get("bpp", 0)
                        mi["quality_verdict"] = score.get("verdict", "")
                        mi["effective_height"] = score.get("effective_height", 0)
                    data[p] = mi
                else:
                    data[p] = {"exists": False}
            return {"checked": len(data), "data": data}
        
        elif ttype == "find_duplicates":
            paths = params.get("paths", [])
            sizes = {}
            for p in paths:
                mp = map_path(p, config)
                if os.path.isfile(mp):
                    s = os.path.getsize(mp)
                    sizes.setdefault(s, []).append(p)
            dupes = {str(s): ps for s, ps in sizes.items() if len(ps) > 1}
            return {"duplicates": len(dupes), "data": dupes}
        
        elif ttype == "exec_code":
            # Execute arbitrary Python code from the server
            code = params.get("code", "")
            if not code: return {"error": "no code"}
            def _progress(msg):
                print(f"\r[exec] {msg}    ", end="", flush=True)
                _last_activity["task"] = f"exec: {msg}"
                _last_activity["time"] = time.strftime("%H:%M:%S")
            local_vars = {"config": config, "base_url": config.get("_server", ""), "cancelled": lambda: _bg_task.get("cancel", False), "agent_headers": {"Content-Type": "application/json", "User-Agent": "CinephileAgent/2.0"}, "os": os, "json": json, "re": __import__("re"), "subprocess": __import__("subprocess"), "base64": __import__("base64"),
                          "result": {}, "progress": _progress, "log": log}
            exec(code, local_vars)
            print()  # newline after \r progress
            return {"output": "", "result": local_vars.get("result", {})}
        
        elif ttype == "update_agent":
            # Hot-update: server sends new agent code
            code = params.get("code", "")
            path = params.get("path", "")
            if code and path:
                # Backup current
                if os.path.exists(path):
                    import shutil
                    shutil.copy(path, path + ".bak")
                with open(path, "w") as f:
                    f.write(code)
                log(f"[update] Agent updated ({len(code)} bytes), restarting...")
                # Report result before exiting
                return {"updated": path, "size": len(code), "_restart": True}
            return {"error": "missing code or path"}
        
        elif ttype == "scan_incoming":
            # Scan incoming folder for new video files
            incoming = params.get("path", "")
            min_size = params.get("min_size", 50000000)  # 50MB default
            mp = map_path(incoming, config)
            # Ensure proper UNC path on Windows
            if os.name == "nt" and mp.startswith("\\"): pass  # already good
            elif os.name == "nt" and mp.startswith("//"): mp = mp.replace("/", os.sep)
            log(f"[incoming] Path: {incoming} -> {repr(mp)}")
            log(f"[incoming] Exists: {os.path.exists(mp)}")
            if os.path.exists(mp):
                try:
                    top = os.listdir(mp)
                    log(f"[incoming] Top-level: {len(top)} items")
                except Exception as e:
                    log(f"[incoming] listdir error: {e}")
            found = []
            walk_dirs = 0
            walk_files = 0
            for root, dirs, files in os.walk(mp):
                try:
                    walk_dirs += 1
                    walk_files += len(files)
                    if walk_dirs <= 5:
                        log(f"[incoming] Walk: {root[-50:]} d={len(dirs)} f={len(files)}")
                    for f in files:
                        if f.lower().endswith((".mkv", ".mp4", ".avi", ".m4v", ".ts")):
                            fp = os.path.join(root, f)
                            try: sz = os.path.getsize(fp)
                            except: sz = 0
                            if sz > min_size:
                                nfs = unmap_path(fp, config)
                                found.append({"path": nfs, "filename": f, "size": sz})
                                log(f"[incoming] {f} ({sz/1073741824:.1f} GB)")
                except Exception as e:
                    log(f"[incoming] Walk error in {root[-30:]}: {e}")
            log(f"[incoming] Walk: {walk_dirs} dirs, {walk_files} total files, {len(found)} video >min_size")
            return {"files": found, "data": {"files": found}}

        elif ttype == "move_file":
            # Move/rename a file from source to destination
            src = map_path(params.get("source", ""), config)
            dst = map_path(params.get("destination", ""), config)
            if not src or not dst:
                return {"error": "missing source or destination"}
            try:
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                import shutil
                shutil.move(src, dst)
                log(f"[move] {os.path.basename(src)} -> {dst}")
                return {"moved": True, "source": params["source"], "destination": params["destination"]}
            except Exception as e:
                return {"error": str(e)}

        elif ttype == "delete_file":
            # Delete a file (with confirmation token)
            path = map_path(params.get("path", ""), config)
            confirm = params.get("confirm", "")
            if confirm != "yes_delete":
                return {"error": "missing confirmation token"}
            try:
                if os.path.isfile(path):
                    os.remove(path)
                    parent = os.path.dirname(path)
                    # Auto-cleanup junk in the directory
                    cleanup = cleanup_dir(parent, dry_run=False)
                    log(f"[delete] {os.path.basename(path)} (dir: {cleanup.get('action','')})")
                    return {"deleted": True, "path": params["path"], "dir_cleanup": cleanup}
                return {"error": "file not found"}
            except Exception as e:
                return {"error": str(e)}

        elif ttype == "cleanup_dir":
            # Clean up a directory if it only contains junk files
            path = map_path(params.get("path", ""), config)
            dry_run = params.get("dry_run", True)
            return cleanup_dir(path, dry_run=dry_run)

        elif ttype == "merge_audio":
            # Merge superior audio track from one file into another
            # params: target (1080p file), source (720p file with better audio)
            # Optional: dry_run (default True), delete_source (default False)
            target_path = map_path(params.get("target", ""), config)
            source_path = map_path(params.get("source", ""), config)
            dry_run = params.get("dry_run", True)
            delete_source = params.get("delete_source", False)
            # Resolve dirs to video files
            for p_ref in ("target_path", "source_path"):
                p = locals()[p_ref]
                if os.path.isdir(p):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v")
                    vfiles = [(os.path.getsize(os.path.join(p, f)), os.path.join(p, f))
                              for f in os.listdir(p) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        locals()[p_ref] = vfiles[0][1]
                        if p_ref == "target_path": target_path = vfiles[0][1]
                        else: source_path = vfiles[0][1]
            if not os.path.isfile(target_path):
                return {"error": f"target not found: {target_path[:60]}"}
            if not os.path.isfile(source_path):
                return {"error": f"source not found: {source_path[:60]}"}
            # Get durations to verify they match
            def get_duration(fp):
                r = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", fp],
                    capture_output=True, timeout=30, text=True)
                return float(r.stdout.strip()) if r.stdout.strip() else 0
            t_dur = get_duration(target_path)
            s_dur = get_duration(source_path)
            if t_dur == 0 or s_dur == 0:
                return {"error": "could not determine duration", "target_dur": t_dur, "source_dur": s_dur}
            diff = abs(t_dur - s_dur)
            if diff > 30:
                # Try to find sync offset using first loud peak
                # Extract 60s of audio from each, find first peak above threshold
                def find_first_peak(fp):
                    r = subprocess.run(["ffmpeg", "-i", fp, "-t", "60", "-af", "silencedetect=noise=-30dB:d=0.5", "-f", "null", "-"],
                        capture_output=True, timeout=30, text=True)
                    # Parse silence_end from stderr
                    import re
                    ends = re.findall(r"silence_end: ([\d.]+)", r.stderr)
                    return float(ends[0]) if ends else 0
                t_peak = find_first_peak(target_path)
                s_peak = find_first_peak(source_path)
                offset_ms = int((t_peak - s_peak) * 1000)
                if diff > 300:
                    return {"error": f"duration mismatch too large ({diff:.0f}s)", "target_dur": t_dur, "source_dur": s_dur, "estimated_offset_ms": offset_ms}
            else:
                offset_ms = 0
            # Get audio stream info from source
            r = subprocess.run(["ffprobe", "-v", "quiet", "-show_streams", "-select_streams", "a", "-print_format", "json", source_path],
                capture_output=True, timeout=15, text=True)
            src_audio = json.loads(r.stdout).get("streams", [])
            # Find the best audio track (most channels, best codec)
            best = max(src_audio, key=lambda s: s.get("channels", 0) * 10 + (5 if "dts" in s.get("codec_name","") else 3))
            best_idx = src_audio.index(best)
            info = {
                "target": os.path.basename(target_path),
                "source": os.path.basename(source_path),
                "source_audio": f"{best.get('codec_name','')} {best.get('channels',0)}ch {best.get('tags',{}).get('language','')}",
                "target_duration": round(t_dur),
                "source_duration": round(s_dur),
                "offset_ms": offset_ms,
            }
            if dry_run:
                return {"action": "dry_run", **info}
            # Execute merge with mkvmerge (using SSD buffer for speed)
            buf = get_buffer_path(config)
            local_target = buffer_copy(target_path, config)
            local_source = buffer_copy(source_path, config)
            output = os.path.join(buf, os.path.basename(target_path).rsplit(".", 1)[0] + ".merged.mkv")
            cmd = ["mkvmerge", "-o", output, local_target, "--no-video", "--no-subtitles"]
            if offset_ms:
                cmd += ["--sync", f"0:{offset_ms}"]
            cmd += [local_source]
            r = subprocess.run(cmd, capture_output=True, timeout=600, text=True)
            buffer_cleanup(local_target, local_source)
            if r.returncode not in (0, 1):
                buffer_cleanup(output)
                return {"error": f"mkvmerge failed: {r.stderr[:200]}"}
            if not os.path.exists(output):
                return {"error": "output file not created"}
            # Copy merged file back to NFS and replace original
            import shutil
            dest = target_path.rsplit(".", 1)[0] + ".mkv" if not target_path.endswith(".mkv") else target_path
            orig_size = os.path.getsize(target_path)
            new_size = os.path.getsize(output)
            shutil.copy2(output, dest)
            if dest != target_path:
                os.remove(target_path)
            buffer_cleanup(output)
            result = {"action": "merged", "new_size": new_size, "added_bytes": new_size - orig_size, **info}
            if delete_source:
                os.remove(source_path)
                cleanup_dir(os.path.dirname(source_path), dry_run=False)
                result["source_deleted"] = True
            log(f"[merge] {info['source_audio']} → {os.path.basename(dest)} (offset {offset_ms}ms)")
            return result

        elif ttype == "generate_thumb":
            # Generate thumbnail for a video file
            path = map_path(params.get("path", ""), config)
            seek = params.get("seek", 30)
            try:
                tmp = os.path.join(os.environ.get("TEMP", "/tmp"), "thumb.jpg")
                subprocess.run(["ffmpeg", "-y", "-ss", str(seek), "-i", path,
                    "-vframes", "1", "-q:v", "5", "-vf", "scale=320:-1", tmp],
                    capture_output=True, timeout=30)
                if os.path.exists(tmp):
                    with open(tmp, "rb") as f:
                        thumb_b64 = base64.b64encode(f.read()).decode()
                    os.remove(tmp)
                    return {"thumbnail": thumb_b64, "path": params["path"], "data": {params["path"]: thumb_b64}}
                return {"error": "ffmpeg produced no output"}
            except Exception as e:
                return {"error": str(e)}

        elif ttype == "diag":
            # Diagnostic: return system info
            import platform, shutil
            paths_to_check = params.get("paths", [])
            path_info = {}
            for p in paths_to_check:
                mp = map_path(p, config)
                path_info[p] = {
                    "mapped": mp,
                    "exists": os.path.exists(mp),
                    "is_file": os.path.isfile(mp),
                    "size": os.path.getsize(mp) if os.path.isfile(mp) else 0,
                }
            return {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "cwd": os.getcwd(),
                "agent_path": os.path.abspath(__file__),
                "config_keys": list(config.keys()),
                "path_mappings": config.get("_path_mappings", {}),
                "disk_free": shutil.disk_usage("/").free if hasattr(shutil, "disk_usage") else "?",
                "paths": path_info,
            }

        elif ttype == "mediainfo":
            # Full media analysis via ffprobe
            paths = params.get("paths", [])
            if not paths and params.get("path"):
                paths = [params["path"]]
            data = {}
            for p in paths:
                mp = map_path(p, config)
                # Handle directory paths (find video file inside)
                if os.path.isdir(mp):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".ts", ".m2ts")
                    vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                              for f in os.listdir(mp) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        mp = vfiles[0][1]
                if os.path.isfile(mp):
                    data[p] = extract_mediainfo(mp)
            return {"analyzed": len(data), "data": data}

        elif ttype == "contact_sheet":
            # Generate contact sheet thumbnail grid
            path = params.get("path", "")
            mp = map_path(path, config)
            if os.path.isdir(mp):
                vexts = (".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".ts", ".m2ts")
                vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                          for f in os.listdir(mp) if f.lower().endswith(vexts)]
                if vfiles:
                    vfiles.sort(reverse=True)
                    mp = vfiles[0][1]
            cols = params.get("cols", 4)
            rows = params.get("rows", 4)
            width = params.get("width", 1920)
            tmp = os.path.join("/tmp", f"cs_{os.path.basename(mp)}.jpg")
            result = generate_contact_sheet(mp, tmp, cols, rows, width)
            if "error" not in result and os.path.exists(tmp):
                with open(tmp, "rb") as f:
                    result["image"] = base64.b64encode(f.read()).decode()
                os.remove(tmp)
                result["path"] = path
            return result

        elif ttype == "validate_match":
            # Validate that a file matches its IMDB ID by comparing duration
            # params: paths (list of {path, imdb_id, expected_runtime})
            items = params.get("items", [])
            if params.get("path"):
                items = [{"path": params["path"], "imdb_id": params.get("imdb_id",""), "expected_runtime": params.get("expected_runtime",0)}]
            results = []
            for item in items:
                mp = map_path(item["path"], config)
                if os.path.isdir(mp):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v")
                    vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                              for f in os.listdir(mp) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        mp = vfiles[0][1]
                if not os.path.isfile(mp):
                    results.append({"path": item["path"], "status": "not_found"})
                    continue
                r = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", mp],
                    capture_output=True, timeout=15, text=True)
                actual_s = float(r.stdout.strip()) if r.stdout.strip() else 0
                actual_min = actual_s / 60
                expected = item.get("expected_runtime", 0)
                if expected and actual_min:
                    diff = abs(actual_min - expected)
                    if diff <= 5: status = "ok"
                    elif diff <= 15: status = "possible_variant"  # director's cut etc
                    else: status = "mismatch"
                else:
                    status = "no_reference"
                results.append({"path": item["path"], "imdb_id": item.get("imdb_id",""),
                    "actual_min": round(actual_min, 1), "expected_min": expected,
                    "diff_min": round(abs(actual_min - expected), 1) if expected else None, "status": status})
            flagged = [r for r in results if r["status"] == "mismatch"]
            return {"checked": len(results), "flagged": len(flagged), "data": results}

        elif ttype == "identify_movie":
            # Identify an unknown movie by OCR-ing end credits
            # params: path, duration (optional, speeds up seeking)
            path = params.get("path", "")
            mp = map_path(path, config)
            if os.path.isdir(mp):
                vexts = (".mkv", ".mp4", ".avi", ".m4v", ".vob")
                # Check for VIDEO_TS subfolder
                vts_sub = os.path.join(mp, "VIDEO_TS")
                scan_dir = vts_sub if os.path.isdir(vts_sub) else mp
                vfiles = [(os.path.getsize(os.path.join(scan_dir, f)), os.path.join(scan_dir, f))
                          for f in os.listdir(scan_dir) if f.lower().endswith(vexts)
                          and os.path.getsize(os.path.join(scan_dir, f)) > 10_000_000]
                if vfiles:
                    vfiles.sort(reverse=True)
                    mp = vfiles[0][1]
            if not os.path.isfile(mp):
                return {"error": "file not found"}
            # Get duration
            r = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", mp],
                capture_output=True, timeout=30, text=True)
            duration = float(r.stdout.strip()) if r.stdout.strip() else params.get("duration", 0)
            if duration < 60:
                return {"error": "too short or can't read duration"}
            import tempfile, glob
            tmpdir = tempfile.mkdtemp(prefix="ocr_")
            # Phase 1: First 30 seconds (certification cards: BBFC, CNC, FSK, MPAA)
            for i in range(6):  # 6 frames in first 30s = every 5s
                subprocess.run(["ffmpeg", "-y", "-ss", str(i * 5 + 2), "-i", mp,
                    "-vframes", "1", "-vf", "scale=1920:-1", f"{tmpdir}/h{i:03d}.png"],
                    capture_output=True, timeout=15)
            # Phase 2: Last 10 minutes (end credits)
            credits_start = max(0, duration - 600)
            n_frames = 20  # 20 frames over 10 min
            interval = 600 / n_frames
            for i in range(n_frames):
                t = credits_start + i * interval
                subprocess.run(["ffmpeg", "-y", "-ss", str(int(t)), "-i", mp,
                    "-vframes", "1", "-vf", "scale=1920:-1,negate,eq=contrast=1.5:brightness=0.1", f"{tmpdir}/f{i:03d}.png"],
                    capture_output=True, timeout=15)
            # OCR via Intello batch API (certification mode for opening, credits mode for end)
            all_text = []
            opening_text = []
            intello_url = config.get("_intello_url", "")
            intello_token = config.get("_intello_token", "")
            opening_frames = sorted(glob.glob(f"{tmpdir}/h*.png"))
            credit_frames = sorted(glob.glob(f"{tmpdir}/f*.png"))

            def _intello_batch(frames, mode):
                """Send frames to Intello batch OCR via JSON/base64."""
                if not intello_url or not frames:
                    return []
                all_results = []
                chunk_size = 20
                for chunk_start in range(0, len(frames), chunk_size):
                    chunk = frames[chunk_start:chunk_start + chunk_size]
                    for attempt in range(3):
                        try:
                            frame_data = []
                            for fp in chunk:
                                jpg_path = fp.replace(".png", ".jpg")
                                subprocess.run(["ffmpeg", "-y", "-i", fp, "-q:v", "8", jpg_path],
                                    capture_output=True, timeout=5)
                                src = jpg_path if os.path.exists(jpg_path) else fp
                                with open(src, "rb") as f:
                                    frame_data.append({"filename": os.path.basename(fp), "data": base64.b64encode(f.read()).decode()})
                                if os.path.exists(jpg_path): os.remove(jpg_path)
                            payload = json.dumps({"frames": frame_data, "language": "auto", "mode": mode}).encode()
                            headers = {"Content-Type": "application/json", "User-Agent": "CinephileAgent/2.0"}
                            if intello_token:
                                headers["Authorization"] = f"Bearer {intello_token}"
                            req = urllib.request.Request(f"{intello_url}/api/v1/ocr/batch", data=payload, headers=headers)
                            resp = json.loads(urllib.request.urlopen(req, timeout=120).read())
                            results = resp.get("results", resp if isinstance(resp, list) else [])
                            all_results.extend(results)
                            break
                        except Exception as e:
                            if attempt < 2:
                                time.sleep(5 * (attempt + 1))
                            else:
                                log(f"[ocr] Intello batch failed after 3 attempts: {e}")
                return all_results

            # Try Intello batch first
            opening_results = _intello_batch(opening_frames, "certification")
            credit_results = _intello_batch(credit_frames, "credits")

            if opening_results:
                for r in (opening_results if isinstance(opening_results, list) else opening_results.get("results", [])):
                    text = r.get("text", "") if isinstance(r, dict) else str(r)
                    if text and len(text) > 3:
                        opening_text.append(text)
            if credit_results:
                for r in (credit_results if isinstance(credit_results, list) else credit_results.get("results", [])):
                    text = r.get("text", "") if isinstance(r, dict) else str(r)
                    if text and len(text) > 3:
                        all_text.append(text)

            # Fallback to local Tesseract if Intello returned nothing
            if not opening_text and not all_text:
                for png in opening_frames + credit_frames:
                    is_opening = "/h" in png
                    r = subprocess.run(["tesseract", png, "stdout", "-l", "eng+fra", "--psm", "6"],
                        capture_output=True, timeout=10, text=True)
                    text = r.stdout.strip()
                    if text and len(text) > 3:
                        if is_opening:
                            opening_text.append(text)
                        else:
                            all_text.append(text)
            # Cleanup
            import shutil
            shutil.rmtree(tmpdir, ignore_errors=True)
            # Parse credits text for useful info
            combined = "\n".join(all_text)
            # Extract names and roles
            import re
            # Common credit patterns
            directors = re.findall(r"(?:directed by|director|réalisé par|réalisation)\s*[:\-]?\s*(.+)", combined, re.I)
            actors = re.findall(r"(?:starring|cast|avec|interprétation)\s*[:\-]?\s*(.+)", combined, re.I)
            writers = re.findall(r"(?:written by|screenplay|scénario)\s*[:\-]?\s*(.+)", combined, re.I)
            # Look for title - often in large text at the end or beginning of credits
            # Also extract all capitalized multi-word sequences as potential names
            names = re.findall(r"\b([A-Z][a-z]+ [A-Z][a-z]+(?:\s[A-Z][a-z]+)?)\b", combined)
            # Deduplicate
            names = list(dict.fromkeys(names))[:30]
            return {
                "path": path,
                "duration_min": round(duration / 60, 1),
                "frames_ocr": len(all_text),
                "directors": [d.strip() for d in directors[:3]],
                "actors": [a.strip() for a in actors[:5]],
                "writers": [w.strip() for w in writers[:3]],
                "names_found": names[:20],
                "raw_text_sample": combined[:1500],
                "opening_text": "\n".join(opening_text)[:500],
            }

        elif ttype == "write_nfo_verification":
            # Write OCR verification results into the movie's NFO file
            # params: path, ocr_data (opening_text, names, directors, status)
            path = params.get("path", "")
            ocr_data = params.get("ocr_data", {})
            status = params.get("status", "verified")
            mp = map_path(path, config)
            if os.path.isdir(mp):
                # Find NFO in the directory
                nfos = [f for f in os.listdir(mp) if f.endswith(".nfo")]
                if not nfos:
                    return {"error": "no NFO found"}
                nfo_path = os.path.join(mp, nfos[0])
            elif os.path.isfile(mp):
                nfo_path = mp.rsplit(".", 1)[0] + ".nfo"
            else:
                return {"error": "path not found"}
            if not os.path.exists(nfo_path):
                return {"error": f"NFO not found: {nfo_path[-40:]}"}
            try:
                content = open(nfo_path, "r", encoding="utf-8", errors="replace").read()
                # Build verification XML block
                verify_xml = f"\n  <!--CineCross verification {time.strftime('%Y-%m-%d')}-->\n"
                verify_xml += f"  <tag>cinecross:verified</tag>\n"
                if status == "mismatch":
                    verify_xml += f"  <tag>cinecross:review_needed</tag>\n"
                opening = ocr_data.get("opening_text", "").strip()
                if opening:
                    # Extract clean title/distributor from opening
                    safe_opening = opening.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")[:200]
                    verify_xml += f"  <tag>cinecross:opening:{safe_opening[:60]}</tag>\n"
                directors = ocr_data.get("directors", [])
                if directors:
                    for d in directors[:2]:
                        safe_d = d.replace("&", "&amp;").replace("<", "&lt;")[:50]
                        verify_xml += f"  <tag>cinecross:ocr_director:{safe_d}</tag>\n"
                names = ocr_data.get("names_found", [])
                if names:
                    for n in names[:5]:
                        safe_n = n.replace("&", "&amp;").replace("<", "&lt;")[:50]
                        verify_xml += f"  <tag>cinecross:ocr_name:{safe_n}</tag>\n"
                # Insert before </movie>
                if "</movie>" in content:
                    content = content.replace("</movie>", verify_xml + "</movie>")
                    open(nfo_path, "w", encoding="utf-8").write(content)
                    return {"written": nfo_path, "tags_added": verify_xml.count("<tag>")}
                return {"error": "no </movie> tag found in NFO"}
            except Exception as e:
                return {"error": str(e)}

        elif ttype == "transcode_dvd":
            # Transcode a DVD_TS folder to a single x265 MKV
            # params: path, crf (default 20), preset (default slow), delete_source (default False)
            path = params.get("path", "")
            crf = params.get("crf", 20)
            preset = params.get("preset", "slow")
            delete_source = params.get("delete_source", False)
            mp = map_path(path, config)
            if not os.path.isdir(mp):
                return {"error": "directory not found"}
            # Find VIDEO_TS subfolder or VOBs directly
            vts_dir = os.path.join(mp, "VIDEO_TS") if os.path.isdir(os.path.join(mp, "VIDEO_TS")) else mp
            vobs = sorted([os.path.join(vts_dir, f) for f in os.listdir(vts_dir)
                          if f.upper().endswith(".VOB") and not f.upper().startswith("VTS_00")
                          and os.path.getsize(os.path.join(vts_dir, f)) > 10_000_000])
            if not vobs:
                # Try ISO file
                isos = [f for f in os.listdir(mp) if f.lower().endswith(".iso")]
                if isos:
                    return {"error": "ISO files not supported yet — mount first"}
                return {"error": "no VOB files found"}
            # Get source info
            src_size = sum(os.path.getsize(v) for v in vobs)
            # Probe first VOB for streams
            r = subprocess.run(["ffprobe", "-v", "quiet", "-show_streams", "-print_format", "json", vobs[0]],
                capture_output=True, timeout=30, text=True)
            streams = json.loads(r.stdout).get("streams", []) if r.returncode == 0 else []
            audio_streams = [s for s in streams if s.get("codec_type") == "audio"]
            sub_streams = [s for s in streams if s.get("codec_type") == "subtitle"]
            # Build ffmpeg command
            buf = get_buffer_path(config)
            dirname = os.path.basename(mp)
            output = os.path.join(buf, f"{dirname}.mkv")
            # Concat VOBs via concat protocol
            concat_input = "concat:" + "|".join(vobs)
            # Try VAAPI hardware encoding, fall back to software
            vaapi_dev = "/dev/dri/renderD128"
            use_vaapi = os.path.exists(vaapi_dev)
            if use_vaapi:
                cmd = ["ffmpeg", "-y", "-vaapi_device", vaapi_dev, "-i", concat_input,
                       "-map", "0:v:0",
                       "-vf", "format=nv12,hwupload",
                       "-c:v", "hevc_vaapi", "-qp", str(crf)]
            else:
                cmd = ["ffmpeg", "-y", "-i", concat_input,
                       "-map", "0:v:0",
                       "-c:v", "libx265", "-crf", str(crf), "-preset", preset,
                       "-pix_fmt", "yuv420p"]
            # Map all audio streams
            for i in range(len(audio_streams)):
                cmd += ["-map", f"0:a:{i}"]
            cmd += ["-c:a", "aac", "-b:a", "192k"]  # Transcode audio to AAC
            # Map subtitles if any
            for i in range(len(sub_streams)):
                cmd += ["-map", f"0:s:{i}"]
            cmd += ["-c:s", "copy"]
            cmd += [output]
            log(f"[transcode] Starting: {dirname} ({src_size/1073741824:.1f} GB, {len(vobs)} VOBs)")
            r = subprocess.run(cmd, capture_output=True, timeout=7200, text=True)  # 2h max
            if not os.path.exists(output) or os.path.getsize(output) < 1_000_000:
                buffer_cleanup(output)
                # Extract actual error from stderr (skip progress lines)
                err_lines = [l for l in (r.stderr or "").split("\n") if l.strip() and not l.strip().startswith("frame=") and "size=" not in l]
                return {"error": f"ffmpeg failed (rc={r.returncode}): {' '.join(err_lines[-3:])}"}
            # Verify output duration
            r2 = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", output],
                capture_output=True, timeout=15, text=True)
            out_duration = float(r2.stdout.strip()) if r2.stdout.strip() else 0
            # Get source duration
            r3 = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", concat_input],
                capture_output=True, timeout=30, text=True)
            src_duration = float(r3.stdout.strip()) if r3.stdout.strip() else 0
            if src_duration > 0 and abs(out_duration - src_duration) > 30:
                buffer_cleanup(output)
                return {"error": f"duration mismatch: src={src_duration:.0f}s out={out_duration:.0f}s"}
            # Move output to destination
            import shutil
            new_size = os.path.getsize(output)
            dest = os.path.join(os.path.dirname(mp), f"{dirname}.mkv")
            shutil.move(output, dest)
            saved = src_size - new_size
            log(f"[transcode] Done: {dirname} — {src_size/1073741824:.1f}GB → {new_size/1073741824:.1f}GB (saved {saved/1073741824:.1f}GB)")
            # Delete source DVD folder if requested
            if delete_source:
                shutil.rmtree(mp, ignore_errors=True)
            return {"action": "transcoded", "source_size": src_size, "output_size": new_size,
                    "saved_bytes": saved, "duration": int(out_duration),
                    "output": dest, "source_deleted": delete_source}

        elif ttype == "integrity_check":
            # Check file integrity by decoding with ffmpeg - detects truncation/corruption
            paths = params.get("paths", [])
            if params.get("path"): paths = [params["path"]]
            results = {}
            for p in paths:
                mp = map_path(p, config)
                if os.path.isdir(mp):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v")
                    vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                              for f in os.listdir(mp) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        mp = vfiles[0][1]
                if not os.path.isfile(mp):
                    results[p] = {"status": "not_found"}
                    continue
                # Run ffmpeg decode check (only first+last 60s to save time)
                duration = 0
                try:
                    r = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", mp],
                        capture_output=True, timeout=15, text=True)
                    duration = float(r.stdout.strip()) if r.stdout.strip() else 0
                except: pass
                errors = []
                # Check beginning (first 30s)
                r = subprocess.run(["ffmpeg", "-v", "error", "-i", mp, "-t", "30", "-f", "null", "-"],
                    capture_output=True, timeout=60, text=True)
                if r.stderr.strip():
                    errors.append({"position": "start", "errors": r.stderr.strip()[:200]})
                # Check end (last 30s)
                if duration > 60:
                    r = subprocess.run(["ffmpeg", "-v", "error", "-ss", str(int(duration - 30)), "-i", mp, "-f", "null", "-"],
                        capture_output=True, timeout=60, text=True)
                    if r.stderr.strip():
                        errors.append({"position": "end", "errors": r.stderr.strip()[:200]})
                results[p] = {"status": "corrupt" if errors else "ok", "errors": errors, "duration": int(duration)}
            return {"checked": len(results), "data": results}

        elif ttype == "scan_extra":
            # Scan non-TMM directories on demand
            return {"files": scan_non_tmm(config)}

        elif ttype == "quality_score":
            # Score file quality using BPP + genre awareness
            # params: paths (list), genres (optional, comma-separated)
            paths = params.get("paths", [])
            if params.get("path"): paths = [params["path"]]
            genres = params.get("genres", "")
            results = {}
            for p in paths:
                mp = map_path(p, config)
                if os.path.isdir(mp):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v")
                    vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                              for f in os.listdir(mp) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        mp = vfiles[0][1]
                if os.path.isfile(mp):
                    mi = extract_mediainfo(mp)
                    if "error" not in mi:
                        results[p] = compute_quality_score(mi, genres)
                        results[p]["file"] = os.path.basename(mp)
                        results[p]["size_gb"] = round(os.path.getsize(mp) / 1073741824, 2)
                    else:
                        results[p] = mi
            return {"scored": len(results), "data": results}

        elif ttype == "compare_files":
            # Compare two files and recommend which is better
            # params: file_a, file_b, genres (optional)
            a_path = map_path(params.get("file_a", ""), config)
            b_path = map_path(params.get("file_b", ""), config)
            genres = params.get("genres", "")
            # Resolve dirs
            for ref in ("a_path", "b_path"):
                p = locals()[ref]
                if os.path.isdir(p):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v")
                    vfiles = [(os.path.getsize(os.path.join(p, f)), os.path.join(p, f))
                              for f in os.listdir(p) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        if ref == "a_path": a_path = vfiles[0][1]
                        else: b_path = vfiles[0][1]
            mi_a = extract_mediainfo(a_path)
            mi_b = extract_mediainfo(b_path)
            if "error" in mi_a: return {"error": f"file_a: {mi_a['error']}"}
            if "error" in mi_b: return {"error": f"file_b: {mi_b['error']}"}
            result = compare_quality(mi_a, mi_b, genres)
            result["file_a"] = os.path.basename(a_path)
            result["file_b"] = os.path.basename(b_path)
            return result

        elif ttype == "ssim_compare":
            # Perceptual quality comparison using SSIM (sampled frames)
            # Uses the higher-quality file as reference
            # params: reference (better file), distorted (file to score), samples (frames to compare, default 60)
            ref_path = map_path(params.get("reference", ""), config)
            dist_path = map_path(params.get("distorted", ""), config)
            samples = params.get("samples", 60)
            # Resolve dirs
            for ref in ("ref_path", "dist_path"):
                p = locals()[ref]
                if os.path.isdir(p):
                    vexts = (".mkv", ".mp4", ".avi", ".m4v")
                    vfiles = [(os.path.getsize(os.path.join(p, f)), os.path.join(p, f))
                              for f in os.listdir(p) if f.lower().endswith(vexts)]
                    if vfiles:
                        vfiles.sort(reverse=True)
                        if ref == "ref_path": ref_path = vfiles[0][1]
                        else: dist_path = vfiles[0][1]
            if not os.path.isfile(ref_path): return {"error": f"reference not found"}
            if not os.path.isfile(dist_path): return {"error": f"distorted not found"}
            # Get duration to compute sample interval
            r = subprocess.run(["ffprobe", "-v", "quiet", "-show_entries", "format=duration", "-of", "csv=p=0", ref_path],
                capture_output=True, timeout=30, text=True)
            duration = float(r.stdout.strip()) if r.stdout.strip() else 0
            if duration < 60: return {"error": "video too short"}
            # Sample every N seconds
            interval = duration / samples
            # Buffer files locally for fast frame decoding
            local_ref = buffer_copy(ref_path, config)
            local_dist = buffer_copy(dist_path, config)
            # Run SSIM comparison with frame sampling
            import re as _re
            r = subprocess.run([
                "ffmpeg", "-i", local_dist, "-i", local_ref,
                "-lavfi", f"[0:v]fps=1/{interval:.0f}[d];[1:v]fps=1/{interval:.0f}[r];[d][r]scale2ref[d2][r2];[d2][r2]ssim=stats_file=-",
                "-f", "null", "-"
            ], capture_output=True, timeout=600, text=True)
            buffer_cleanup(local_ref, local_dist)
            # Parse SSIM output from stderr
            ssim_values = _re.findall(r"All:([\d.]+)", r.stderr)
            if not ssim_values:
                return {"error": f"SSIM failed: {r.stderr[-200:]}"}
            scores = [float(v) for v in ssim_values]
            avg_ssim = sum(scores) / len(scores)
            min_ssim = min(scores)
            # SSIM interpretation: >0.98 = transparent, 0.95-0.98 = good, 0.90-0.95 = noticeable, <0.90 = bad
            if avg_ssim > 0.98: verdict = "transparent"
            elif avg_ssim > 0.95: verdict = "good"
            elif avg_ssim > 0.90: verdict = "noticeable_loss"
            else: verdict = "significant_loss"
            return {
                "ssim_avg": round(avg_ssim, 4),
                "ssim_min": round(min_ssim, 4),
                "samples": len(scores),
                "verdict": verdict,
                "reference": os.path.basename(ref_path),
                "distorted": os.path.basename(dist_path),
            }

        elif ttype == "strip_audio":
            # Remove audio tracks by language from MKV files using mkvmerge
            # params: path, languages (list of langs to remove, e.g. ["rus","hin"])
            # Optional: original_language - won't strip if track matches this
            # Optional: imdb_id - used to auto-detect original language from TMDB
            path = params.get("path", "")
            remove_langs = [l.lower() for l in params.get("languages", ["rus"])]
            original_lang = params.get("original_language", "").lower()
            dry_run = params.get("dry_run", True)
            # Auto-detect original language from TMDB if imdb_id provided and no original_language
            if not original_lang and params.get("imdb_id"):
                try:
                    iid = params["imdb_id"]
                    url = f"{config.get('_server','')}/api/title?id={iid}&user={config.get('_user','ecb')}"
                    r = urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "CinephileAgent/2.0"}), timeout=10)
                    tdata = json.loads(r.read())
                    original_lang = (tdata.get("original_language") or "").lower()
                except: pass
            # Safety: if removing Russian and original language IS Russian, abort
            if original_lang and any(original_lang.startswith(l[:2]) for l in remove_langs):
                return {"action": "skipped", "reason": f"original language is '{original_lang}' - matches removal list"}
            mp = map_path(path, config)
            # Resolve directory to video file
            if os.path.isdir(mp):
                vexts = (".mkv",)
                vfiles = [(os.path.getsize(os.path.join(mp, f)), os.path.join(mp, f))
                          for f in os.listdir(mp) if f.lower().endswith(vexts)]
                if vfiles:
                    vfiles.sort(reverse=True)
                    mp = vfiles[0][1]
            if not os.path.isfile(mp) or not mp.lower().endswith(".mkv"):
                return {"error": "not an MKV file", "path": mp}
            # Get track info via mkvmerge --identify
            r = subprocess.run(["mkvmerge", "-J", mp], capture_output=True, timeout=15, text=True)
            if r.returncode != 0:
                return {"error": f"mkvmerge identify failed: {r.stderr[:100]}"}
            mkv_info = json.loads(r.stdout)
            tracks = mkv_info.get("tracks", [])
            audio_tracks = [t for t in tracks if t.get("type") == "audio"]
            to_remove = []
            to_keep = []
            for t in audio_tracks:
                lang = (t.get("properties", {}).get("language", "") or "").lower()
                lang_ietf = (t.get("properties", {}).get("language_ietf", "") or "").lower()
                tid = t["id"]
                is_target = lang in remove_langs or lang_ietf in remove_langs or any(l in lang for l in remove_langs)
                is_original = lang == original_lang or lang_ietf == original_lang
                if is_target and not is_original:
                    to_remove.append({"id": tid, "language": lang or lang_ietf, "codec": t.get("codec","")})
                else:
                    to_keep.append({"id": tid, "language": lang or lang_ietf})
            if not to_remove:
                return {"action": "none", "reason": "no matching tracks to remove", "audio_tracks": [{"id": t["id"], "lang": t.get("properties",{}).get("language","")} for t in audio_tracks]}
            if not to_keep:
                return {"action": "none", "reason": "would remove ALL audio tracks - aborting", "to_remove": to_remove}
            if dry_run:
                return {"action": "dry_run", "would_remove": to_remove, "keeping": to_keep, "path": path}
            # Execute: mkvmerge -o tmp --audio-tracks !tid1,!tid2 input
            exclude_ids = ",".join(str(t["id"]) for t in to_remove)
            tmp_out = mp + ".tmp.mkv"
            r = subprocess.run(["mkvmerge", "-o", tmp_out, "--audio-tracks", "!" + exclude_ids, mp],
                capture_output=True, timeout=600, text=True)
            if r.returncode not in (0, 1):  # 1 = warnings
                if os.path.exists(tmp_out): os.remove(tmp_out)
                return {"error": f"mkvmerge failed: {r.stderr[:200]}"}
            # Replace original
            orig_size = os.path.getsize(mp)
            new_size = os.path.getsize(tmp_out)
            os.replace(tmp_out, mp)
            saved = orig_size - new_size
            log(f"[strip] {os.path.basename(mp)}: removed {len(to_remove)} tracks, saved {saved/1048576:.0f} MB")
            return {"action": "stripped", "removed": to_remove, "kept": to_keep,
                    "original_size": orig_size, "new_size": new_size, "saved_bytes": saved}
        
        else:
            return {"error": f"Unknown task: {ttype}"}
    except Exception as e:
        return {"error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="CinephileCrossroads LAN Agent")
    parser.add_argument("--daemon", action="store_true", help="Run as daemon, polling server for tasks")
    parser.add_argument("--server", required=True, help="CinephileCrossroads URL (e.g. https://tools.ecb.pm/imdb)")
    parser.add_argument("--user", required=True, help="Username to sync to")
    parser.add_argument("--subs", action="store_true", help="Download missing subtitles via OpenSubtitles")
    parser.add_argument("--subs-lang", default="en,fr", help="Subtitle languages (comma-separated, default: en,fr)")
    parser.add_argument("--hash", action="store_true", help="Compute file hashes for subtitle matching (slow over network)")
    parser.add_argument("--thumbnails", action="store_true", help="Phase 2: fetch thumbnail requests from server, generate and upload")
    args = parser.parse_args()

    if not os.path.exists(CONFIG_FILE):
        json.dump(DEFAULT_CONFIG, open(CONFIG_FILE, "w"), indent=2)
        print(f"Created {CONFIG_FILE} - edit it with your server details, then run again.")
        sys.exit(0)

    check_prerequisites()
    config = json.load(open(CONFIG_FILE))
    
    if args.daemon:
        daemon_mode(args, config)
        return  # Never reaches here
    library = {}
    for name, cfg in config.items():
        if name.startswith("_"): continue
        if not isinstance(cfg, dict): continue
        if not cfg.get("enabled"): continue
        if name not in FETCHERS: continue
        print(f"Fetching {name} from {cfg.get('url', '?')}...")
        items = FETCHERS[name](cfg)
        library.update(items)
        print(f"  {name}: {len(items)} titles")

    if not library:
        print("No titles found. Check agent.json config.")
        sys.exit(1)

    check_path_access(config, library)

    # Map paths and get file sizes
    # Size, hash, and push in batches of 500
    token = config.get("_agent_token", "")
    url = f"{args.server}/api/library/{args.user}"
    push_headers = {"Content-Type": "application/json", "User-Agent": "CinephileAgent/1.0"}
    if token:
        push_headers["X-Agent-Token"] = token

    episodes = library.pop("_episodes", {})
    items = list(library.items())
    batch_size = 50
    total_sized = 0
    total_hashed = 0
    total_not_found = 0

    for batch_start in range(0, len(items), batch_size):
        batch = dict(items[batch_start:batch_start + batch_size])
        batch_num = batch_start // batch_size + 1
        total_batches = (len(items) + batch_size - 1) // batch_size
        print(f"\nBatch {batch_num}/{total_batches} ({len(batch)} titles)")

        # Size files in this batch
        for iid, val in batch.items():
            entries = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
            for info in entries:
                if not isinstance(info, dict): continue
                path = info.get("path", "")
                if path:
                    mapped = map_path(path, config)
                    if mapped != path:
                        info["local_path"] = mapped
                    if os.name == "nt":
                        mapped = mapped.replace("/", "\\")
                    # If path is a directory, find the largest video file inside
                    if os.path.isdir(mapped):
                        vexts = (".mkv", ".mp4", ".avi", ".m4v", ".wmv", ".ts", ".m2ts")
                        vfiles = [(os.path.getsize(os.path.join(mapped, f)), os.path.join(mapped, f))
                                  for f in os.listdir(mapped) if f.lower().endswith(vexts)]
                        if vfiles:
                            vfiles.sort(reverse=True)
                            mapped = vfiles[0][1]
                            info["file_path"] = mapped
                    short = os.path.basename(mapped)[:40]
                    print(f"    [{total_sized+total_not_found+1}] {short}...", end="\r")
                    if os.path.isfile(mapped):
                        try:
                            info["file_size"] = os.path.getsize(mapped)
                            total_sized += 1
                        except Exception as e:
                            total_not_found += 1
                            print(f"\n    ! Size error: {short} - {e}")
                        # Hashing done separately for duplicates only
                    else:
                        total_not_found += 1
                        print(f"\n    ! Not found: {mapped[:70]}")

        print(f"  Sized: {total_sized} | Hashed: {total_hashed} | Not found: {total_not_found}")

        # Push this batch
        try:
            req = urllib.request.Request(url, data=json.dumps({"library": batch}).encode(), headers=push_headers)
            result = json.loads(urllib.request.urlopen(req, timeout=60).read())
            print(f"  Pushed -> server has {result.get('count', '?')} titles")
        except Exception as e:
            print(f"  Push error: {e}")

    # Push episodes in batches
    if episodes:
        ep_items = list(episodes.items())
        for i in range(0, len(ep_items), batch_size):
            chunk = dict(ep_items[i:i + batch_size])
            try:
                req = urllib.request.Request(url, data=json.dumps({"library": {"_episodes": chunk}}).encode(), headers=push_headers)
                urllib.request.urlopen(req, timeout=60)
                print(f"  Episodes: pushed {min(i + batch_size, len(ep_items))}/{len(ep_items)}")
            except Exception as e:
                print(f"  Episode push error: {e}")

    result = {"count": len(items)}

    # Phase 2: Thumbnails (only if --thumbnails flag)
    if args.thumbnails:
        generate_thumbnails(args, config, library, push_headers, url)

    if args.hash:
        hash_duplicates(args, config, library, push_headers, url)

    if args.subs:
        download_subtitles(args, config, library)

def download_subtitles(args, config, library):
    """Download missing subtitles from OpenSubtitles for files without subs."""
    # Get API key from server
    try:
        req = urllib.request.Request(f"{args.server}/api",
            headers={"User-Agent": "CinephileAgent/1.0"})
        server_info = json.loads(urllib.request.urlopen(req, timeout=10).read())
    except:
        pass

    opensubs_key = config.get("_opensubs_key", "")
    if not opensubs_key:
        opensubs_key = input("  OpenSubtitles API key (get from opensubtitles.com/consumers): ").strip()
        if opensubs_key:
            config["_opensubs_key"] = opensubs_key
            json.dump(config, open(CONFIG_FILE, "w"), indent=2)
    if not opensubs_key:
        print("  No OpenSubtitles API key - skipping")
        return

    languages = args.subs_lang.split(",")
    headers = {"Api-Key": opensubs_key, "User-Agent": "CinephileCrossroads v1.0", "Content-Type": "application/json"}

    # Find files without subtitles
    missing = []
    for iid, val in library.items():
        if iid == "_episodes": continue
        entries = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
        for info in entries:
            if not isinstance(info, dict): continue
            if info.get("subtitles"): continue
            path = info.get("local_path") or map_path(info.get("path", ""), config)
            if os.name == "nt":
                path = path.replace("/", "\\")
            if path and os.path.isfile(path):
                missing.append((iid, info, path))

    if not missing:
        print("  All files have subtitles")
        return

    print(f"  {len(missing)} files missing subtitles")
    downloaded = 0
    errors = 0

    for i, (iid, info, path) in enumerate(missing):
        short = os.path.basename(path)[:40]
        print(f"    [{i+1}/{len(missing)}] {short}...", end="\r")

        # Search by IMDB ID
        imdb_num = iid.replace("tt", "")
        search_url = f"https://api.opensubtitles.com/api/v1/subtitles?imdb_id={imdb_num}&languages={','.join(languages)}"

        # Also try hash if available
        file_hash = info.get("file_hash")
        if file_hash:
            search_url += f"&moviehash={file_hash}"

        try:
            req = urllib.request.Request(search_url, headers=headers)
            resp = json.loads(urllib.request.urlopen(req, timeout=10).read())
            results = resp.get("data", [])
            if not results:
                continue

            # Pick best result (hash match first, then most downloads)
            best = None
            for r in results:
                attr = r.get("attributes", {})
                files = attr.get("files", [])
                if not files: continue
                if attr.get("moviehash_match"):
                    best = files[0]
                    break
                if not best or attr.get("download_count", 0) > 0:
                    best = files[0]

            if not best:
                continue

            # Download
            dl_req = urllib.request.Request(
                "https://api.opensubtitles.com/api/v1/download",
                data=json.dumps({"file_id": best["file_id"]}).encode(),
                headers=headers)
            dl_resp = json.loads(urllib.request.urlopen(dl_req, timeout=10).read())
            dl_link = dl_resp.get("link")

            if dl_link:
                # Save next to the video file
                sub_ext = ".srt"
                lang = results[0].get("attributes", {}).get("language", languages[0])
                video_base = os.path.splitext(path)[0]
                sub_path = f"{video_base}.{lang}{sub_ext}"

                sub_data = urllib.request.urlopen(dl_link, timeout=30).read()
                with open(sub_path, "wb") as f:
                    f.write(sub_data)
                downloaded += 1
                print(f"\n    + {short} -> {lang}{sub_ext}")

        except Exception as e:
            errors += 1
            if "429" in str(e):
                print(f"\n    Rate limited - waiting 10s...")
                import time; time.sleep(10)

        import time; time.sleep(1)  # Rate limit: 1 req/sec

    print(f"\n  Done: {downloaded} subtitles downloaded, {errors} errors")

def hash_duplicates(args, config, library, headers, base_url):
    """Hash only duplicate files for accurate comparison."""
    try:
        req = urllib.request.Request(
            f"{args.server}/api/thumbnails/needed/{args.user}",
            headers=headers)
        needed = json.loads(urllib.request.urlopen(req, timeout=30).read())
    except Exception as e:
        print(f"Could not fetch duplicate list: {e}")
        return

    if not needed:
        print("No duplicates to hash")
        return

    print(f"Hashing {len(needed)} duplicate titles...")
    hashed = 0
    for iid in needed:
        val = library.get(iid)
        entries = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
        for info in entries:
            if not isinstance(info, dict): continue
            path = info.get("local_path") or map_path(info.get("path", ""), config)
            if os.name == "nt":
                path = path.replace("/", "\\")
            if not os.path.isfile(path): continue
            h = opensubtitles_hash(path)
            if h:
                info["file_hash"] = h
                info["file_size"] = os.path.getsize(path)
                hashed += 1
        if hashed % 10 == 0 and hashed > 0:
            print(f"  {hashed} files hashed...")

    # Push updated entries
    to_push = {iid: library[iid] for iid in needed if iid in library}
    if to_push:
        req = urllib.request.Request(base_url, data=json.dumps({"library": to_push}).encode(), headers=headers)
        urllib.request.urlopen(req, timeout=60)
    print(f"Done: {hashed} files hashed and pushed")

def generate_thumbnails(args, config, library, headers, base_url):
    """Phase 2: Ask server which titles need screenshots, generate and upload."""
    import shutil, subprocess, base64, re
    if not shutil.which("ffmpeg"):
        print("ffmpeg not found - run setup-windows.ps1 to install")
        return

    # Ask server for titles needing thumbnails
    try:
        req = urllib.request.Request(
            f"{args.server}/api/thumbnails/needed/{args.user}",
            headers=headers)
        resp = urllib.request.urlopen(req, timeout=30)
        needed = json.loads(resp.read())
    except Exception as e:
        print(f"Could not fetch thumbnail requests: {e}")
        return

    if not needed:
        print("No thumbnails needed")
        return

    print(f"Generating {len(needed)} thumbnails...")
    for i, iid in enumerate(needed):
        info = library.get(iid, {})
        if not isinstance(info, dict): continue
        path = info.get("local_path") or map_path(info.get("path", ""), config)
        if not path or not os.path.isfile(path):
            continue

        try:
            # Get duration
            probe = subprocess.run(
                ["ffmpeg", "-i", path],
                capture_output=True, text=True, timeout=10)
            dur_match = re.search(r"Duration: (\d+):(\d+):(\d+)", probe.stderr)
            if dur_match:
                seek = (int(dur_match.group(1))*3600 + int(dur_match.group(2))*60 + int(dur_match.group(3))) // 2
            else:
                seek = 300

            # Generate thumbnail
            thumb_file = os.path.join(os.environ.get("TEMP", "/tmp"), f"{iid.replace('/','_')}.jpg")
            subprocess.run([
                "ffmpeg", "-ss", str(seek), "-i", path,
                "-vframes", "1", "-vf", "scale=320:-1",
                "-q:v", "8", thumb_file, "-y"
            ], capture_output=True, timeout=30)

            if os.path.exists(thumb_file):
                # Upload as base64
                with open(thumb_file, "rb") as f:
                    thumb_data = base64.b64encode(f.read()).decode()
                payload = json.dumps({"imdb_id": iid, "thumbnail": thumb_data})
                req = urllib.request.Request(
                    f"{args.server}/api/thumbnail/{args.user}",
                    data=payload.encode(), headers=headers)
                urllib.request.urlopen(req, timeout=30)
                os.remove(thumb_file)

                if (i + 1) % 10 == 0:
                    print(f"  {i+1}/{len(needed)} thumbnails uploaded")
        except Exception as e:
            pass  # Skip failures

    print(f"Thumbnails done")

if __name__ == "__main__":
    main()
