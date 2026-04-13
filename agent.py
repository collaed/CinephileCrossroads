#!/usr/bin/env python3
"""
CinephileCrossroads LAN Agent - runs on your local network, syncs media servers.

Usage:
    python3 agent.py --server https://tools.ecb.pm/imdb --user ecb

Configure your media servers in agent.json (created on first run).
Run via cron for automatic sync: */30 * * * * python3 /path/to/agent.py --server URL --user USER
"""
import json, os, sys, time, threading, urllib.request, urllib.parse, argparse

AGENT_VERSION = "2.1"
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
    scan_path = cfg.get("path", "")
    if scan_path and os.path.isdir(scan_path):
        print(f"  Scanning NFO files in {scan_path}...")
        scanned = 0
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

def map_path(path, config):
    """Map remote paths (e.g. Kodi NFS) to local paths (e.g. Windows SMB).
    Handles stack:// URLs by extracting the first file path."""
    # Handle stack:// (Kodi multi-part files)
    if path.startswith("stack://"):
        path = path.replace("stack://", "").split(" , ")[0].strip()
    mappings = config.get("_path_mappings", {})
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
        """Periodic Kodi/media server sync."""
        while True:
            try:
                print("[sync] Fetching media servers...")
                library = {}
                for name, cfg in config.items():
                    if not isinstance(cfg, dict) or not cfg.get("enabled"): continue
                    if name not in FETCHERS: continue
                    try:
                        items = FETCHERS[name](cfg)
                        library.update(items)
                        log(f"[sync] {name}: {len(items)} titles")
                    except Exception as e:
                        log(f"[sync] {name} error: {e}")
                if library:
                    library = compute_hashes(library)
                    api_post(f"{base_url}/api/library/{args.user}", {"library": library})
                    log(f"[sync] Pushed {len(library)} titles")
            except Exception as e:
                log(f"[sync] Error: {e}")
                time.sleep(60)  # Retry sooner on error
                continue
            time.sleep(1800)  # Every 30 min
    
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
                    # Normal tasks: run inline, one at a time
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
                    break  # One task per poll cycle
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
            time.sleep(15)
    
    log(f"Agent daemon - server: {base_url}, user: {args.user}")
    log(f"  Sync thread: every 30 min")
    log(f"  Task thread: every 15 sec")
    
    threading.Thread(target=sync_loop, daemon=True).start()
    task_loop()  # Run task loop in main thread

def run_task(ttype, params, config):
    """Execute a single task from the server."""
    try:
        if ttype == "size_files":
            paths = params.get("paths", [])
            data = {}
            for p in paths:
                mp = map_path(p, config)
                if os.path.isfile(mp):
                    data[p] = os.path.getsize(mp)
            return {"sized": len(data), "data": data}
        
        elif ttype == "hash_files":
            paths = params.get("paths", [])
            data = {}
            for p in paths:
                mp = map_path(p, config)
                if os.path.isfile(mp):
                    h = opensubtitles_hash(mp)
                    if h: data[p] = {"hash": h, "size": os.path.getsize(mp)}
            return {"hashed": len(data), "data": data}
        
        elif ttype == "download_subs":
            imdb_id = params.get("imdb_id")
            path = params.get("path")
            if imdb_id and path:
                mp = map_path(path, config)
                file_hash = opensubtitles_hash(mp) if os.path.isfile(mp) else None
                return {"imdb_id": imdb_id, "hash": file_hash, "path": path}
            return {"error": "missing imdb_id or path"}
        
        elif ttype == "check_quality":
            paths = params.get("paths", [])
            data = {}
            for p in paths:
                mp = map_path(p, config)
                data[p] = {"exists": os.path.isfile(mp), "size": os.path.getsize(mp) if os.path.isfile(mp) else 0}
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
            local_vars = {"config": config, "base_url": config.get("_server", ""), "cancelled": lambda: _bg_task.get("cancel", False), "agent_headers": {"Content-Type": "application/json", "User-Agent": "CinephileAgent/2.0"}, "os": os, "json": json, "re": __import__("re"),
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
