#!/usr/bin/env python3
"""
CinephileCrossroads LAN Agent - runs on your local network, syncs media servers.

Usage:
    python3 agent.py --server https://tools.ecb.pm/imdb --user ecb

Configure your media servers in agent.json (created on first run).
Run via cron for automatic sync: */30 * * * * python3 /path/to/agent.py --server URL --user USER
"""
import json, os, sys, urllib.request, urllib.parse, argparse

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent.json")

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
               "params": {"properties": ["imdbnumber", "file", "streamdetails", "runtime", "title", "year"]}}
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

def main():
    parser = argparse.ArgumentParser(description="CinephileCrossroads LAN Agent")
    parser.add_argument("--server", required=True, help="CinephileCrossroads URL (e.g. https://tools.ecb.pm/imdb)")
    parser.add_argument("--user", required=True, help="Username to sync to")
    parser.add_argument("--thumbnails", action="store_true", help="Phase 2: fetch thumbnail requests from server, generate and upload")
    args = parser.parse_args()

    if not os.path.exists(CONFIG_FILE):
        json.dump(DEFAULT_CONFIG, open(CONFIG_FILE, "w"), indent=2)
        print(f"Created {CONFIG_FILE} - edit it with your server details, then run again.")
        sys.exit(0)

    check_prerequisites()
    config = json.load(open(CONFIG_FILE))
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
    batch_size = 500
    total_sized = 0
    total_hashed = 0
    total_not_found = 0

    batch_start_count = 0
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
                    if os.path.isfile(mapped):
                        try:
                            info["file_size"] = os.path.getsize(mapped)
                            total_sized += 1
                        except:
                            total_not_found += 1
                        h = opensubtitles_hash(mapped)
                        if h:
                            info["file_hash"] = h
                            total_hashed += 1
                    elif path:
                        total_not_found += 1
                batch_done = total_sized + total_not_found
                if batch_done % 50 == 0:
                    print(f"    {batch_done - batch_start_count} files processed in this batch...", end="\r")
        batch_start_count = total_sized + total_not_found

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
