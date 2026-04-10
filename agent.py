#!/usr/bin/env python3
"""
CinephileCrossroads LAN Agent — runs on your local network, syncs media servers.

Usage:
    python3 agent.py --server https://tools.ecb.pm/imdb --user ecb

Configure your media servers in agent.json (created on first run).
Run via cron for automatic sync: */30 * * * * python3 /path/to/agent.py --server URL --user USER
"""
import json, os, sys, urllib.request, urllib.parse, argparse

CONFIG_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "agent.json")

DEFAULT_CONFIG = {
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
        print(f"  Error: {url[:60]} — {e}")
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
    # Mode 1: scan local NFO files (preferred — works offline)
    scan_path = cfg.get("path", "")
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
                        lib[iid] = info
                except: pass
        return lib
    # Mode 2: TMM HTTP API — export to temp dir on same machine, then read
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

FETCHERS = {"plex": fetch_plex, "jellyfin": fetch_jellyfin, "emby": fetch_jellyfin,
            "kodi": fetch_kodi, "radarr": fetch_radarr, "sonarr": fetch_sonarr, "tmm": fetch_tmm}

def main():
    parser = argparse.ArgumentParser(description="CinephileCrossroads LAN Agent")
    parser.add_argument("--server", required=True, help="CinephileCrossroads URL (e.g. https://tools.ecb.pm/imdb)")
    parser.add_argument("--user", required=True, help="Username to sync to")
    args = parser.parse_args()

    if not os.path.exists(CONFIG_FILE):
        json.dump(DEFAULT_CONFIG, open(CONFIG_FILE, "w"), indent=2)
        print(f"Created {CONFIG_FILE} — edit it with your server details, then run again.")
        sys.exit(0)

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

    # Get file sizes for library entries
    print("Getting file sizes...")
    sized = 0
    for iid, info in library.items():
        if not isinstance(info, dict): continue
        path = info.get("path", "")
        if path and os.path.isfile(path):
            try:
                info["file_size"] = os.path.getsize(path)
                sized += 1
            except: pass
    print(f"  {sized} files sized")

    # Compute file hashes for subtitle matching
    print("Computing file hashes...")
    library = compute_hashes(library)
    hashed = sum(1 for v in library.values() if v.get("file_hash"))
    print(f"  {hashed} files hashed")

    # Report titles missing subtitles
    missing_subs = find_missing_subs(library)
    if missing_subs:
        print(f"  {len(missing_subs)} titles have no subtitles")

    # Convert to list format for duplicates
    multi_lib = {}
    for iid, info in library.items():
        if not isinstance(info, dict): continue
        if iid in multi_lib:
            if isinstance(multi_lib[iid], list):
                multi_lib[iid].append(info)
            else:
                multi_lib[iid] = [multi_lib[iid], info]
        else:
            multi_lib[iid] = info
    library = multi_lib
    # Separate episodes from main library
    episodes = library.pop("_episodes", {})
    print(f"Pushing {len(library)} titles + {len(episodes)} episodes to {args.server}...")
    token = config.get("_agent_token", "")
    url = f"{args.server}/api/library/{args.user}"
    headers = {"X-Agent-Token": token} if token else {}
    req = urllib.request.Request(url, data=json.dumps({"library": library}).encode(),
        headers={"Content-Type": "application/json", "User-Agent": "CinephileAgent/1.0", **headers})
    result = json.loads(urllib.request.urlopen(req, timeout=30).read())
    # Push episodes separately within the same library
    if episodes:
        ep_payload = {"library": {"_episodes": episodes}}
        ep_req = urllib.request.Request(url, data=json.dumps(ep_payload).encode(),
            headers={"Content-Type": "application/json", "User-Agent": "CinephileAgent/1.0", **headers})
        urllib.request.urlopen(ep_req, timeout=60)
    print(f"Done — server has {result.get('count', '?')} titles in library")

if __name__ == "__main__":
    main()
