"""CineCross — Logic: recommendations, enrichment, Trakt, CSV import, API helpers."""
import csv, json, os, io, time, urllib.request, urllib.parse, threading, math, re
from data import *
from data import _load_key, _imdb_cache, _exec_results, _merge_agent_data

# ── LLM Integration ────────────────────────────────────────────────────
def llm_ask(prompt, system=None, max_tokens=500):
    """Call LLM via OpenAI-compatible API (intello/ollama/openai)."""
    llm_url = _load_key("llm_url") or os.environ.get("LLM_URL", "http://intello:8000")
    llm_token = _load_key("llm_token") or os.environ.get("LLM_TOKEN", "")
    messages = []
    if system: messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})
    headers = {"Content-Type": "application/json"}
    if llm_token: headers["Authorization"] = "Bearer " + llm_token
    try:
        data = json.dumps({"messages": messages, "max_tokens": max_tokens}).encode()
        req = urllib.request.Request(llm_url.rstrip("/") + "/v1/chat/completions", data=data, headers=headers)
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read())
        return result.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception as e:
        return ""

def taste_personality(user):
    """Generate a personality description from taste profile via LLM."""
    ratings = load_user_ratings(user)
    titles = load_titles()
    top = sorted(ratings.items(), key=lambda x: x[1]["rating"], reverse=True)[:15]
    top_str = ", ".join(f'{titles.get(iid,{}).get("title","?")} ({r["rating"]})' for iid, r in top)
    genres = {}
    for iid, r in ratings.items():
        for g in (titles.get(iid,{}).get("genres","") or "").split(","):
            g = g.strip()
            if g: genres[g] = genres.get(g, 0) + 1
    top_genres = ", ".join(g for g, _ in sorted(genres.items(), key=lambda x: x[1], reverse=True)[:5])
    prompt = f"Based on this cinephile's taste, write a fun 2-sentence personality description (like a horoscope for movie lovers). Top genres: {top_genres}. Favorites: {top_str}"
    return llm_ask(prompt, system="You are a witty film critic writing personality profiles. Be playful and specific.", max_tokens=100)

def movie_companion(imdb_id, question, titles):
    """Spoiler-aware movie AI companion."""
    t = titles.get(imdb_id, {})
    title = t.get("title", "Unknown")
    plot = (t.get("overview") or t.get("plot") or "")[:300]
    genres = t.get("genres", "")
    system = f"You are a movie companion for \"{title}\" ({t.get('year','')}, {genres}). Plot: {plot}. NEVER reveal major spoilers or plot twists unless the user explicitly asks. Be concise."
    return llm_ask(question, system=system, max_tokens=300)

def movie_summary_l1(imdb_id, titles):
    """Level 1: Instant summary from metadata only. No LLM needed."""
    t = titles.get(imdb_id, {})
    genres = t.get("genres", "")
    year = t.get("year", "")
    dirs = t.get("directors", [])
    dir_str = ", ".join(dirs) if isinstance(dirs, list) else str(dirs)
    rating = t.get("imdb_rating", "?")
    rt = t.get("rt_score", "")
    kw = t.get("keywords", [])
    top_kw = ", ".join(kw[:5]) if isinstance(kw, list) else ""
    one_liner = f"{genres} ({year}) directed by {dir_str}. IMDB {rating}/10" + (f", RT {rt}" if rt else "") + "."
    mood = []
    if isinstance(kw, list):
        mood_map = {"revenge":"intense","love":"romantic","murder":"dark","comedy":"funny","friendship":"feel-good",
            "dystopia":"mind-bending","war":"epic","ghost":"scary","dream":"surreal","heist":"thrilling",
            "superhero":"epic","chase":"thrilling","twist":"mind-bending","time travel":"mind-bending",
            "corruption":"dark","prison":"dark","redemption":"feel-good","sacrifice":"epic",
            "satire":"funny","dark humor":"dark-humor","slapstick":"funny","parody":"funny",
            "romance":"romantic","wedding":"romantic","forbidden love":"romantic",
            "serial killer":"scary","horror":"scary","zombie":"scary","haunted":"scary",
            "space":"epic","alien":"mind-bending","robot":"mind-bending","artificial intelligence":"mind-bending",
            "drug":"dark","crime":"intense","mafia":"intense","gang":"intense","noir":"dark",
            "survival":"intense","disaster":"epic","race against time":"thrilling",
            "coming of age":"feel-good","family":"feel-good","childhood":"feel-good",
            "loneliness":"melancholic","grief":"melancholic","death":"melancholic",
            "music":"feel-good","dance":"feel-good","art":"inspiring","biography":"inspiring"}
        for k in kw:
            kl = k.lower()
            for signal, m in mood_map.items():
                if signal in kl and m not in mood: mood.append(m)
    return {"one_liner": one_liner, "moods": mood[:4], "keywords": top_kw, "level": 1}

def movie_summary(imdb_id, style, titles):
    """Generate movie summary in different styles."""
    t = titles.get(imdb_id, {})
    title = t.get("title", "Unknown")
    plot = (t.get("overview") or t.get("plot") or "")[:500]
    dirs = t.get("directors", [])
    dir_str = ", ".join(dirs) if isinstance(dirs, list) else str(dirs)
    prompts = {
        "eli5": f"Explain the movie \"{title}\" like I\'m 5 years old. Use simple words and fun comparisons.",
        "film_school": f"Give a film school analysis of \"{title}\" ({t.get('year','')}) directed by {dir_str}. Cover cinematography, themes, narrative structure, and cultural significance.",
        "pitch": f"Write a 2-sentence elevator pitch for \"{title}\" that would make someone want to watch it immediately.",
        "debate": f"Give both sides: why \"{title}\" is a masterpiece AND why it\'s overrated. Be provocative.",
    }
    prompt = prompts.get(style, prompts["pitch"])
    return llm_ask(prompt + f"\nPlot summary: {plot}", max_tokens=400)

def auto_tag_title(imdb_id, titles):
    """Generate mood/theme tags for a title via LLM."""
    t = titles.get(imdb_id, {})
    plot = (t.get("overview") or t.get("plot") or "")[:400]
    genres = t.get("genres", "")
    prompt = f"For the movie \"{t.get('title','')}\" ({genres}): {plot}\n\nGenerate 5-8 mood/theme tags. Examples: mind-bending, feel-good, slow-burn, visually-stunning, thought-provoking, edge-of-seat, tear-jerker, dark-humor. Return ONLY comma-separated tags."
    result = llm_ask(prompt, max_tokens=60)
    if result:
        tags = [tag.strip().lower() for tag in result.split(",") if tag.strip()]
        return tags[:8]
    return []

def generate_watchlist_rss(user):
    """Generate RSS feed that drip-releases one watchlist item per day."""
    wl = load_watchlist(user)
    titles = load_titles()
    from datetime import datetime, timedelta
    items = ""
    for i, iid in enumerate(wl[:30]):
        t = titles.get(iid, {})
        pub_date = (datetime.now() - timedelta(days=len(wl)-i)).strftime("%a, %d %b %Y 08:00:00 +0000")
        poster = t.get("poster", "")
        desc = (t.get("overview") or "")[:200]
        items += f"""<item><title>{esc(t.get("title","?"))} ({t.get("year","")})</title>
<link>https://www.imdb.com/title/{iid}/</link>
<description><![CDATA[<img src="{poster}" width="100"><br>{desc}]]></description>
<pubDate>{pub_date}</pubDate><guid>{iid}</guid></item>\n"""
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel>
<title>{esc(user)}\'s Watchlist</title>
<description>Daily movie recommendation from your watchlist</description>
<link>{BASE}/watchlist/{user}</link>
{items}</channel></rss>"""

def fetch_letterboxd_data(imdb_id, title):
    """Fetch mood/theme data from Letterboxd (public page scraping)."""
    try:
        url = f"https://letterboxd.com/imdb/{imdb_id}/"
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=10)
        text = resp.read().decode(errors="replace")
        import re
        # Extract themes from nano-genre tags
        themes = re.findall(r'href="/films/theme/([^/"]+)/"', text)
        # Extract average rating
        rating_m = re.search(r'name="twitter:data2" content="([\d.]+) out of', text)
        lb_rating = float(rating_m.group(1)) if rating_m else None
        return {"themes": themes[:10], "lb_rating": lb_rating}
    except: return {}

def search_internet_archive(query, limit=10):
    """Search Internet Archive for free movies."""
    url = "https://archive.org/advancedsearch.php?q=" + urllib.parse.quote(query)
    url += "+mediatype:movies&fl[]=identifier,title,year,description,avg_rating,downloads"
    url += "&sort[]=downloads+desc&rows=" + str(limit) + "&output=json"
    data = api_get(url)
    if data and data.get("response"):
        return [{"id": d.get("identifier",""), "title": d.get("title",""), "year": d.get("year",""),
                 "desc": (d.get("description","") or "")[:150], "rating": d.get("avg_rating",""),
                 "url": "https://archive.org/details/" + d.get("identifier","")}
                for d in data["response"].get("docs", [])]
    return []

def notify(message, user=""):
    """Send notification via configured webhook/Signal."""
    webhook_url = _load_key("webhook_url")
    if not webhook_url: return
    try:
        data = json.dumps({"text": message, "user": user}).encode()
        req = urllib.request.Request(webhook_url, data=data, headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=5)
    except: pass

def generate_tasks_for_library(user):
    """Analyze library and enqueue tasks for the agent across all work lanes."""
    import re as _re
    library = load_user_tmm(user)
    if not library: return 0
    titles = load_titles()

    db_clear_auto_tasks()

    new_tasks = []
    def _add(task_type, params, priority):
        db_enqueue_task(task_type, params, priority)
        new_tasks.append(task_type)

    sub_lang = _load_key("sub_language") or "eng"
    needs_size, needs_hash, needs_subs, needs_quality = [], [], [], []
    needs_validate, needs_ocr, dupe_groups = [], [], []

    # ── Collect all needs across dict and list entries ──
    for iid, val in library.items():
        if iid.startswith("_"): continue
        entries = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
        t = titles.get(iid, {})
        try:
            expected_runtime = int(t.get("runtime", 0) or 0)
        except (ValueError, TypeError):
            expected_runtime = 0

        for info in entries:
            if not isinstance(info, dict) or not info.get("path"): continue
            path = info["path"]
            # Disk lane: sizing
            if not info.get("file_size") and not info.get("size"):
                needs_size.append((iid, path))
            # Disk lane: hashing (requires size first)
            if info.get("file_size") and not info.get("file_hash"):
                needs_hash.append((iid, path))
            # Disk lane: quality/mediainfo
            if not info.get("video_codec") and not info.get("quality"):
                needs_quality.append(path)
            # API lane: subtitles
            if not info.get("subtitles") and not info.get("suggested_sub"):
                needs_subs.append((iid, path))
            # Disk lane: runtime validation (only for sized files with expected runtime)
            if expected_runtime and info.get("file_size") and not info.get("runtime_verified"):
                try:
                    actual = int(info.get("runtime", 0) or 0)
                except (ValueError, TypeError):
                    actual = 0
                # Flag if runtime differs by >15 min (skip IFO/kodi bogus values >10000)
                if actual and actual < 1000 and abs(actual - expected_runtime) > 15:
                    needs_validate.append({"path": path, "imdb_id": iid, "expected_runtime": expected_runtime})

        # Duplicate detection: multi-copy titles that are sized but not yet compared
        if isinstance(val, list) and len(val) >= 2:
            sized_entries = [e for e in val if isinstance(e, dict) and e.get("file_size") and e.get("path")]
            if len(sized_entries) >= 2 and not any(e.get("dupe_checked") for e in val):
                dupe_groups.append((iid, [e["path"] for e in sized_entries]))

    # ── Title mismatches needing OCR identification ──
    mismatches = find_mismatches(user)
    for m in mismatches:
        info = library.get(m["iid"])
        if isinstance(info, dict) and info.get("path") and not info.get("ocr_checked"):
            needs_ocr.append({"path": info["path"], "imdb_id": m["iid"]})

    # ── Transcoding candidates: DVD_TS folders or high-bitrate files ──
    transcode_candidates = []
    for iid, val in library.items():
        if iid.startswith("_"): continue
        entries = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
        for info in entries:
            if not isinstance(info, dict) or not info.get("path"): continue
            path = info["path"]
            # DVD_TS folders → transcode to MKV
            if "VIDEO_TS" in path or "DVD_TS" in path:
                if not info.get("transcoded"):
                    folder = _re.sub(r"/VIDEO_TS.*$", "", path)
                    transcode_candidates.append(folder)

    # ── Enqueue tasks by lane ──

    # DISK LANE: sizing (batches of 50)
    for i in range(0, len(needs_size), 50):
        batch = needs_size[i:i+50]
        _add("size_files", {"paths": [p for _, p in batch], "imdb_ids": [iid for iid, _ in batch]}, PRIORITY_QUALITY)

    # DISK LANE: hashing (batches of 50)
    for i in range(0, len(needs_hash), 50):
        batch = needs_hash[i:i+50]
        _add("hash_files", {"paths": [p for _, p in batch]}, PRIORITY_QUALITY)

    # DISK LANE: quality/mediainfo (batches of 50)
    for i in range(0, len(needs_quality), 50):
        _add("check_quality", {"paths": needs_quality[i:i+50]}, PRIORITY_QUALITY)

    # DISK LANE: runtime validation (batches of 20, lower priority)
    for i in range(0, min(len(needs_validate), 200), 20):
        _add("validate_match", {"items": needs_validate[i:i+20]}, PRIORITY_QUALITY)

    # DISK LANE: duplicate detection (batches of 10 groups)
    for i in range(0, min(len(dupe_groups), 100), 10):
        batch = dupe_groups[i:i+10]
        all_paths = []
        for _, paths in batch:
            all_paths.extend(paths)
        _add("find_duplicates", {"paths": all_paths}, PRIORITY_DUPES)

    # CPU LANE: transcoding (one per task, capped at 5 per run)
    seen_transcode = set()
    for folder in transcode_candidates[:5]:
        if folder not in seen_transcode:
            seen_transcode.add(folder)
            _add("transcode_dvd", {"path": folder, "crf": 20, "preset": "medium"}, PRIORITY_QUALITY)

    # CPU LANE: audio merge (better video + better audio from different copies, capped at 5)
    merge_candidates = []
    for iid, val in library.items():
        if iid.startswith("_") or not isinstance(val, list) or len(val) < 2: continue
        entries = [e for e in val if isinstance(e, dict) and e.get("file_size") and e.get("audio") and e.get("path")]
        if len(entries) < 2: continue
        if any(e.get("audio_merged") for e in entries): continue
        def _audio_score(e):
            tracks = e.get("audio", [])
            if not isinstance(tracks, list): return 0
            return max((t.get("channels", 0) * (5 if "dts" in t.get("codec", "").lower() or "truehd" in t.get("codec", "").lower() else 3 if "ac3" in t.get("codec", "").lower() or "eac3" in t.get("codec", "").lower() else 1) for t in tracks if isinstance(t, dict)), default=0)
        scored = [(e, e.get("video_width", 0), _audio_score(e)) for e in entries]
        best_video = max(scored, key=lambda x: x[1])
        best_audio = max(scored, key=lambda x: x[2])
        if best_video[0] is not best_audio[0] and best_audio[2] > best_video[2] * 2:
            merge_candidates.append({"target": best_video[0]["path"], "source": best_audio[0]["path"], "dry_run": True})
    for m in merge_candidates[:5]:
        _add("merge_audio", m, PRIORITY_QUALITY)

    # API LANE: subtitles (capped at 50 per run)
    for iid, path in needs_subs[:50]:
        _add("download_subs", {"imdb_id": iid, "path": path, "language": sub_lang,
             "os_user": _load_key("opensubs_user"), "os_pass": _load_key("opensubs_pass")}, PRIORITY_SUBS)

    # API LANE: OCR identification for title mismatches (capped at 5, expensive)
    for item in needs_ocr[:25]:
        _add("identify_movie", {"path": item["path"], "imdb_id": item["imdb_id"]}, PRIORITY_SUBS)

    # CPU LANE: visual verification via TMDB stills (low priority, 5 per run)
    try:
        db = get_db()
        unverified = db.execute("""SELECT path, imdb_id FROM verification
            WHERE status IN ('mismatch','possible_variant') AND step='duration'
            ORDER BY ts LIMIT 20""").fetchall()
        stills_queued = 0
        for path, iid in unverified:
            t = titles.get(iid, {})
            stills = t.get("stills", [])
            if stills and stills_queued < 5:
                _add("verify_stills", {"path": path, "stills": stills[:3], "imdb_id": iid}, PRIORITY_SUBS)
                stills_queued += 1
    except: pass

    # Scan incoming folder if configured
    incoming = _load_key("incoming_path")
    if incoming:
        code = (
            "import os, json, urllib.request, re\n"
            "incoming = " + repr(incoming) + "\n"
            "mp = incoming\n"
            "for src, dst in config.get('_path_mappings', {}).items():\n"
            "    if incoming.startswith(src): mp = dst + incoming[len(src):]\n"
            "mp = mp.replace('/', os.sep) if os.name == 'nt' else mp\n"
            "found = []\n"
            "for root, dirs, files in os.walk(mp):\n"
            "    for f in files:\n"
            "        if f.lower().endswith(('.mkv','.mp4','.avi','.m4v')):\n"
            "            fp = os.path.join(root, f)\n"
            "            sz = os.path.getsize(fp) if os.path.isfile(fp) else 0\n"
            "            if sz > 50000000:\n"
            "                nfs = fp.replace(os.sep, '/')\n"
            "                for dst2, src2 in config.get('_path_mappings', {}).items():\n"
            "                    if nfs.startswith(dst2): nfs = src2 + nfs[len(dst2):]\n"
            "                found.append({'path': nfs, 'filename': f, 'size': sz})\n"
            "                log('[incoming] ' + f + ' (' + str(round(sz/1073741824,1)) + ' GB)')\n"
            "result = {'files': found}\n"
            "log('[incoming] Found ' + str(len(found)) + ' files')\n"
        )
        _add("exec_code", {"code": code, "description": "Scan incoming folder"}, PRIORITY_QUALITY)

    from collections import Counter
    counts = Counter(new_tasks)
    summary = ", ".join(f"{n} {t}" for t, n in counts.most_common())
    print(f"Generated {len(new_tasks)} tasks for {user}: {summary}")
    return len(new_tasks)


def scan_staging_upgrades(user):
    """Scan staging folders for files that upgrade existing library entries. Returns list of upgrade suggestions."""
    import re
    staging_raw = _load_key("staging_paths") or _load_key("incoming_path") or ""
    staging_paths = [p.strip() for p in staging_raw.replace(",", "\n").split("\n") if p.strip()]
    if not staging_paths:
        return []

    library = load_user_tmm(user)
    episodes = library.get("_episodes", {})
    titles = load_titles()
    upgrades = []

    # Parse show/movie info from filename
    def parse_filename(fname):
        # TV: Show.Name.S01E02.stuff.mkv
        m = re.match(r"(.+?)[.\s_-]+S(\d{1,2})E(\d{1,2})", fname, re.I)
        if m:
            show = m.group(1).replace(".", " ").replace("_", " ").strip()
            return {"type": "episode", "show": show, "season": int(m.group(2)), "episode": int(m.group(3)), "filename": fname}
        # Movie: Title.(Year).stuff.mkv
        m2 = re.match(r"(.+?)[.\s_-]+\(?(\d{4})\)?", fname)
        if m2:
            title = m2.group(1).replace(".", " ").replace("_", " ").strip()
            return {"type": "movie", "title": title, "year": int(m2.group(2)), "filename": fname}
        return None

    def quality_from_filename(fname):
        """Extract quality signals from filename."""
        f = fname.upper()
        score = 0
        res = 0
        if "2160P" in f or "4K" in f: res = 2160
        elif "1080P" in f: res = 1080
        elif "720P" in f: res = 720
        if "REMUX" in f: score += 50
        if "BLURAY" in f or "BLU-RAY" in f: score += 30
        if "WEB-DL" in f or "WEBDL" in f: score += 20
        if "ATVP" in f or "AMZN" in f or "NF" in f: score += 15
        if "DTS-HD" in f or "TRUEHD" in f or "ATMOS" in f: score += 20
        if "EAC3" in f or "DDP" in f or "AC3" in f: score += 10
        if "5.1" in f or "7.1" in f: score += 10
        if "HEVC" in f or "X265" in f or "H265" in f: score += 5
        return {"resolution": res, "score": score + res}

    # Scan staging folders
    staged_files = []
    for sp in staging_paths:
        # Agent will have reported files via exec_code, but we can also check the incoming DB
        db = get_db()
        rows = db.execute("SELECT path, filename, size FROM incoming WHERE user=? AND status='new'", (user,)).fetchall()
        for r in rows:
            staged_files.append({"path": r[0], "filename": r[1], "size": r[2]})

    # Also check agent's last scan_extra report for non-TMM titles
    # For now, use whatever the agent reported as incoming
    if not staged_files:
        # Fallback: queue an exec_code scan of staging paths
        return []

    # Match staged files against library
    for sf in staged_files:
        parsed = parse_filename(sf["filename"])
        if not parsed:
            continue
        if parsed["type"] == "episode":
            # Find matching episode in library
            for ep_key, ep_data in episodes.items():
                if not isinstance(ep_data, dict):
                    continue
                if (ep_data.get("showtitle", "").lower() == parsed["show"].lower()
                    and ep_data.get("season") == parsed["season"]
                    and ep_data.get("episode") == parsed["episode"]):
                    # Compare quality
                    staged_q = quality_from_filename(sf["filename"])
                    existing_h = ep_data.get("video_height", 0) or 0
                    existing_ch = 2  # default stereo
                    audio = ep_data.get("audio", [])
                    if isinstance(audio, list) and audio:
                        existing_ch = max(t.get("channels", 2) for t in audio if isinstance(t, dict))
                    existing_score = existing_h + (existing_ch * 5)
                    if staged_q["score"] > existing_score:
                        upgrades.append({
                            "type": "episode",
                            "title": f"{ep_data.get('showtitle')} S{parsed['season']:02d}E{parsed['episode']:02d}",
                            "staged_path": sf["path"],
                            "staged_file": sf["filename"],
                            "staged_quality": staged_q,
                            "existing_path": ep_data.get("path", ""),
                            "existing_quality": {"resolution": existing_h, "channels": existing_ch},
                            "improvement": staged_q["score"] - existing_score,
                        })
                    break
        elif parsed["type"] == "movie":
            # Find matching movie in library by title+year
            for iid, t in titles.items():
                if (t.get("title", "").lower() == parsed["title"].lower()
                    and str(t.get("year", "")) == str(parsed["year"])
                    and iid in library):
                    staged_q = quality_from_filename(sf["filename"])
                    entries = library[iid] if isinstance(library[iid], list) else [library[iid]]
                    for e in entries:
                        if not isinstance(e, dict):
                            continue
                        existing_h = e.get("video_width", 0) or 0
                        if staged_q["resolution"] > existing_h:
                            upgrades.append({
                                "type": "movie",
                                "title": f"{t.get('title')} ({t.get('year')})",
                                "imdb_id": iid,
                                "staged_path": sf["path"],
                                "staged_file": sf["filename"],
                                "staged_quality": staged_q,
                                "existing_path": e.get("path", ""),
                                "existing_quality": {"resolution": existing_h},
                                "improvement": staged_q["score"] - existing_h,
                            })
                        break
                    break

    upgrades.sort(key=lambda x: -x["improvement"])
    return upgrades


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

def complete_task(task_id, result=None):
    """Mark task complete (SQLite) and apply results."""
    # Store in SQLite
    db_complete_task(task_id, result)
    # Build task dict for _apply_task_result
    db = get_db()
    row = db.execute("SELECT * FROM task_queue WHERE id=?", (task_id,)).fetchone()
    task = dict(row) if row else None
    if task and task.get("params"):
        task["params"] = json.loads(task["params"]) if isinstance(task["params"], str) else task["params"]
    # Handle batch results (nfo_batch_, thumb_)
    if not task and (task_id.startswith("nfo_batch_") or task_id.startswith("thumb_")) and result:
        task = {"id": task_id, "type": "exec_code", "params": {}}
    # Apply results to library
    if task and result and not result.get("error"):
        _apply_task_result(task, result)
    # Trim old done tasks
    db_trim_done(500)

def _apply_task_result(task, result):
    """Update library with task results."""
    ttype = task["type"]
    params = task.get("params", {})
    # Handle tasks that don't use the standard "data" format
    if ttype == "download_subs":
        # Queue sync_subs for any newly downloaded subtitles
        path = params.get("path", "")
        results_list = result.get("results", [])
        if path and any(r.get("status") == "downloaded" for r in results_list):
            db_enqueue_task("sync_subs", {"path": path}, PRIORITY_SUBS)
        # Store identity confirmation from OpenSubtitles hash match
        imdb_id = params.get("imdb_id", "")
        for r in results_list:
            if r.get("identity_confirmed") and r.get("os_imdb"):
                os_iid = r["os_imdb"]
                if os_iid == imdb_id:
                    db_set_agent_data("ecb", imdb_id, "hash_confirmed", "1")
                elif imdb_id:
                    # OS says different movie than what we think — flag mismatch
                    db_set_agent_data("ecb", imdb_id, "hash_mismatch", os_iid)
                    print(f"[identity] ⚠ Hash says {os_iid} ({r.get('os_title','')}) but library has {imdb_id}")
                break
        return
    if ttype == "identify_movie":
        db = get_db()
        path = result.get("path", "")
        # Check if this was a mismatch — flag truncated files for human review
        dur_row = db.execute("SELECT result FROM verification WHERE path=? AND step='duration'", (path,)).fetchone()
        status = "done"
        if dur_row:
            dur = json.loads(dur_row[0])
            actual = dur.get("actual_min", 0)
            expected = dur.get("expected_min", 0)
            if actual < expected * 0.5:
                status = "truncated"  # Less than half expected = definitely broken
            elif actual < expected - 15:
                status = "review_needed"
        db.execute("""INSERT OR REPLACE INTO verification (path, imdb_id, step, status, result, version, ts)
            VALUES (?, ?, 'identify', ?, ?, ?, ?)""",
            (path, "", status, json.dumps(result), VERIFY_VERSION, time.strftime("%Y-%m-%dT%H:%M:%S")))
        db.commit()
        # Queue NFO write to persist OCR data alongside the file
        if path and result.get("frames_ocr", 0) > 0:
            db_enqueue_task("write_nfo_verification", {"path": path, "ocr_data": result, "status": status}, 35)
        print(f"[tasks] Stored OCR result [{status}] for {path[-40:]}")
        return
    if ttype == "validate_match":
        _apply_verification_result(result)
        return
    data = result.get("data", {})
    if not data:
        print(f"[apply] No data for {ttype}")
        return
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
                    if iid:
                        db_set_agent_data(user, iid, "file_size", data[path])
            updated = False
        elif ttype == "hash_files":
            for path, info in data.items():
                for iid, lib_info in library.items():
                    if isinstance(lib_info, dict) and lib_info.get("path") == path:
                        if info.get("hash"): db_set_agent_data(user, iid, "file_hash", info["hash"])
                        if info.get("size"): db_set_agent_data(user, iid, "file_size", info["size"])
            updated = False
        elif ttype == "check_quality":
            for path, info in data.items():
                # Normalize path for matching (strip nfs:// prefix)
                norm_path = path
                if "nfs://" in norm_path:
                    import re as _re
                    m = _re.match(r"nfs://[^/]+(/volume1)?(/.+)", norm_path)
                    if m: norm_path = m.group(2)
                for iid, lib_entry in library.items():
                    if iid.startswith("_"): continue
                    entries = lib_entry if isinstance(lib_entry, list) else [lib_entry] if isinstance(lib_entry, dict) else []
                    for e in entries:
                        if not isinstance(e, dict): continue
                        ep = e.get("path", "")
                        # Normalize library path too
                        ep_norm = ep
                        if "nfs://" in ep_norm:
                            m2 = _re.match(r"nfs://[^/]+(/volume1)?(/.+)", ep_norm)
                            if m2: ep_norm = m2.group(2)
                        if ep_norm == norm_path or ep == path or norm_path.endswith(ep_norm.split("/")[-1] if "/" in ep_norm else ep_norm):
                            # Flatten video/audio into top-level for UI access
                            if info.get("video"):
                                e["video_codec"] = info["video"].get("codec", "")
                                e["video_width"] = info["video"].get("width", 0)
                                e["video_height"] = info["video"].get("height", 0)
                                e["hdr"] = info["video"].get("hdr", "no")
                            if info.get("audio") and isinstance(info["audio"], list):
                                e["audio"] = info["audio"]
                            if info.get("bpp"): e["bpp"] = info["bpp"]
                            if info.get("quality_verdict"): e["quality_verdict"] = info["quality_verdict"]
                            if info.get("container"): e["container"] = info["container"]
                            if info.get("bitrate"): e["bitrate"] = info["bitrate"]
                            if info.get("duration"): e["runtime"] = info["duration"] // 60
                            updated = True
                            break
            return  # Skip the "no data" check below
        elif (ttype == "scan_incoming" or (ttype == "exec_code" and data.get("files") is not None)):
            # Incoming folder scan results
            incoming_file = os.path.join(DATA_DIR, "users", user, "incoming.json") if user != "default" else None
            if incoming_file:
                existing = safe_json_load(incoming_file) or []
                existing_paths = {e["path"] for e in existing}
                new_files = [f for f in data["files"] if f["path"] not in existing_paths]
                if new_files:
                    # Parse filenames and try to match
                    for f in new_files:
                        parsed = parse_movie_filename(f["filename"])
                        f["title_guess"] = parsed.get("title", "")
                        f["year_guess"] = parsed.get("year", "")
                        f["quality"] = parsed.get("quality", "")
                        f["status"] = "pending"
                        # Auto-search TMDB for match
                        if TMDB_KEY and f["title_guess"]:
                            q = urllib.parse.quote(f["title_guess"])
                            url = f"https://api.themoviedb.org/3/search/multi?api_key={TMDB_KEY}&query={q}"
                            if f["year_guess"]: url += "&year=" + f["year_guess"]
                            results = api_get(url)
                            if results and results.get("results"):
                                r = results["results"][0]
                                f["tmdb_match"] = {
                                    "id": r.get("id"),
                                    "title": r.get("title") or r.get("name", ""),
                                    "year": (r.get("release_date") or r.get("first_air_date") or "")[:4],
                                    "type": r.get("media_type", "movie"),
                                    "poster": ("https://image.tmdb.org/t/p/w92" + r["poster_path"]) if r.get("poster_path") else "",
                                }
                            time.sleep(0.1)
                    existing.extend(new_files)
                    safe_json_save(incoming_file, existing)
                    updated = True
                    print(f"[incoming] {len(new_files)} new files from incoming folder")
        elif task.get("id", "").startswith("thumb_") or ttype == "generate_thumb":
            # Thumbnail results: {nfs_path: base64_jpg}
            PATH_MAP = {"//zeus/Movies": "nfs://192.168.0.235/volume1/Movies",
                        "//zeus/TVShows": "nfs://192.168.0.235/volume1/TVShows",
                        "//zeus/V_HD": "nfs://192.168.0.235/volume1/V_HD"}
            print(f"[thumb] Processing {len(data)} thumbnails")
            for path, b64 in data.items():
                print(f"[thumb] path={path[:60]} b64_len={len(b64)}")
                nfs_path = path.replace("\\", "/")
                for smb, nfs in PATH_MAP.items():
                    if nfs_path.startswith(smb):
                        nfs_path = nfs + nfs_path[len(smb):]
                        break
                for lib_iid, lib_info in library.items():
                    if isinstance(lib_info, dict) and nfs_path in lib_info.get("path", ""):
                        import base64 as _b64
                        thumb_dir = os.path.join(DATA_DIR, "thumbnails")
                        os.makedirs(thumb_dir, exist_ok=True)
                        fname = lib_iid.replace("/","_") + ".jpg"
                        with open(os.path.join(thumb_dir, fname), "wb") as tf:
                            tf.write(_b64.b64decode(b64))
                        agent = load_agent_data(user)
                        agent.setdefault(lib_iid, {})["thumbnail"] = fname
                        save_agent_data(user, agent)
                        break
        elif ttype == "exec_code" and (task.get("id", "").startswith("nfo_") or task.get("id", "").startswith("nfo_batch_")):
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

# ── API helpers ───────────────────────────────────────────────────────
# ── Adaptive Rate Limiter ──────────────────────────────────────────────
_rate_last = {}   # domain -> last_request_time
_rate_fails = {}  # domain -> consecutive_failure_count
_rate_backoff = {} # domain -> backoff_until_time

def _is_tv(t):
    """Check if a title is a TV show (handles inconsistent type values)."""
    tp = (t.get("type") or "").lower().replace(" ", "")
    return tp in ("tvseries","tvminiseries","tv","tvepisode")

def _rate_domain(url):
    for d in ("themoviedb","omdbapi","thetvdb","trakt","archive.org","letterboxd"):
        if d in url: return d
    return "default"

def _rate_wait(url):
    """Rate limit + adaptive backoff. Call before every external request."""
    domain = _rate_domain(url)
    now = time.time()
    # Check backoff
    until = _rate_backoff.get(domain, 0)
    if now < until:
        wait = min(until - now, 300)
        time.sleep(wait)
        return
    # Normal rate limit (2s for TMDB to avoid connection resets with concurrent threads)
    last = _rate_last.get(domain, 0)
    min_gap = 2.0 if domain == "themoviedb" else 1.0
    if now - last < min_gap:
        time.sleep(min_gap - (now - last))
    _rate_last[domain] = time.time()

def _rate_ok(url):
    """Report success — reset failure counter."""
    _rate_fails[_rate_domain(url)] = 0
    _rate_backoff[_rate_domain(url)] = 0

def _rate_fail(url):
    """Report failure — escalate backoff after 3+ consecutive failures."""
    domain = _rate_domain(url)
    fails = _rate_fails.get(domain, 0) + 1
    _rate_fails[domain] = fails
    if fails >= 3:
        delay = {3:30, 5:120, 8:600, 12:1800}.get(fails, 3600 if fails < 50 else 21600)
        for threshold in sorted({3:30, 5:120, 8:600, 12:1800}.keys(), reverse=True):
            if fails >= threshold:
                delay = {3:30, 5:120, 8:600, 12:1800}[threshold]
                break
        _rate_backoff[domain] = time.time() + delay
        print(f"[rate] {domain}: {fails} failures, backing off {delay}s")

def _rate_status():
    """Get rate limiter status for all domains."""
    now = time.time()
    return {d: {"fails": _rate_fails.get(d,0), "backed_off": _rate_backoff.get(d,0) > now,
                "backoff_remaining": max(0, int(_rate_backoff.get(d,0) - now))}
            for d in set(list(_rate_fails.keys()) + list(_rate_backoff.keys()))}

def api_get(url, headers=None):
    _rate_wait(url)
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0", **(headers or {})})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            _rate_ok(url)
            return json.loads(r.read())
    except Exception as e:
        err = str(e)
        if "429" in err or "500" in err or "502" in err or "503" in err:
            _rate_fail(url)
        if "401" not in err and "404" not in err: print(f"API error {url[:80]}: {e}")
        return None

def api_post(url, data, headers=None):
    req = urllib.request.Request(url, data=json.dumps(data).encode(),
        headers={"User-Agent": "Mozilla/5.0", **(headers or {}), "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r: return json.loads(r.read())
    except Exception as e: print(f"API POST error {url[:80]}: {e}"); return None

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
    if not data:
        print(f"[tmdb] No data for {imdb_id}")
        return {}
    movies, shows = data.get("movie_results") or [], data.get("tv_results") or []
    if not movies and not shows: return {}
    is_tv = len(shows) > 0
    r = shows[0] if is_tv else movies[0]
    tmdb_id = r["id"]; kind = "tv" if is_tv else "movie"
    result = {
        "poster": f"https://image.tmdb.org/t/p/w185{r['poster_path']}" if r.get("poster_path") else "",
        "overview": r.get("overview", ""), "tmdb_rating": r.get("vote_average"), "tmdb_id": tmdb_id,
        "original_language": r.get("original_language", ""),
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
    # Alternative titles (for matching foreign/localized names)
    alt = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/alternative_titles?api_key={TMDB_KEY}")
    if alt:
        alt_list = alt.get("titles") or alt.get("results") or []
        result["alt_titles"] = [a["title"] for a in alt_list if a.get("title")][:15]
    # Release dates with alternate runtimes (Director's Cut, Extended, etc)
    if not is_tv:
        rd = api_get(f"https://api.themoviedb.org/3/movie/{tmdb_id}/release_dates?api_key={TMDB_KEY}")
        if rd:
            runtimes = set()
            for country in rd.get("results", []):
                for rel in country.get("release_dates", []):
                    note = rel.get("note", "").lower()
                    rt_min = rel.get("runtime", 0)
                    if rt_min and rt_min > 0:
                        runtimes.add(rt_min)
            if len(runtimes) > 1:
                result["alt_runtimes"] = sorted(runtimes)
    # Stills/backdrops for visual verification
    images = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/images?api_key={TMDB_KEY}")
    if images:
        stills = images.get("stills") or images.get("backdrops") or []
        if stills:
            result["stills"] = [f"https://image.tmdb.org/t/p/w300{s['file_path']}" for s in stills[:5]]
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

LANG_NAMES = {"eng":"English","ger":"German","deu":"German","fre":"French","fra":"French",
    "spa":"Spanish","ita":"Italian","por":"Portuguese","dut":"Dutch","nld":"Dutch",
    "swe":"Swedish","dan":"Danish","fin":"Finnish","nor":"Norwegian","pol":"Polish",
    "cze":"Czech","ces":"Czech","hun":"Hungarian","rum":"Romanian","ron":"Romanian",
    "tur":"Turkish","ara":"Arabic","chi":"Chinese","zho":"Chinese","jpn":"Japanese",
    "kor":"Korean","rus":"Russian","hin":"Hindi","tha":"Thai","heb":"Hebrew","und":"Unknown"}

SOURCE_ICONS = {"bluray": "💿", "dvd": "📀", "webrip": "🌐", "webdl": "🌐", "hdtv": "📡", "telesync": "📹", "cam": "📷", "remux": "💎"}

def detect_video_source(path):
    p = path.lower()
    if "remux" in p: return "remux"
    if "blu-ray" in p or "bluray" in p or "brrip" in p or "bdmv" in p or "bdrip" in p: return "bluray"
    if "dvd" in p or "video_ts" in p: return "dvd"
    if "webrip" in p or "web-rip" in p: return "webrip"
    if "webdl" in p or "web-dl" in p or "web dl" in p: return "webdl"
    if "hdtv" in p: return "hdtv"
    if "telesync" in p: return "telesync"
    if "cam" in p.split("_") or "camrip" in p: return "cam"
    return ""

def _normalize(s):
    s = s.lower()
    # Expand umlauts first (ä→ae, ö→oe, ü→ue, ß→ss)
    for src, dst in [("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss")]:
        s = s.replace(src, dst)
    # Then collapse transliterations so both forms match (ae→a, oe→o, ue→u)
    for src, dst in [("ae","a"),("oe","o"),("ue","u")]:
        s = s.replace(src, dst)
    for src, dst in [("/"," "),("&","and"),("-"," ")]:
        s = s.replace(src, dst)
    for tag in ["1080p","720p","480p","2160p","4k","uhd","hdr","bluray","blu-ray","webrip",
                "brrip","dvdrip","hdtv","aac","ac3","dts","x264","x265","hevc","h264",
                "mbps","telesync","remastered","extended","directors.cut","unrated","multi"]:
        s = s.replace(tag.lower(), "")
    import unicodedata as _ud
    s = "".join(c for c in _ud.normalize("NFD", s) if _ud.category(c) != "Mn")
    import re as _re
    s = _re.sub(r"[()\[\]{}_.,:;!?\-'\"]", " ", s)
    s = _re.sub(r"\b(19|20)\d{2}\b", "", s)
    s = _re.sub(r"\b\d{1,3}\b", "", s)
    s = _re.sub(r"\s+", " ", s).strip()
    return s

import re as _re_mod
def _canonical_path(path):
    """Normalize library path: strip protocol/host/mount prefix, start at Movies/ or TVShows/."""
    p = path.replace("\\", "/").rstrip("/")
    m = _re_mod.search(r"(Movies|TVShows)(/.*)?$", p)
    return m.group(0) if m else p

def _extract_title_from_path(path):
    parts = path.replace("\\", "/").split("/")
    fname = parts[-1] if parts else ""
    if fname.lower() in ("video_ts.ifo","index.bdmv","movieobject.bdmv",""):
        fname = parts[-3] if len(parts) >= 3 else parts[-2] if len(parts) >= 2 else ""
    else:
        fname = fname.rsplit(".", 1)[0]
        fname = __import__("re").sub(r"(?i)\b(cd|disc|part)\d\b", "", fname)
    return _normalize(fname)

def _fuzzy_match(a, b):
    wa = set(a.split())
    wb = set(b.split())
    if not wa or not wb: return 0
    common = wa & wb
    return len(common) / max(len(wa), len(wb))

def detect_library_convention(user):
    """Analyze library paths to detect naming convention."""
    library = load_user_tmm(user)
    import re
    from collections import Counter
    separators = Counter()
    collections = Counter()
    patterns = Counter()
    total = 0
    for iid, info in library.items():
        if not isinstance(info, dict) or iid.startswith("_"): continue
        path = info.get("path", "")
        if not path: continue
        total += 1
        # Normalize away NFS prefix
        for prefix in ["nfs://192.168.0.235/volume1/Movies/", "nfs://192.168.0.235/volume1/"]:
            if path.startswith(prefix): path = path[len(prefix):]
        parts = path.split("/")
        if len(parts) >= 2: collections["/".join(parts[:2])] += 1
        folder = parts[-2] if len(parts) >= 2 else ""
        separators["_" if "_" in folder else " "] += 1
        if len(parts) >= 3 and re.match(r"\d{4}", parts[2]): patterns["year_folder"] += 1
        for tag in ["Blu-ray","Webrip","Web-DL","DVD","Telesync","HDRip"]:
            if tag.lower() in folder.lower(): patterns["source_tag"] += 1; break
        if re.search(r"\d+\.\d+.Mbps", folder): patterns["bitrate"] += 1
    if not total: return {}
    sep = separators.most_common(1)[0][0] if separators else "_"
    coll = collections.most_common(1)[0][0] if collections else "Movies"
    return {
        "separator": sep,
        "collection": coll,
        "year_folder": patterns.get("year_folder", 0) > total * 0.5,
        "source_tag": patterns.get("source_tag", 0) > total * 0.5,
        "bitrate": patterns.get("bitrate", 0) > total * 0.5,
        "total": total,
        "template": "{collection}/{year}/{title}{sep}({year}){sep}{quality}{sep}{codec}{sep}{bitrate}{sep}{source}/{filename}.{ext}"
    }

def build_destination_path(title, year, quality, codec, bitrate, source, ext, convention):
    """Build a destination path following the detected convention."""
    sep = convention.get("separator", "_")
    coll = convention.get("collection", "YiFY/TMM")
    parts = [title.replace(" ", sep)]
    parts.append(f"({year})")
    if quality: parts.append(quality)
    if codec: parts.append(codec)
    if bitrate and convention.get("bitrate"): parts.append(bitrate)
    if source and convention.get("source_tag"): parts.append(source)
    folder_name = sep.join(parts)
    fname = sep.join(parts) + "." + ext
    if convention.get("year_folder"):
        return f"{coll}/{year}/{folder_name}/{fname}"
    return f"{coll}/{folder_name}/{fname}"

def find_mismatches(user, threshold=0.2):
    library = load_user_tmm(user)
    titles = load_titles()
    mismatches = []
    pull_queue = []
    for iid, info in library.items():
        if iid.startswith("_") or not isinstance(info, dict): continue
        path = info.get("path", "")
        if not path or info.get("confirmed"): continue
        t = titles.get(iid, {})
        db_title = t.get("title", "")
        if not db_title: continue
        path_title = _extract_title_from_path(path)
        norm_db = _normalize(db_title)
        if not path_title or not norm_db: continue
        score = _fuzzy_match(norm_db, path_title)
        best_via = "title"
        # Check originalTitle
        for alt in [t.get("originalTitle", ""), t.get("original_title", "")]:
            if not alt: continue
            alt_norm = _normalize(alt)
            if not alt_norm: continue
            s = _fuzzy_match(alt_norm, path_title)
            if alt_norm in path_title or path_title in alt_norm:
                s = max(s, 0.85)
            if s > score: score, best_via = s, "originalTitle"
        # Check alt_titles
        for alt in (t.get("alt_titles") or []):
            if not alt: continue
            alt_norm = _normalize(alt)
            if not alt_norm: continue
            s = _fuzzy_match(alt_norm, path_title)
            if alt_norm in path_title or path_title in alt_norm:
                s = max(s, 0.85)
            if s > score: score, best_via = s, "alt:" + alt[:20]
        if score < threshold and not t.get("alt_titles") and t.get("tmdb_id"):
            pull_queue.append(iid)
        if score < threshold:
            mismatches.append({"iid": iid, "db_title": db_title, "year": str(t.get("year","")),
                "path_title": path_title, "path": path, "match": round(score, 2), "via": best_via})
    # Batch-pull alt titles for mismatches missing them
    if pull_queue and TMDB_KEY:
        changed = False
        for iid in pull_queue[:50]:
            t = titles[iid]
            kind = "tv" if _is_tv(t) else "movie"
            alt = api_get(f"https://api.themoviedb.org/3/{kind}/{t['tmdb_id']}/alternative_titles?api_key={TMDB_KEY}")
            if alt:
                alt_list = alt.get("titles") or alt.get("results") or []
                t["alt_titles"] = [a["title"] for a in alt_list if a.get("title")][:20]
                changed = True
        if changed:
            save_titles(titles)
            for m in mismatches:
                if m["iid"] in pull_queue:
                    t = titles.get(m["iid"], {})
                    for alt in (t.get("alt_titles") or []):
                        if not alt: continue
                        alt_norm = _normalize(alt)
                        if not alt_norm: continue
                        s = _fuzzy_match(alt_norm, m["path_title"])
                        if alt_norm in m["path_title"] or m["path_title"] in alt_norm:
                            s = max(s, 0.85)
                        if s > m["match"]: m["match"], m["via"] = s, "alt:" + alt[:20]
        mismatches = [m for m in mismatches if m["match"] < threshold]
    mismatches.sort(key=lambda x: x["match"])
    return mismatches

def compute_confidence(iid, entry, title_info):
    """Compute 0-100 confidence score that a library entry is correctly identified.
    Returns {score: int, signals: [{name, value, points}], conflicts: [str]}."""
    signals = []
    conflicts = []
    path = entry.get("path", "")

    # Signal 1: Filename→title match (max 25 pts)
    path_title = _extract_title_from_path(path) if path else ""
    db_title = title_info.get("title", "")
    if path_title and db_title:
        score = _fuzzy_match(_normalize(db_title), path_title)
        # Also check alt titles
        for alt in [title_info.get("originalTitle", ""), title_info.get("original_title", "")] + (title_info.get("alt_titles") or []):
            if alt:
                s = _fuzzy_match(_normalize(alt), path_title)
                if _normalize(alt) in path_title or path_title in _normalize(alt): s = max(s, 0.9)
                score = max(score, s)
        pts = int(score * 25)
        signals.append({"name": "title_match", "value": f"{int(score*100)}%", "points": pts})
        if score < 0.2: conflicts.append("filename doesn't match any known title")

    # Signal 2: Runtime match (max 20 pts)
    lib_runtime = entry.get("runtime", 0) or 0
    db_runtime = title_info.get("runtime", 0) or 0
    if lib_runtime > 0 and db_runtime > 0:
        diff = abs(lib_runtime - db_runtime)
        if diff <= 2: pts = 20
        elif diff <= 5: pts = 15
        elif diff <= 10: pts = 10
        elif diff <= 15: pts = 5
        else: pts = 0; conflicts.append(f"runtime mismatch: file={lib_runtime}m vs db={db_runtime}m")
        signals.append({"name": "runtime", "value": f"{lib_runtime}m vs {db_runtime}m", "points": pts})

    # Signal 3: Year in filename matches (max 15 pts)
    import re
    year_match = re.search(r'[\(._\- ]((?:19|20)\d{2})[\)._\- ]', path)
    db_year = title_info.get("year", 0) or 0
    if year_match and db_year:
        file_year = int(year_match.group(1))
        if file_year == db_year: pts = 15
        elif abs(file_year - db_year) == 1: pts = 10
        else: pts = 0; conflicts.append(f"year mismatch: file={file_year} vs db={db_year}")
        signals.append({"name": "year", "value": f"{file_year} vs {db_year}", "points": pts})

    # Signal 4: NFO/subtitle hash identity confirmation (max 30 pts)
    if entry.get("identity_confirmed"):
        signals.append({"name": "identity_confirmed", "value": "subtitle hash", "points": 30})
    elif entry.get("confirmed"):
        signals.append({"name": "user_confirmed", "value": "manual", "points": 30})

    # Signal 5: File size reasonable for resolution (max 10 pts)
    file_size = entry.get("file_size", 0) or 0
    height = entry.get("video_height", 0) or 0
    if file_size > 0 and lib_runtime > 0 and height > 0:
        gb = file_size / 1073741824
        # Expected size ranges (GB per hour)
        expected = {2160: (4, 80), 1080: (2, 40), 720: (1, 20), 480: (0.3, 8)}
        bracket = min(expected.keys(), key=lambda h: abs(h - height))
        lo, hi = expected[bracket]
        hours = lib_runtime / 60
        if lo * hours <= gb <= hi * hours: pts = 10
        elif gb < lo * hours * 0.5 or gb > hi * hours * 2: pts = 0; conflicts.append("unusual file size for resolution/runtime")
        else: pts = 5
        signals.append({"name": "file_size", "value": f"{gb:.1f}GB for {height}p/{lib_runtime}m", "points": pts})

    total = sum(s["points"] for s in signals)
    max_possible = 100
    return {"score": min(total, max_possible), "signals": signals, "conflicts": conflicts}


def llm_batch_check_translations(mismatches, max_check=20):
    """Use LLM to check if 0% mismatches are actually translations."""
    if not _load_key("llm_url"): return mismatches
    zeros = [m for m in mismatches if m["match"] == 0][:max_check]
    if not zeros: return mismatches
    lines = []
    for i, m in enumerate(zeros):
        lines.append(f'{i+1}. File: "{m["path_title"]}" → Movie: "{m["db_title"]}" ({m.get("year","")})')
    prompt = """I have movie files whose filenames don't match their database title. For each pair, determine if the filename is a translation, alternate title, or original-language title of the listed movie.

IMPORTANT: Answer NO if:
- The filename is a generic format name (like "avchd", "video_ts", "disc1")
- The filename is clearly a different movie
- The filename is a subtitle or bonus feature title

Answer YES only if the filename is genuinely the same movie in another language.

Reply with ONLY the number and YES or NO for each line. Example:
1. YES
2. NO

""" + "\n".join(lines)
    answer = llm_ask(prompt, system="You are a strict multilingual movie title verifier. Only answer YES if you are confident the filename refers to the same movie. When in doubt, answer NO.", max_tokens=max_check * 8)
    if not answer: return mismatches
    for line in answer.split("\n"):
        line = line.strip()
        import re as _re
        m2 = _re.match(r"(\d+)\.\s*(YES|NO)", line, _re.IGNORECASE)
        if m2:
            idx = int(m2.group(1)) - 1
            if 0 <= idx < len(zeros) and m2.group(2).upper() == "YES":
                zeros[idx]["match"] = 0.75
                zeros[idx]["via"] = "🤖 AI translation"
    return mismatches

def parse_movie_filename(filename):
    """Extract title, year, quality, 3D format from a media filename."""
    name = os.path.splitext(os.path.basename(filename))[0]
    # TV episode detection: extract show name before SxxExx
    import re as _re2
    ep_match = _re2.search(r"[. _-](?:[Ss](\d{1,2})[Ee](\d{1,2})|(\d{1,2})[xX](\d{2,3}))", name)
    if ep_match:
        show_name = name[:ep_match.start()]
        show_name = _re2.sub(r"[\.\-_]", " ", show_name).strip()
        season = int(ep_match.group(1) or ep_match.group(3))
        episode = int(ep_match.group(2) or ep_match.group(4))
        # Also try to get episode title (after SxxExx)
        rest = name[ep_match.end():]
        quality = ""
        for q in ["2160p", "1080p", "720p", "480p"]:
            if q.lower() in rest.lower(): quality = q; break
        return {"title": show_name, "year": "", "quality": quality, "is_3d": None,
                "is_tv": True, "season": season, "episode": episode, "filename": filename}
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
    if not data:
        print(f"[opensubs] No data for {imdb_id}")
        return []
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

def library_health_report(user):
    """Library health report — 10 checks like BrainyCat."""
    library = load_user_tmm(user)
    titles = load_titles()
    ratings = load_user_ratings(user)
    checks = []
    total = sum(1 for k, v in library.items() if not k.startswith("_") and isinstance(v, dict))
    # 1. Missing posters
    no_poster = sum(1 for iid in library if not iid.startswith("_") and not titles.get(iid, {}).get("poster"))
    checks.append({"name": "Posters", "icon": "🖼", "ok": total - no_poster, "total": total, "issue": f"{no_poster} missing"})
    # 2. Missing genres
    no_genre = sum(1 for iid in library if not iid.startswith("_") and not titles.get(iid, {}).get("genres"))
    checks.append({"name": "Genres", "icon": "🎭", "ok": total - no_genre, "total": total, "issue": f"{no_genre} missing"})
    # 3. Unrated in library
    unrated = sum(1 for iid in library if not iid.startswith("_") and iid not in ratings)
    checks.append({"name": "Rated", "icon": "⭐", "ok": total - unrated, "total": total, "issue": f"{unrated} unrated"})
    # 4. File sizes known
    sized = db_get_agent_field_count(user, "file_size") if get_db() else 0
    checks.append({"name": "File Sizes", "icon": "💾", "ok": sized, "total": total, "issue": f"{total - sized} unknown"})
    # 5. Hashed
    hashed = db_get_agent_field_count(user, "file_hash") if get_db() else 0
    checks.append({"name": "Hashed", "icon": "🔑", "ok": hashed, "total": total, "issue": f"{total - hashed} unhashed"})
    # 6. Duplicates
    dupes = sum(1 for v in library.values() if isinstance(v, list))
    checks.append({"name": "No Duplicates", "icon": "📋", "ok": total - dupes, "total": total, "issue": f"{dupes} duplicate sets"})
    # 7. Confirmed matches
    confirmed = sum(1 for iid, v in library.items() if isinstance(v, dict) and v.get("confirmed"))
    checks.append({"name": "Confirmed", "icon": "✅", "ok": confirmed, "total": total, "issue": f"{total - confirmed} unconfirmed"})
    # 8. Has keywords (for recommendations)
    has_kw = sum(1 for iid in library if not iid.startswith("_") and titles.get(iid, {}).get("keywords"))
    has_cast = sum(1 for iid in library if not iid.startswith("_") and titles.get(iid, {}).get("cast"))
    checks.append({"name": "Keywords", "icon": "🏷", "ok": has_kw, "total": total, "issue": f"{total - has_kw} missing"})
    # 9. Has streaming info
    has_stream = sum(1 for iid in library if not iid.startswith("_") and titles.get(iid, {}).get("providers"))
    checks.append({"name": "Streaming", "icon": "📺", "ok": has_stream, "total": total, "issue": f"{total - has_stream} unknown"})
    # 10. Overall quality score
    score = int(sum(c["ok"] for c in checks) / max(sum(c["total"] for c in checks), 1) * 100)
    return {"checks": checks, "score": score, "total": total}

def _notify_streaming_alerts(user):
    """Check and notify about new streaming availability."""
    alerts = get_available_alerts(user)
    if alerts:
        titles_str = ", ".join(a.get("title","")[:30] for a in alerts[:3])
        notify(f"🎬 {len(alerts)} watchlisted titles now streaming: {titles_str}", user)

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

def tastedive_similar(imdb_id, title):
    """Get similar titles from TasteDive. Cached in title store."""
    titles = load_titles()
    t = titles.get(imdb_id, {})
    if t.get("_tastedive"):
        return t["_tastedive"]
    q = urllib.parse.quote(title)
    data = api_get(f"https://tastedive.com/api/similar?q={q}&type=movie&limit=5&info=1")
    if not data:
        print(f"[tastedive] No data")
        return []
    results = [{"title": r.get("Name",""), "type": r.get("Type",""), "description": r.get("wTeaser","")} for r in data.get("Similar",{}).get("Results",[])]
    # Cache
    if imdb_id in titles:
        titles[imdb_id]["_tastedive"] = results
        save_titles(titles)
    return results

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


# ── Split-Part Episode Detection ──────────────────────────────────────
def detect_split_episodes(episodes):
    """Detect multi-file episodes (split parts) in the TV library.
    Returns dict: {show: [{season, episode, parts: [path,...], already_named: bool}]}"""
    from collections import defaultdict
    import re

    # Group episodes by show+season+episode
    ep_groups = defaultdict(list)
    for path, ep in episodes.items():
        if not isinstance(ep, dict): continue
        key = (ep.get("showtitle", ""), ep.get("season", 0), ep.get("episode", 0))
        ep_groups[key].append({"path": path, **ep})

    results = defaultdict(list)

    # Pattern 1: files already named with part/cd suffixes
    part_re = re.compile(r'[._\- ](part|pt|cd)(\d+)\.\w+$', re.IGNORECASE)

    for (show, season, episode), entries in ep_groups.items():
        if not show: continue
        # Multiple files for same episode number
        if len(entries) > 1:
            paths = [e["path"] for e in entries if e.get("path")]
            already = any(part_re.search(p) for p in paths)
            results[show].append({
                "season": season, "episode": episode,
                "parts": paths, "already_named": already
            })

    # Pattern 2: consecutive episode pairs that are likely one logical episode
    # (e.g. S04E01+E02 = one 90min episode split into 2x45min)
    show_seasons = defaultdict(lambda: defaultdict(list))
    for (show, season, episode), entries in ep_groups.items():
        if show and season > 0:
            show_seasons[show][season].extend(entries)

    for show, seasons in show_seasons.items():
        for season_num, eps in seasons.items():
            ep_nums = sorted(set(e.get("episode", 0) for e in eps))
            if len(ep_nums) < 2: continue
            # Look for pairs of consecutive episodes with similar file sizes
            # (split episodes are typically ~same duration/size)
            by_num = defaultdict(list)
            for e in eps:
                by_num[e.get("episode", 0)].append(e)

            i = 0
            while i < len(ep_nums) - 1:
                e1, e2 = ep_nums[i], ep_nums[i + 1]
                if e2 - e1 == 1:  # consecutive
                    files1 = by_num[e1]
                    files2 = by_num[e2]
                    # Check if both have exactly 1 file and similar sizes
                    if len(files1) == 1 and len(files2) == 1:
                        s1 = files1[0].get("file_size", 0) or 0
                        s2 = files2[0].get("file_size", 0) or 0
                        if s1 > 0 and s2 > 0:
                            ratio = min(s1, s2) / max(s1, s2) if max(s1, s2) > 0 else 0
                            # If sizes within 30% of each other, likely a split
                            if ratio > 0.7:
                                p1 = files1[0].get("path", "")
                                p2 = files2[0].get("path", "")
                                already = part_re.search(p1) or part_re.search(p2)
                                # Only flag if not already in results from pattern 1
                                existing = [r for r in results[show] if r["season"] == season_num and r["episode"] == e1]
                                if not existing:
                                    results[show].append({
                                        "season": season_num, "episode": e1,
                                        "parts": [p1, p2], "already_named": bool(already),
                                        "consecutive_pair": True
                                    })
                                i += 2
                                continue
                i += 1

    return dict(results)


def rename_split_parts(paths, base_episode_name=None):
    """Rename files to Kodi-compatible .part1/.part2 format.
    paths: list of file paths (ordered by part number).
    Returns list of (old_path, new_path) tuples."""
    import re
    renames = []
    for i, path in enumerate(sorted(paths), 1):
        ext_match = re.search(r'\.\w+$', path)
        ext = ext_match.group() if ext_match else ".mkv"
        # Strip existing part/cd suffixes and episode numbering
        base = re.sub(r'[._\- ]?(part|pt|cd)\d+', '', path[:path.rfind('.')], flags=re.IGNORECASE)
        # If base_episode_name provided, use it
        if base_episode_name:
            directory = path.rsplit("/", 1)[0] if "/" in path else ""
            new_name = f"{base_episode_name}.part{i}{ext}"
            new_path = f"{directory}/{new_name}" if directory else new_name
        else:
            new_path = f"{base}.part{i}{ext}"
        if new_path != path:
            renames.append((path, new_path))
    return renames


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

# ── Simkl ─────────────────────────────────────────────────────────────
def simkl_headers(user):
    token = load_user_simkl_token(user)
    if not token: return None
    return {"Content-Type": "application/json", "Authorization": f"Bearer {token['access_token']}"}

def simkl_auth_url():
    return f"https://simkl.com/oauth/authorize?response_type=code&client_id={SIMKL_ID}&redirect_uri={urllib.parse.quote(SIMKL_REDIRECT)}"

def simkl_exchange_code(code):
    return api_post("https://api.simkl.com/oauth/token", {"code": code, "client_id": SIMKL_ID,
        "client_secret": SIMKL_SECRET, "redirect_uri": SIMKL_REDIRECT, "grant_type": "authorization_code"})

def simkl_fetch_ratings(user):
    """Pull all movie and show ratings from Simkl."""
    h = simkl_headers(user)
    if not h: return {}
    ratings = {}
    for kind in ("movies", "shows", "anime"):
        data = api_get(f"https://api.simkl.com/sync/ratings/{kind}/?client_id={SIMKL_ID}", h)
        if not data: continue
        for item in data:
            obj = item.get("movie") or item.get("show") or item.get("anime") or {}
            iid = obj.get("ids", {}).get("imdb", "")
            if iid: ratings[iid] = {"rating": item.get("rating", 0), "date": (item.get("rated_at") or "")[:10]}
    return ratings

def simkl_fetch_history(user):
    """Pull watch history from Simkl — returns [{id, title, watched_at, type}]."""
    h = simkl_headers(user)
    if not h: return []
    history = []
    for kind in ("movies", "shows", "anime"):
        data = api_get(f"https://api.simkl.com/sync/all-items/{kind}/completed?client_id={SIMKL_ID}", h)
        if not data or kind not in data: continue
        for item in data[kind]:
            obj = item
            iid = obj.get("ids", {}).get("imdb", "")
            if iid:
                history.append({"id": iid, "title": obj.get("title", ""),
                    "watched_at": (obj.get("last_watched_at") or "")[:10],
                    "type": "movie" if kind == "movies" else "show"})
    return history

def simkl_sync_push(user, ratings, titles):
    """Push user ratings to Simkl (bidirectional sync — push half)."""
    h = simkl_headers(user)
    if not h: return
    movies, shows = [], []
    for iid, r in ratings.items():
        t = titles.get(iid, {})
        entry = {"ids": {"imdb": iid}, "rating": r["rating"]}
        if t.get("type") in ("movie", "Movie"): movies.append(entry)
        else: shows.append(entry)
    if movies: api_post(f"https://api.simkl.com/sync/ratings?client_id={SIMKL_ID}", {"movies": movies}, h)
    if shows: api_post(f"https://api.simkl.com/sync/ratings?client_id={SIMKL_ID}", {"shows": shows}, h)

def _bg_simkl_sync(jid, user):
    titles = load_titles(); ratings = load_user_ratings(user)
    job_progress(jid, 0, 3, "Pushing to Simkl...")
    simkl_sync_push(user, ratings, titles)
    job_progress(jid, 1, 3, "Pulling from Simkl...")
    sr = simkl_fetch_ratings(user)
    for iid, r in sr.items():
        if iid not in ratings: ratings[iid] = r
        if iid not in titles: titles[iid] = {"title": "", "_enriched": False}
    save_user_ratings(user, ratings); save_titles(titles)
    job_progress(jid, 2, 3, "Fetching history...")
    history = simkl_fetch_history(user)
    if history:
        existing = load_user_history(user)
        seen = {h["id"] for h in existing}
        for h in history:
            if h["id"] not in seen: existing.append(h)
        save_user_history(user, existing)
    job_progress(jid, 3, 3, f"Done: {len(sr)} ratings, {len(history)} history items")

# ── AniList ────────────────────────────────────────────────────────────
def anilist_auth_url():
    return f"https://anilist.co/api/v2/oauth/authorize?client_id={ANILIST_ID}&redirect_uri={urllib.parse.quote(ANILIST_REDIRECT)}&response_type=code"

def anilist_exchange_code(code):
    return api_post("https://anilist.co/api/v2/oauth/token", {"grant_type": "authorization_code",
        "client_id": ANILIST_ID, "client_secret": ANILIST_SECRET,
        "redirect_uri": ANILIST_REDIRECT, "code": code})

def _anilist_gql(query, variables, token=None):
    """Execute an AniList GraphQL query."""
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    if token: headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request("https://graphql.anilist.co",
        data=json.dumps({"query": query, "variables": variables}).encode(), headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r: return json.loads(r.read())
    except Exception as e: print(f"AniList GQL error: {e}"); return None

def _anilist_get_user_id(token):
    """Get the authenticated user's AniList ID."""
    r = _anilist_gql("query { Viewer { id } }", {}, token)
    return r["data"]["Viewer"]["id"] if r and r.get("data") else None

def _mal_to_imdb(mal_id):
    """Map MAL ID → IMDB ID via TMDB's find endpoint."""
    if not TMDB_KEY or not mal_id: return None
    data = api_get(f"https://api.themoviedb.org/3/find/{mal_id}?api_key={TMDB_KEY}&external_source=mal_id")
    if not data: return None
    for kind in ("tv_results", "movie_results"):
        for item in (data.get(kind) or []):
            tmdb_id = item.get("id")
            media_type = "tv" if "tv" in kind else "movie"
            ext = api_get(f"https://api.themoviedb.org/3/{media_type}/{tmdb_id}/external_ids?api_key={TMDB_KEY}")
            if ext and ext.get("imdb_id"): return ext["imdb_id"]
    return None

def anilist_fetch_ratings(user):
    """Pull anime ratings from AniList → {imdb_id: {rating, date}}."""
    token_data = load_user_anilist_token(user)
    if not token_data: return {}
    token = token_data["access_token"]
    uid = _anilist_get_user_id(token)
    if not uid: return {}
    query = """query ($userId: Int!) {
      MediaListCollection(userId: $userId, type: ANIME) {
        lists { entries { score(format: POINT_10) updatedAt media { id idMal title { romaji english } } } }
      }
    }"""
    r = _anilist_gql(query, {"userId": uid}, token)
    if not r or not r.get("data"): return {}
    ratings = {}
    titles_db = load_titles()
    # Build reverse map: MAL ID → IMDB from existing titles
    mal_map = {}
    for iid, t in titles_db.items():
        if t.get("mal_id"): mal_map[t["mal_id"]] = iid
    for lst in r["data"]["MediaListCollection"]["lists"]:
        for entry in lst["entries"]:
            score = entry.get("score", 0)
            if not score: continue
            mal_id = entry["media"].get("idMal")
            if not mal_id: continue
            # Try cached mapping first, then TMDB lookup
            imdb_id = mal_map.get(mal_id)
            if not imdb_id:
                imdb_id = _mal_to_imdb(mal_id)
                time.sleep(0.3)
            if imdb_id:
                ratings[imdb_id] = {"rating": int(score), "date": time.strftime("%Y-%m-%d", time.gmtime(entry.get("updatedAt", 0)))}
                # Cache the MAL mapping
                if imdb_id in titles_db: titles_db[imdb_id]["mal_id"] = mal_id
    save_titles(titles_db)
    return ratings

def anilist_sync_push(user, ratings, titles):
    """Push anime ratings to AniList."""
    token_data = load_user_anilist_token(user)
    if not token_data: return
    token = token_data["access_token"]
    mutation = """mutation ($mediaId: Int, $score: Float, $status: MediaListStatus) {
      SaveMediaListEntry(mediaId: $mediaId, score: $score, status: $status) { id }
    }"""
    for iid, r in ratings.items():
        t = titles.get(iid, {})
        if not t.get("mal_id"): continue
        # Look up AniList media ID from MAL ID
        lookup = _anilist_gql("query ($malId: Int) { Media(idMal: $malId, type: ANIME) { id } }",
            {"malId": t["mal_id"]}, token)
        if not lookup or not lookup.get("data") or not lookup["data"].get("Media"): continue
        al_id = lookup["data"]["Media"]["id"]
        _anilist_gql(mutation, {"mediaId": al_id, "score": float(r["rating"]), "status": "COMPLETED"}, token)
        time.sleep(2.1)  # Stay under 30 req/min

def _bg_anilist_sync(jid, user):
    titles = load_titles(); ratings = load_user_ratings(user)
    job_progress(jid, 0, 3, "Pulling from AniList...")
    ar = anilist_fetch_ratings(user)
    for iid, r in ar.items():
        if iid not in ratings: ratings[iid] = r
        if iid not in titles: titles[iid] = {"title": "", "_enriched": False}
    save_user_ratings(user, ratings); save_titles(titles)
    job_progress(jid, 1, 3, "Pushing anime to AniList...")
    # Only push titles that have mal_id (anime)
    anime_ratings = {iid: r for iid, r in ratings.items() if titles.get(iid, {}).get("mal_id")}
    anilist_sync_push(user, anime_ratings, titles)
    job_progress(jid, 3, 3, f"Done: pulled {len(ar)}, pushed {len(anime_ratings)} anime ratings")

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
            if _is_tv(t):
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

def quality_score(t):
    """Quality score 0-100: how complete is this title's metadata?"""
    score = 0
    if t.get("poster"): score += 15
    if t.get("overview") or t.get("plot"): score += 10
    if t.get("genres"): score += 10
    if t.get("keywords") and len(t.get("keywords",[])) >= 3: score += 15
    if t.get("directors"): score += 10
    if t.get("cast"): score += 10
    if t.get("rotten_tomatoes"): score += 5
    if t.get("trailer"): score += 5
    if t.get("providers"): score += 10
    if t.get("alt_titles"): score += 5
    if t.get("similar"): score += 5
    return min(score, 100)

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
    changelog = safe_json_load(os.path.join(DATA_DIR, "enrichment_log.json")) or []
    for iid, t in todo:
        before = {k: t.get(k) for k in ("poster", "keywords", "cast", "directors", "writers", "genres", "rotten_tomatoes", "metacritic", "providers", "trailer", "similar_tmdb", "overview")}
        # Fill basics from IMDB dataset (free, no API call)
        ds = cache.get(iid, {})
        for k in ("title", "year", "type", "genres", "runtime", "imdb_rating", "votes"):
            if ds.get(k) and not t.get(k): t[k] = ds[k]
        t.pop("_enriched", None)
        if TMDB_KEY:
            for k, v in tmdb_enrich(iid).items():
                if v: t[k] = v
        if OMDB_KEY and omdb_calls < 80 and not fast:
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
        # Log what changed
        changes = {}
        for k, old_val in before.items():
            new_val = t.get(k)
            if new_val and new_val != old_val:
                if k == "keywords":
                    changes[k] = f"{len(old_val or [])} -> {len(new_val or [])}"
                elif k == "similar_tmdb":
                    changes[k] = f"{len(old_val or [])} -> {len(new_val or [])}"
                elif k == "cast":
                    changes[k] = str(new_val)[:60]
                else:
                    changes[k] = str(new_val)[:40]
        if changes:
            changelog.append({"iid": iid, "title": t.get("title", ""), "year": t.get("year", ""),
                "ts": time.strftime("%Y-%m-%dT%H:%M:%S"), "changes": changes})
        count += 1
        if jid and count % 5 == 0:
            job_progress(jid, count, total, f"Enriching {t.get('title',iid)}")
        if count % 50 == 0:
            save_titles(titles)
            print(f"  Enriched {count}/{total}...")
        time.sleep(0.03 if fast else 0.08)
    save_titles(titles)
    # Keep last 2000 changelog entries
    changelog = changelog[-2000:]
    safe_json_save(os.path.join(DATA_DIR, "enrichment_log.json"), changelog)
    print(f"Enriched {count} titles, {len([c for c in changelog if c['ts'] > time.strftime('%Y-%m-%d')])} changes today")

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

def reconcile_agent_data(user):
    """Flush agent_data (SQLite) into tmm_library.json. Call periodically."""
    library = load_user_tmm(user)
    if not library: return 0
    library = _merge_agent_data(library, user)
    save_user_tmm(user, library)
    return sum(1 for k in library if not k.startswith("_"))

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

def _apply_verification_result(task_result):
    """Store verification results in the DB. Auto-escalate suspicious ones to OCR."""
    db = get_db()
    db.execute("""CREATE TABLE IF NOT EXISTS verification (
        path TEXT, step TEXT, imdb_id TEXT,
        status TEXT, result TEXT, version INTEGER DEFAULT 1, ts TEXT, PRIMARY KEY (path, step))""")
    data = task_result.get("data", [])
    escalate = []
    for item in data:
        db.execute("""INSERT OR REPLACE INTO verification (path, imdb_id, step, status, result, version, ts)
            VALUES (?, ?, 'duration', ?, ?, ?, ?)""",
            (item["path"], item.get("imdb_id",""), item["status"],
             json.dumps(item), VERIFY_VERSION, time.strftime("%Y-%m-%dT%H:%M:%S")))
        # Auto-escalate: mismatches always, variants where file is SHORTER (not a DC)
        actual = item.get("actual_min", 0)
        expected = item.get("expected_min", 0)
        if item["status"] == "mismatch":
            escalate.append(item["path"])
        elif item["status"] == "possible_variant" and actual < expected:
            # File shorter than expected = suspicious (DC would be longer)
            escalate.append(item["path"])
    db.commit()
    # Queue OCR identification for suspicious files
    for path in escalate[:10]:  # Max 10 per batch to avoid overload
        db_enqueue_task("identify_movie", {"path": path}, 25)

