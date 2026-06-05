"""CineCross — Render: all HTML rendering functions."""
from data import *
from data import _load_key, _imdb_cache, _merge_agent_data
from logic import *
from logic import _is_tv

APP_BANNER = '<div style="background:var(--card);padding:6px 20px;border-bottom:1px solid var(--border);display:flex;align-items:center;gap:10px"><span style="font-size:1.2em">🎬</span><b style="font-size:1.1em;letter-spacing:.5px">Cinephile Crossroads</b></div>'

def nav_bar(active="ratings", user=""):
    u = user or (list_users() or ["default"])[0]
    sections = [("ratings", "⭐ Ratings", f"{BASE}/u/{u}"),
                ("discover", "🎯 Discover", f"{BASE}/recs/{u}"),
                ("library", "📚 Library", f"{BASE}/library/{u}"),
                ("social", "👥 Social", f"{BASE}/feed"),
                ("setup", "⚙ Setup", f"{BASE}/setup/{u}")]
    links = ""
    for key, label, href in sections:
        cls = "nav-active" if key == active else ""
        links += f'<a href="{href}" class="nav-link {cls}">{label}</a>'
    return APP_BANNER + f'<nav class="top-nav" role="navigation" aria-label="Main"><span class="hamburger" aria-label="Menu" role="button" onclick="toggleNav()">☰</span><div class="nav-links">{links}</div>{render_user_bar(u)}</nav>'

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

.mode-cockpit .page{max-width:none;padding:0 24px}
.mode-cockpit .widget-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(250px,1fr));gap:12px;margin-bottom:20px}
.mode-cockpit .widget{background:var(--card);border-radius:10px;padding:16px;border:1px solid var(--border,#333)}
.mode-cockpit .widget h4{font-size:.75em;color:var(--muted);text-transform:uppercase;letter-spacing:.05em;margin:0 0 8px}
.mode-cockpit .big-number{font-size:2em;font-weight:700}
.mode-spreadsheet table{font-size:.8em;width:100%}
.mode-spreadsheet td,.mode-spreadsheet th{padding:3px 6px;white-space:nowrap}
.mode-spreadsheet img{height:30px}
.mode-selector{display:flex;gap:4px;margin-left:8px}
.mode-selector a{padding:2px 8px;border-radius:4px;font-size:.75em;color:var(--muted);text-decoration:none;border:1px solid transparent}
.mode-selector a.active,.mode-selector a:hover{border-color:var(--accent,#4fc3f7);color:var(--accent,#4fc3f7)}
.poster-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(140px,1fr));gap:16px;padding:10px 0}
.poster-card{position:relative;border-radius:8px;overflow:hidden;background:var(--card);transition:transform .2s}
.poster-card:hover{transform:scale(1.03)}
.poster-card img{width:100%;aspect-ratio:2/3;object-fit:cover;display:block}
.poster-card .info{padding:8px;font-size:.8em}
.poster-card .title{font-weight:bold;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.poster-card .meta{color:var(--muted);font-size:.85em}
.poster-card .badge{position:absolute;top:6px;right:6px;background:rgba(0,0,0,.7);padding:2px 6px;border-radius:4px;font-size:.75em}
.poster-card .rating{position:absolute;top:6px;left:6px;background:rgba(0,0,0,.8);padding:2px 8px;border-radius:4px;font-weight:bold}
.poster-card .progress{position:absolute;bottom:0;left:0;height:3px;background:var(--accent);border-radius:0 0 8px 8px}
th{background:var(--card);cursor:pointer;position:sticky;top:88px;cursor:pointer}tr:hover{background:var(--card)}
a{color:var(--accent);text-decoration:none}img{border-radius:4px}
.btn{display:inline-block;padding:6px 14px;border-radius:6px;background:var(--card);border:1px solid var(--border);color:var(--fg);text-decoration:none;font-size:.85em;margin:2px}
.btn:hover{border-color:var(--accent);color:var(--accent)}.btn-primary{background:var(--accent);color:#1a1a2e;border-color:var(--accent)}
.x{font-size:.8em;color:var(--muted)}
@media(max-width:768px){.top-nav{flex-direction:column;gap:8px;padding:8px}.page{padding:10px}table{font-size:.8em}th,td{padding:4px 6px}img{height:50px!important}.x{display:none}.nav-links{display:none;flex-direction:column;width:100%}.nav-links.nav-open{display:flex}.hamburger{display:block;cursor:pointer;font-size:1.5em;padding:4px 8px}}
@media(min-width:769px){.hamburger{display:none}}
.bpp-legend{display:inline-flex;gap:8px;align-items:center;font-size:.75em;color:var(--muted);margin-left:12px}
.bpp-legend span{display:inline-flex;align-items:center;gap:2px}
"""

SHARED_JS = ('<script>'
    'if(localStorage.getItem("theme")==="light")document.body.classList.add("light");'
    'function sortTable(n){var tb=document.querySelector("tbody");if(!tb)return;var rows=[].slice.call(tb.rows),dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:"";rows.sort(function(a,b){var x=a.cells[n].dataset.sort||a.cells[n].textContent,y=b.cells[n].dataset.sort||b.cells[n].textContent;x=isNaN(x)?x:Number(x);y=isNaN(y)?y:Number(y);return(typeof x==="number"&&typeof y==="number"?(x-y):(String(x)).localeCompare(String(y),undefined,{numeric:true}))*dir});rows.forEach(function(r){tb.appendChild(r)})}'
    'function filterTable(){var q=(document.getElementById("s")||{}).value;q=q?q.toLowerCase():"";var rows=document.querySelectorAll("tbody tr");rows.forEach(function(r){r.style.display=r.textContent.toLowerCase().indexOf(q)>=0?"":"none"});syncFilterURL()}'
    # Keyboard: / to focus search, Escape to blur
    'document.addEventListener("keydown",function(e){if(e.key==="/"&&document.activeElement.tagName!=="INPUT"){e.preventDefault();var s=document.getElementById("s");if(s)s.focus()}if(e.key==="Escape"){document.activeElement.blur()}});'
    # URL filter sync: read on load, write on change
    'function syncFilterURL(){var s=document.getElementById("s");if(!s)return;var p=new URLSearchParams(location.search);if(s.value)p.set("q",s.value);else p.delete("q");var selects=document.querySelectorAll("select[name]");selects.forEach(function(sel){if(sel.value)p.set(sel.name,sel.value);else p.delete(sel.name)});history.replaceState(null,"",p.toString()?"?"+p.toString():location.pathname)}'
    'window.addEventListener("load",function(){var p=new URLSearchParams(location.search);var s=document.getElementById("s");if(s&&p.get("q")){s.value=p.get("q");filterTable()}var selects=document.querySelectorAll("select[name]");selects.forEach(function(sel){var v=p.get(sel.name);if(v){sel.value=v;sel.dispatchEvent(new Event("change"))}})});'
    # Hamburger menu toggle for mobile
    'function toggleNav(){var n=document.querySelector(".nav-links");if(n)n.classList.toggle("nav-open")}'
    '</script>')

def page_head(title, extra_css=""):
    return f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>{title}</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="theme-color" content="#1a1a2e">
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

def render_getting_started():
    """Landing page: onboarding guide for users with an imported library."""
    user = DEFAULT_USER
    titles = load_titles()
    library = load_user_tmm(user)
    ratings = load_user_ratings(user)
    db = get_db()

    # Stats
    all_entries = [e for k, v in library.items() if not k.startswith("_") for e in (v if isinstance(v, list) else [v]) if isinstance(e, dict)]
    total_files = len(all_entries)
    sized = sum(1 for e in all_entries if e.get("file_size"))
    hashed = sum(1 for e in all_entries if e.get("file_hash"))
    subs = sum(1 for e in all_entries if e.get("subtitles"))
    total_size = sum(e.get("file_size", 0) for e in all_entries)
    # Movies vs TV
    episodes = library.get("_episodes", {})
    tv_files = len(episodes)
    movie_files = total_files
    movie_titles = sum(1 for k in library if not k.startswith("_"))
    tv_shows = len(set(v.get("showtitle", "") for v in episodes.values() if isinstance(v, dict)))

    pending = db.execute("SELECT count(*) FROM task_queue WHERE status='pending'").fetchone()[0]
    done_total = db.execute("SELECT count(*) FROM task_queue WHERE status='done'").fetchone()[0]
    agent = load_agent_status()
    agent_seen = agent.get("last_seen", "never")
    agent_online = agent_seen != "never"

    html = page_head("Cinephile Crossroads") + nav_bar("home", user)
    html += '<div class="page" style="max-width:900px;margin:0 auto">'
    html += '<h1 style="margin-bottom:5px">🎬 Cinephile Crossroads</h1>'
    html += '<p style="color:var(--muted)">Your self-hosted media library is being automatically curated.</p>'

    # Quick summary — 3 key numbers
    html += '<div class="grid" style="grid-template-columns:repeat(3,1fr);gap:12px;margin:20px 0">'
    html += f'<div class="card" style="text-align:center"><div style="font-size:2em">{movie_titles:,}</div><small>Movies</small></div>'
    html += f'<div class="card" style="text-align:center"><div style="font-size:2em">{tv_shows}</div><small>TV Shows</small></div>'
    html += f'<div class="card" style="text-align:center"><div style="font-size:2em">{total_size/1099511627776:.1f} TB</div><small>Library</small></div>'
    html += '</div>'

    # Primary CTA — what to do next
    backlog_count = sum(1 for _ in [1] if pending > 0)  # count action items
    try:
        ocr_count = db.execute("SELECT count(*) FROM verification WHERE status IN ('review_needed','truncated')").fetchone()[0]
    except: ocr_count = 0
    action_count = ocr_count + (3 if find_mismatches else 0)  # approximate
    if action_count > 0:
        html += f'<a href="{BASE}/library/backlog/{user}" style="display:block;background:var(--accent);color:#1a1a2e;text-align:center;padding:14px;border-radius:8px;text-decoration:none;font-weight:600;margin:10px 0">📋 {action_count} items need your attention → Backlog</a>'
    elif pending > 0:
        html += f'<div class="card" style="text-align:center;padding:14px;border-left:4px solid var(--accent)">⏳ {pending} tasks running automatically. Nothing for you to do right now.</div>'
    else:
        html += '<div class="card" style="text-align:center;padding:14px;border-left:4px solid var(--accent2)">✅ Everything is up to date.</div>'

    # Recently watched but unrated — quick rate prompt
    history = load_user_history(user)
    if history:
        seen_ids = set()
        unrated_watched = []
        for e in sorted(history, key=lambda x: x.get("watched_at", ""), reverse=True):
            iid = e.get("id", "")
            if not iid or iid in ratings or iid in seen_ids: continue
            seen_ids.add(iid)
            unrated_watched.append(e)
            if len(unrated_watched) >= 5: break
        if unrated_watched:
            html += '<h3 style="margin-top:20px;font-size:1em">⭐ Recently watched — rate these?</h3>'
            html += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin:8px 0">'
            for e in unrated_watched:
                t = titles.get(e["id"], {})
                poster = t.get("poster", "")
                title_short = (e.get("title") or "?")[:25]
                html += f'<a href="{BASE}/title/{e["id"]}" style="text-decoration:none;background:var(--card);border:1px solid var(--border);border-radius:8px;padding:8px 12px;color:var(--fg);font-size:.85em;max-width:150px;text-align:center">'
                if poster:
                    html += f'<img src="{poster}" style="height:60px;border-radius:4px;display:block;margin:0 auto 4px"><br>'
                html += f'{title_short}</a>'
            html += '</div>'

    # Compact automation progress
    html += '<h3 style="margin-top:25px;font-size:1em;color:var(--muted)">Automation Progress</h3>'
    html += '<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:8px;margin:8px 0">'
    for label, pct in [("Sized", sized*100//total_files if total_files else 0), ("Hashed", hashed*100//total_files if total_files else 0), ("Subtitles", subs*100//total_files if total_files else 0)]:
        color = "var(--accent2)" if pct > 80 else "var(--accent)" if pct > 40 else "var(--warn)"
        html += f'<div style="font-size:.85em">{label} <b style="color:{color}">{pct}%</b></div>'
    html += '</div>'
    html += f'<div style="font-size:.8em;color:var(--muted);margin-top:4px">Agent {"🟢 online" if agent_online else "🔴 offline"} · {pending} tasks pending · {done_total} completed</div>'

    # Quick links (compact)
    html += '<h3 style="margin-top:25px;font-size:1em;color:var(--muted)">Quick Links</h3>'
    html += '<div style="display:flex;flex-wrap:wrap;gap:8px;margin:8px 0">'
    links = [
        ("📋 Backlog", f"{BASE}/library/backlog/{user}"),
        ("💡 Suggestions", f"{BASE}/library/suggestions/{user}"),
        ("⭐ Ratings", f"{BASE}/u/{user}"),
        ("🎯 Discover", f"{BASE}/recs/{user}"),
        ("📚 Library", f"{BASE}/library/{user}"),
        ("📺 TV Shows", f"{BASE}/tvshows/{user}"),
        ("⚙ Setup", f"{BASE}/setup/{user}"),
    ]
    for label, url in links:
        html += f'<a href="{url}" style="padding:6px 12px;background:var(--card);border:1px solid var(--border);border-radius:6px;color:var(--fg);text-decoration:none;font-size:.85em">{label}</a>'
    html += '</div>'

    html += '</div>'
    return html

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
            poster = '<img src="<img src="' + l.get("poster","") + '" height=40 loading="lazy">" height=40 loading="lazy">' if l.get("poster") else ""
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
    return page_head(f"{user}'s Ratings ({len(ratings)})") + nav_bar("ratings", user) + render_ratings_nav(user, "ratings") + f"""<div class="page">
{job_banner}
<h2>🎬 {user}'s Ratings — {len(ratings)} titles</h2>
<div class="bar"><input id="s" aria-label="Search titles" onkeyup="f()" placeholder="Search..." style="width:220px">
<select id="g" name="genre" aria-label="Filter by genre" onchange="f();syncFilterURL()"><option value="">All genres</option>{genre_opts}</select>
<select id="mr" name="rating" aria-label="Minimum rating" onchange="f();syncFilterURL()"><option value="">Min ★</option>{''.join(f'<option value="{i}">{i}+</option>' for i in range(10,0,-1))}</select>
<select id="dec" name="decade" aria-label="Filter by decade" onchange="f();syncFilterURL()"><option value="">All decades</option><option value="2020">2020s</option><option value="2010">2010s</option><option value="2000">2000s</option><option value="1990">1990s</option><option value="1980">1980s</option><option value="1970">1970s</option><option value="1960">1960s</option><option value="1950">1950s</option></select>
<select id="st" name="stream" aria-label="Filter by streaming" onchange="f();syncFilterURL()"><option value="">All streams</option>{"".join('<option value="' + p + '">' + PROVIDER_ICONS.get(p,"▪") + " " + p + '</option>' for p in sorted(user_provs))}</select>
<select id="vs" name="source" aria-label="Filter by source" onchange="f();syncFilterURL()"><option value="">All sources</option><option value="bluray">💿 Blu-ray</option><option value="dvd">📀 DVD</option><option value="webrip">🌐 Web</option><option value="remux">💎 Remux</option></select>
<button onclick="document.body.classList.toggle('light');localStorage.setItem('theme',document.body.classList.contains('light')?'light':'dark')" style="background:none;border:1px solid #444;border-radius:4px;cursor:pointer;padding:2px 8px;color:var(--fg)" title="Toggle dark/light theme">🌓</button></div>
<div style="margin-bottom:8px;font-size:.9em;color:var(--muted)">Mood: <a href="{BASE}/mood/{user}/light" title="Light">☀️</a> <a href="{BASE}/mood/{user}/intense" title="Intense">🔥</a> <a href="{BASE}/mood/{user}/funny" title="Funny">😂</a> <a href="{BASE}/mood/{user}/mind-bending" title="Mind-Bending">🌀</a> <a href="{BASE}/mood/{user}/dark" title="Dark">🌑</a> <a href="{BASE}/mood/{user}/epic" title="Epic">⚔️</a> <a href="{BASE}/mood/{user}/romantic" title="Romantic">💕</a> <a href="{BASE}/mood/{user}/scary" title="Scary">👻</a> <a href="{BASE}/mood/{user}/inspiring" title="Inspiring">✨</a></div>
<table><thead><tr><th></th><th onclick="sortTable(1)">Title</th><th onclick="sortTable(2)">Year</th><th onclick="sortTable(3)">★</th><th onclick="sortTable(4)">IMDB</th><th>Scores</th><th>Stream</th><th onclick="sortTable(7)">Genres</th><th onclick="sortTable(8)">Rated</th><th>💾</th></tr></thead>
<tbody>{rows}</tbody></table></div>""" + page_foot()

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
            poster_url = t.get("poster", "")
            poster_img = '<img src="' + poster_url + '" loading="lazy" alt="' + t.get("title","").replace('"','') + '">' if poster_url else '<div style="width:100%;aspect-ratio:2/3;background:var(--border);display:flex;align-items:center;justify-content:center;font-size:.7em;color:var(--muted)">No poster</div>'
            provs = " ".join(PROVIDER_ICONS.get(p,"") for p in t.get("providers",[]) if p in get_user_active_providers(user))
            imdb_r = str(t.get("imdb_rating",""))
            wl = '<a href="' + BASE + '/watchlist/add/' + iid + '" style="text-decoration:none">🤍</a>' if iid not in watchlist else '<a href="' + BASE + '/watchlist/rm/' + iid + '" style="text-decoration:none">❤️</a>'
            cards += '<a href="' + BASE + '/title/' + iid + '" style="text-decoration:none;color:var(--fg)"><div class="poster-card">'
            cards += poster_img
            cards += '<div class="rating" style="color:var(--accent)">' + str(score) + '</div>'
            if imdb_r: cards += '<div class="badge">' + imdb_r + '</div>'
            if provs: cards += '<div style="position:absolute;bottom:40px;right:4px;font-size:.9em">' + provs + '</div>'
            cards += '<div class="info"><div class="title">' + t.get("title","?") + '</div>'
            cards += '<div class="meta">' + str(t.get("year","")) + ' ' + wl + '</div></div></div></a>'
        columns += '<div><h4 style="margin:0 0 8px">' + cat_title + '</h4>'
        columns += '<p style="color:var(--muted);font-size:.8em;margin:0 0 12px">' + cat_desc + '</p><div class="poster-grid" style="grid-template-columns:repeat(auto-fill,minmax(120px,1fr))">' 
        columns += cards + '</div></div>'
    sections = '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:20px;align-items:start">' + columns + '</div>'
    
    user_bar = render_user_bar(user, "recs", False)
    html = page_head("Recommendations for " + user)
    html += nav_bar("discover", user)
    html += render_discover_nav(user, "recs")
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
    has_simkl = load_user_simkl_token(user) is not None
    user_bar = render_user_bar(user, "setup")
    media_servers = _render_media_servers(user)
    provider_config = _render_provider_config(user)
    trakt_section = '<span style="color:#2d7">✓ Connected</span> <a href="' + BASE + '/trakt/auth/' + user + '">(reconnect)</a>' if has_trakt else ('<a href="' + BASE + '/trakt/auth/' + user + '"><button>Connect Trakt</button></a>' if TRAKT_ID else '')
    simkl_section = '<span style="color:#2d7">✓ Connected</span> <a href="' + BASE + '/simkl/auth/' + user + '">(reconnect)</a>' if has_simkl else ('<a href="' + BASE + '/simkl/auth/' + user + '"><button>Connect Simkl</button></a>' if SIMKL_ID else '')
    
    # Build page with concatenation (avoids f-string issues with JS braces)
    html = page_head(f"Setup - {user}")
    html += nav_bar("setup", user)
    html += render_setup_nav(user, "setup")
    html += '<div class="page"><div style="max-width:600px;margin:0 auto"><style>input,textarea{background:var(--card)!important;color:var(--fg)!important;border-color:var(--border)!important}button{background:var(--accent);color:#fff;border:none;padding:10px 30px;border-radius:6px;cursor:pointer}</style>'
    html += '<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap">'
    html += '<h2>Setup — ' + user + '</h2>' + user_bar + '</div>'
    
    # Upload CSV
    html += '<h3>Upload IMDB CSV</h3>'
    html += '<form method="POST" action="' + BASE + '/upload/' + user + '" enctype="multipart/form-data">'
    html += '<input type="file" name="csv" accept=".csv"><button type="submit">Upload</button></form><hr>'
    
    # API Keys
    html += '<h3 style="margin-top:30px">API Keys</h3>'
    html += '<form method="POST" action="' + BASE + '/keys">'
    html += '<div style="display:grid;gap:14px;margin-bottom:20px">'
    html += '<div><label style="font-weight:bold;display:block;margin-bottom:4px">TMDB</label><input name="tmdb" value="' + TMDB_KEY + '"></div>'
    html += '<div><label style="font-weight:bold;display:block;margin-bottom:4px">OMDB</label><input name="omdb" value="' + OMDB_KEY + '"></div>'
    html += '<div><label style="font-weight:bold;display:block;margin-bottom:4px">TVDB</label><input name="tvdb" value="' + TVDB_KEY + '"></div>'
    html += '</div>'
    html += '<h4 style="margin-top:20px;margin-bottom:10px">OpenSubtitles.org</h4>'
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">'
    html += '<div><label style="display:block;margin-bottom:4px">Username</label><input name="opensubs_user" value="' + _load_key("opensubs_user") + '"></div>'
    html += '<div><label style="display:block;margin-bottom:4px">Password</label><input name="opensubs_pass" type="password" value="' + _load_key("opensubs_pass") + '"></div>'
    html += '</div>'
    html += '<h4 style="margin-top:16px;margin-bottom:10px">OpenSubtitles.com</h4>'
    html += '<label style="display:block;margin-bottom:4px">API key (<a href="https://www.opensubtitles.com/consumers" target="_blank">get key</a>)</label>'
    html += '<input name="opensubs" value="" + _load_key("opensubs") + "" placeholder="OpenSubtitles API key">'
    html += '<h4 style="margin-top:20px;margin-bottom:10px">Preferences</h4>'
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">'
    html += '<div><label style="display:block;margin-bottom:4px">Preferred subtitle language</label>'
    html += '<select name="sub_language" style="width:100%;padding:6px"><option value="">Select...</option>'
    cur_lang = _load_key("sub_language")
    for code, name in [("eng","English"),("ger","German"),("fre","French"),("spa","Spanish"),("ita","Italian"),("por","Portuguese"),("dut","Dutch"),("swe","Swedish"),("dan","Danish"),("fin","Finnish"),("nor","Norwegian"),("pol","Polish"),("cze","Czech"),("hun","Hungarian"),("rum","Romanian"),("tur","Turkish"),("ara","Arabic"),("chi","Chinese"),("jpn","Japanese"),("kor","Korean")]:
        sel = " selected" if cur_lang == code else ""
        html += '<option value="' + code + '"' + sel + '>' + name + '</option>'
    html += '</select></div>'
    html += '<div><label style="display:block;margin-bottom:4px">Preferred audio language</label>'
    cur_audio = _load_key("audio_language")
    html += '<select name="audio_language" style="width:100%;padding:6px"><option value="">Any</option>'
    for code, name in [("eng","English"),("ger","German"),("fre","French"),("spa","Spanish"),("ita","Italian"),("jpn","Japanese")]:
        sel = " selected" if cur_audio == code else ""
        html += '<option value="' + code + '"' + sel + '>' + name + '</option>'
    html += '</select></div></div>'
    html += '<h4 style="margin-top:20px;margin-bottom:10px">AI / LLM</h4>'
    html += '<div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:16px">'
    html += '<div><label style="display:block;margin-bottom:4px">LLM API URL</label>'
    html += '<input name="llm_url" value="' + _load_key("llm_url") + '" placeholder="http://intello:8000"></div>'
    html += '<div><label style="display:block;margin-bottom:4px">LLM Token (optional)</label>'
    html += '<input name="llm_token" value="' + _load_key("llm_token") + '" placeholder="Bearer token"></div>'
    html += '</div>'
    html += '<div><label style="display:block;margin-bottom:4px">Webhook URL (notifications)</label>'
    html += '<input name="webhook_url" value="' + _load_key("webhook_url") + '" placeholder="https://hooks.slack.com/... or Signal API"></div>'
    html += '</div>'
    html += '<h4 style="margin-top:20px;margin-bottom:10px">Incoming Folder</h4>'
    html += '<div><label style="display:block;margin-bottom:4px">Staging/download folders (one per line)</label>'
    staging = _load_key("staging_paths") or _load_key("incoming_path") or ""
    html += '<textarea name="staging_paths" rows="3" style="width:100%;background:var(--bg);color:var(--fg);border:1px solid var(--border);border-radius:4px;padding:8px;font-family:monospace;font-size:.85em" placeholder="/mnt/zeus/Movies/.downloads&#10;/mnt/buffer/completed">' + staging + '</textarea>'
    html += '<small style="color:var(--muted);display:block;margin-top:4px">Agent scans these folders for new files and upgrade candidates. One path per line.</small></div>'
    html += '<button type="submit">Save</button></form><hr>'

    # Trakt
    html += '<h3>Trakt</h3>' + trakt_section + '<hr>'
    
    # Simkl
    html += '<h3>Simkl</h3>' + simkl_section + '<hr>'
    
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
    
    html += '</div></div>' + page_foot()
    return html




# ── Library Organization ──────────────────────────────────────────────

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


def render_verification(user):
    """Verification results page — shows duration mismatches and identification results."""
    db = get_db()
    # Load IMDB editions index
    import os as _os
    _editions_path = _os.path.join(DATA_DIR, "imdb_editions.json")
    _imdb_editions = json.loads(open(_editions_path).read()) if _os.path.exists(_editions_path) else {}
    db.execute("""CREATE TABLE IF NOT EXISTS verification (
        path TEXT, step TEXT, imdb_id TEXT,
        status TEXT, result TEXT, version INTEGER DEFAULT 1, ts TEXT, PRIMARY KEY (path, step))""")
    titles = load_titles()
    
    # Get all verification results
    rows = db.execute("SELECT path, imdb_id, step, status, result, ts FROM verification ORDER BY status, ts DESC").fetchall()
    
    # Stats
    total = len(rows)
    mismatches = [r for r in rows if r[3] == "mismatch"]
    variants = [r for r in rows if r[3] == "possible_variant"]
    ok = [r for r in rows if r[3] == "ok"]
    identified = [r for r in rows if r[2] == "identify"]
    
    html = page_head(f"Verification - {user}")
    html += nav_bar("library", user)
    html += render_library_nav(user, "verify")
    html += '<div class="page">'
    html += '<h2>🔍 File Verification</h2>'
    html += f'<div class="grid"><div class="card"><b>Checked</b><br><span style="font-size:2em">{total}</span></div>'
    html += f'<div class="card"><b>OK</b><br><span style="font-size:2em;color:#2ecc71">{len(ok)}</span></div>'
    html += f'<div class="card"><b>Variants</b><br><span style="font-size:2em;color:#f39c12">{len(variants)}</span></div>'
    html += f'<div class="card" style="border:1px solid #d72"><b>Mismatches</b><br><span style="font-size:2em;color:#e74c3c">{len(mismatches)}</span></div>'
    html += f'<div class="card"><b>Identified (OCR)</b><br><span style="font-size:2em;color:#4fc3f7">{len(identified)}</span></div></div>'
    
    # Show mismatches first
    if mismatches:
        html += '<h3 style="color:#e74c3c">❌ Duration Mismatches (>15 min difference)</h3>'
        html += '<table><thead><tr><th>Title</th><th>Actual</th><th>Expected</th><th>Diff</th><th>Path</th><th>Action</th></tr></thead><tbody>'
        for path, iid, step, status, result_json, ts in mismatches:
            r = json.loads(result_json) if result_json else {}
            t = titles.get(iid, {})
            title = t.get("title", "?")
            year = t.get("year", "")
            html += f'<tr><td><a href="{BASE}/title/{iid}">{title}</a> ({year})</td>'
            html += f'<td>{r.get("actual_min",0):.0f} min</td><td>{r.get("expected_min",0)} min</td>'
            diff = r.get("actual_min", 0) - r.get("expected_min", 0)
            sign = "+" if diff > 0 else ""
            color = "#e74c3c" if diff < 0 else "#f39c12"
            # Detect edition from filename
            fname_lower = path.lower()
            edition_kws = {"director": "Director's Cut", "extended": "Extended", "unrated": "Unrated", "theatrical": "Theatrical", "imax": "IMAX", "ultimate": "Ultimate", "final.cut": "Final Cut", "redux": "Redux", "remastered": "Remastered", "special.edition": "Special Edition"}
            edition = next((v for k, v in edition_kws.items() if k in fname_lower), None)
            # Check if actual runtime matches a known alternate runtime from TMDB
            actual = r.get("actual_min", 0)
            alt_rts = t.get("alt_runtimes", [])
            if not edition and alt_rts and any(abs(actual - art) < 3 for art in alt_rts):
                edition = "confirmed alt version"
            # Check IMDB editions index
            if not edition and iid in _imdb_editions:
                edition = _imdb_editions[iid][0]  # first matching attribute
            if not edition and 5 < diff < 40:
                edition = "likely extended"
            label = f"{edition}" if edition else ("shorter" if diff < 0 else "longer")
            html += f'<td style="color:{color};font-weight:bold">{sign}{diff:.0f} <span style="font-weight:normal;font-size:.8em">({label})</span></td>'
            html += f'<td style="font-size:.75em;color:var(--muted)">{path.split("/")[-2] if "/" in path else path[-30:]}</td>'
            html += f'<td><a href="{BASE}/verify/{user}?identify={path}" class="btn">🔍 OCR</a></td></tr>'
        html += '</tbody></table>'
    
    # Show variants
    if variants:
        html += '<h3 style="color:#f39c12">🟡 Possible Variants (5-15 min difference)</h3>'
        html += '<table><thead><tr><th>Title</th><th>Actual</th><th>Expected</th><th>Diff</th><th>Note</th></tr></thead><tbody>'
        for path, iid, step, status, result_json, ts in variants[:20]:
            r = json.loads(result_json) if result_json else {}
            t = titles.get(iid, {})
            html += f'<tr><td><a href="{BASE}/title/{iid}">{t.get("title","?")}</a> ({t.get("year","")})</td>'
            html += f'<td>{r.get("actual_min",0):.0f}</td><td>{r.get("expected_min",0)}</td>'
            diff_v = r.get("actual_min", 0) - r.get("expected_min", 0)
            sign_v = "+" if diff_v > 0 else ""
            html += f'<td style="color:#f39c12">{sign_v}{diff_v:.0f}</td>'
            html += f'<td style="font-size:.8em;color:var(--muted)">Likely extended/director\'s cut</td></tr>'
        html += '</tbody></table>'
    
    # Show OCR identification results
    if identified:
        html += '<h3 style="color:#4fc3f7">🔬 OCR Identification Results</h3>'
        for path, iid, step, status, result_json, ts in identified[:10]:
            r = json.loads(result_json) if result_json else {}
            html += f'<div class="card" style="margin-bottom:10px"><b>{r.get("path","?").split("/")[-1][:50]}</b> ({r.get("duration_min",0):.0f} min)'
            if r.get("directors"): html += f'<br>Directors: {", ".join(r["directors"][:3])}'
            if r.get("names_found"): html += f'<br>Names: {", ".join(r["names_found"][:10])}'
            html += f'<br><small style="color:var(--muted)">{ts}</small></div>'
    
    if not rows:
        html += '<p style="color:var(--muted)">No files verified yet. The pipeline runs every 5 minutes and checks 20 files per cycle.</p>'
    
    html += '</div>'
    html += SHARED_JS + page_foot()
    return html


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


def render_ratings_nav(user, active="ratings"):
    return sub_nav([
        ("ratings", "📊 Ratings", f"{BASE}/u/{user}"),
        ("stats", "📈 Stats", f"{BASE}/stats/{user}"),
        ("unrated", "❓ Unrated", f"{BASE}/unrated/{user}"), ("history", "📜 History", f"{BASE}/history/{user}"),
    ], active)

def render_discover_nav(user, active="recs"):
    return sub_nav([
        ("recs", "🎯 Recommendations", f"{BASE}/recs/{user}"),
        ("ai", "🤖 AI Friend", f"{BASE}/ai-friend/{user}"),
        ("tonight", "🎲 Tonight", f"{BASE}/tonight/{user}"), ("random", "🎰 Random", f"{BASE}/random/{user}"),
        ("catalog", "📺 Catalog", f"{BASE}/catalog"),
        ("new", "🆕 New", f"{BASE}/new"),
        ("updates", "📋 Updates", f"{BASE}/updates"),
    ], active)

def render_social_nav(user, active="feed"):
    return sub_nav([
        ("feed", "📡 Feed", f"{BASE}/feed"),
        ("compare", "🤝 Compare", f"{BASE}/compare/"),
        ("alerts", "🔔 Alerts", f"{BASE}/alerts/{user}"),
        ("contribute", "🌍 Contribute", f"{BASE}/contribute/{user}"),
    ], active)

def render_setup_nav(user, active="setup"):
    return sub_nav([
        ("setup", "⚙ Config", f"{BASE}/setup/{user}"),
        ("trakt", "↕ Trakt", f"{BASE}/trakt/sync/{user}"),
        ("simkl", "↕ Simkl", f"{BASE}/simkl/sync/{user}"),
        ("export", "⬇ Export", f"{BASE}/export/{user}"),
        ("import", "📥 Import", f"{BASE}/import/streaming/{user}"), ("rss", "📡 RSS", f"{BASE}/rss/{user}"), ("wl-rss", "📋 Watchlist RSS", f"{BASE}/watchlist-rss/{user}"), ("efficiency", "📊 Efficiency", f"{BASE}/efficiency"),
    ], active)

def render_library_nav(user, active="library"):
    return sub_nav([
        ("library", "📚 Overview", f"{BASE}/library/{user}"),
        ("browse", "📖 Browse", f"{BASE}/library/browse/{user}"),
        ("tvshows", "📺 TV Shows", f"{BASE}/tvshows/{user}"),
        ("suggestions", "💡 Suggestions", f"{BASE}/library/suggestions/{user}"),
        ("backlog", "📋 Backlog", f"{BASE}/library/backlog/{user}"),
    ], active)


def render_backlog(user):
    """Human action items — things the system can't do alone."""
    db = get_db()
    library = load_user_tmm(user)
    titles = load_titles()
    html = page_head("Backlog") + nav_bar("library", user) + render_library_nav(user, "backlog")
    html += '<h2>📋 Backlog — Human Actions Needed</h2>'
    html += '<p style="color:var(--muted)">Items requiring your attention. The system handles everything else automatically.</p>'

    # Radarr/Sonarr status widget
    import urllib.request as _ur
    arr_html = '<div class="grid" style="grid-template-columns:1fr 1fr;gap:10px;margin:15px 0">'
    try:
        rd = json.loads(_ur.urlopen("http://beirao:7878/api/v3/queue?apikey=a058ec8d36f04aafb197d2f49c15a327", timeout=3).read())
        rm = json.loads(_ur.urlopen("http://beirao:7878/api/v3/movie?apikey=a058ec8d36f04aafb197d2f49c15a327", timeout=3).read())
        rd_q = rd.get("totalRecords", 0)
        rd_monitored = sum(1 for m in rm if m.get("monitored") and not m.get("hasFile"))
        arr_html += f'<div class="card"><b>🎬 Radarr</b><br><span style="font-size:.85em">{rd_q} downloading · {rd_monitored} wanted</span></div>'
    except:
        arr_html += '<div class="card"><b>🎬 Radarr</b><br><span style="font-size:.85em;color:var(--muted)">unreachable</span></div>'
    try:
        sd = json.loads(_ur.urlopen("http://beirao:8989/api/v3/queue?apikey=5dd23f064c10436e856a68d6bf4e586a", timeout=3).read())
        sw = json.loads(_ur.urlopen("http://beirao:8989/api/v3/wanted/missing?apikey=5dd23f064c10436e856a68d6bf4e586a&pageSize=1", timeout=3).read())
        sd_q = sd.get("totalRecords", 0)
        sd_wanted = sw.get("totalRecords", 0)
        arr_html += f'<div class="card"><b>📺 Sonarr</b><br><span style="font-size:.85em">{sd_q} downloading · {sd_wanted} missing eps</span></div>'
    except:
        arr_html += '<div class="card"><b>📺 Sonarr</b><br><span style="font-size:.85em;color:var(--muted)">unreachable</span></div>'
    arr_html += '</div>'
    html += arr_html

    items = []

    # 1. Radarr/Sonarr indexer setup
    try:
        import urllib.request
        r = urllib.request.urlopen("http://beirao:7878/api/v3/indexer?apikey=a058ec8d36f04aafb197d2f49c15a327", timeout=3)
        radarr_indexers = len(json.loads(r.read()))
    except:
        radarr_indexers = -1
    try:
        r = urllib.request.urlopen("http://beirao:8989/api/v3/indexer?apikey=5dd23f064c10436e856a68d6bf4e586a", timeout=3)
        sonarr_indexers = len(json.loads(r.read()))
    except:
        sonarr_indexers = -1
    if radarr_indexers == 0:
        items.append(("🔴", "Configure Radarr indexers", "Radarr has no indexers — it can't search for movies without them.",
            "http://beirao.local:7878/settings/indexers", "Open Radarr Settings"))
    if sonarr_indexers == 0:
        items.append(("🔴", "Configure Sonarr indexers", "Sonarr has no indexers — it can't search for TV shows without them.",
            "http://beirao.local:8989/settings/indexers", "Open Sonarr Settings"))

    # 2. OCR results needing human review
    try:
        ocr_reviews = db.execute("SELECT path, result, status FROM verification WHERE status IN ('review_needed', 'truncated') ORDER BY ts DESC").fetchall()
    except:
        ocr_reviews = []
    if ocr_reviews:
        items.append(("🟡", f"Review {len(ocr_reviews)} OCR identifications",
            "Movies where OCR couldn't confidently match — may be truncated files or wrong matches.",
            f"{BASE}/verify/{user}", "Open Verify Page"))

    # 3. Merge dry-runs awaiting approval
    merge_dryrun = db.execute("SELECT params, result FROM task_queue WHERE type='merge_audio' AND status='done' AND result LIKE '%dry_run%'").fetchall()
    if merge_dryrun:
        items.append(("🟡", f"Approve {len(merge_dryrun)} audio merge candidates",
            "Files where a better audio track was found. Review and approve to merge.",
            f"{BASE}/library/{user}", "Review Merges"))

    # 4. Upgrade requests without indexers
    upgrades = db.execute("SELECT imdb_id, value FROM agent_data WHERE field='upgrade_wanted' AND user=?", (user,)).fetchall()
    if upgrades and (radarr_indexers == 0 or sonarr_indexers == 0):
        items.append(("🟡", f"{len(upgrades)} upgrade requests waiting",
            "You've flagged titles for upgrade but indexers aren't configured yet.",
            f"{BASE}/library/suggestions/{user}", "View Suggestions"))

    # 5. Title mismatches needing confirmation
    try:
        from app import find_mismatches
        mismatches = find_mismatches(user)
    except:
        mismatches = []
    if mismatches:
        items.append(("🟡", f"Confirm {len(mismatches)} title mismatches",
            "Filenames that don't match their IMDB title — may be wrong matches or foreign titles.",
            f"{BASE}/confirm/{user}", "Open Confirm Page"))

    # 6. Pending automation status
    pending = dict(db.execute("SELECT type, count(*) FROM task_queue WHERE status='pending' GROUP BY type").fetchall())
    total_pending = sum(pending.values())
    if total_pending > 0:
        items.append(("🔵", f"{total_pending} automated tasks running",
            " · ".join(f"{c}× {t}" for t, c in sorted(pending.items(), key=lambda x: -x[1])),
            f"{BASE}/library/{user}", "View Dashboard"))

    # 7. Subs coverage
    all_entries = [e for k, v in library.items() if not k.startswith("_") for e in (v if isinstance(v, list) else [v]) if isinstance(e, dict)]
    total_files = len(all_entries)
    with_subs = sum(1 for e in all_entries if e.get("subtitles"))
    if with_subs < total_files * 0.5:
        items.append(("🔵", f"Subtitle coverage: {with_subs*100//total_files}%",
            f"{total_files - with_subs} files still need subtitles. Batches of 50 are queued automatically every 6h.",
            None, None))

    # Render
    if not items:
        html += '<div style="text-align:center;padding:40px"><div style="font-size:3em">✅</div><p>Nothing to do! The system is running smoothly.</p></div>'
    else:
        for priority, title, desc, link, link_text in items:
            html += f'<div class="card" style="margin-bottom:12px;padding:16px;border-left:4px solid {"#e74c3c" if priority=="🔴" else "#f39c12" if priority=="🟡" else "#4fc3f7"}">'
            html += f'<div style="display:flex;justify-content:space-between;align-items:center">'
            html += f'<div><b>{priority} {title}</b><br><span style="color:var(--muted);font-size:.85em">{desc}</span></div>'
            if link:
                html += f'<a href="{link}" class="btn" style="background:var(--accent);color:#1a1a2e;padding:6px 14px;border-radius:6px;text-decoration:none;font-size:.85em;white-space:nowrap">{link_text}</a>'
            html += '</div></div>'

    # Recheck button
    html += f'<div style="margin-top:20px;text-align:center"><a href="{BASE}/library/backlog/{user}" style="color:var(--accent);text-decoration:none">🔄 Recheck Status</a></div>'
    return html

def render_suggestions(user):
    """Quality suggestions: upgrade starved files, transcode bloated ones, reorganize TV."""
    library = _merge_agent_data(load_user_tmm(user), user)
    titles = load_titles()
    html = page_head("Suggestions") + nav_bar("library", user) + render_library_nav(user, "suggestions")
    html += '<h2>💡 Library Suggestions</h2>'

    starved, bloated, tv_misplaced = [], [], []
    for iid, val in library.items():
        if iid.startswith("_"): continue
        t = titles.get(iid, {})
        try:
            runtime = int(t.get("runtime", 0) or 0)
        except (ValueError, TypeError):
            runtime = 0
        if not runtime or runtime < 30: continue
        entries = val if isinstance(val, list) else [val] if isinstance(val, dict) else []
        for e in entries:
            if not isinstance(e, dict): continue
            sz = e.get("file_size", 0)
            if not sz: continue
            bitrate = (sz * 8) / (runtime * 60) / 1_000_000
            w = e.get("video_width", 0) or 0
            path = e.get("path", "")
            # Starved: < 1.5 Mbps and supposed to be HD
            if bitrate < 1.5 and sz > 100_000_000:
                starved.append((iid, t, e, bitrate, sz))
            # Bloated: > 15 Mbps (remux/raw that could be re-encoded)
            elif bitrate > 15 and sz > 5_000_000_000:
                bloated.append((iid, t, e, bitrate, sz))
            # TV in Movies folder
            if t.get("type") in ("tvSeries", "tvMiniSeries") and "Movies" in path:
                tv_misplaced.append((iid, t, e))

    # Sort: starved by bitrate (worst first), bloated by size (biggest savings first)
    starved.sort(key=lambda x: x[3])
    bloated.sort(key=lambda x: -x[4])
    # Deduplicate: one row per IMDB ID (keep worst/biggest)
    seen_s, deduped_s = set(), []
    for item in starved:
        if item[0] not in seen_s: seen_s.add(item[0]); deduped_s.append(item)
    starved = deduped_s
    seen_b, deduped_b = set(), []
    for item in bloated:
        if item[0] not in seen_b: seen_b.add(item[0]); deduped_b.append(item)
    bloated = deduped_b

    # === STARVED FILES ===
    html += f'<h3 style="margin-top:20px">⚠️ Upgrade Candidates — Starved Quality ({len(starved)})</h3>'
    html += '<p style="color:var(--muted);font-size:.85em">Low bitrate — likely old XviD/DivX rips needing HD replacement.</p>'
    if starved:
        limit_s = len(starved) if "all" in str(locals().get("qs", "")) else 20
        html += '<table><thead><tr><th>Title</th><th>Year</th><th>Size</th><th>Bitrate</th><th>Res</th><th></th></tr></thead><tbody>'
        for iid, t, e, bitrate, sz in starved[:limit_s]:
            sz_str = f"{sz/1073741824:.1f} GB" if sz > 1073741824 else f"{sz/1048576:.0f} MB"
            h = e.get("video_height", 0) or 0
            res = f"{h}p" if h else "?"
            html += f'<tr><td><a href="{BASE}/title/{iid}">{t.get("title","?")}</a></td>'
            html += f'<td>{t.get("year","")}</td><td>{sz_str}</td>'
            html += f'<td style="color:#e74c3c;font-weight:bold">{bitrate:.1f}</td><td>{res}</td>'
            html += f'<td><a href="{BASE}/library/suggestions/{user}?action=flag_upgrade&iid={iid}" style="color:var(--accent)">🔍</a></td></tr>'
        html += '</tbody></table>'
        if len(starved) > 20 and limit_s == 20:
            html += f'<p><a href="{BASE}/library/suggestions/{user}?show=all" style="color:var(--accent);font-size:.85em">Show all {len(starved)} →</a></p>'
    else:
        html += '<p style="color:var(--accent2)">✅ No starved files found.</p>'

    # === BLOATED FILES ===
    html += f'<h3 style="margin-top:30px">🐘 Transcode Candidates — Bloated ({len(bloated)})</h3>'
    html += '<p style="color:var(--muted);font-size:.85em">High-bitrate files (remuxes, raw Blu-ray) that could be re-encoded to HEVC with minimal quality loss, saving significant disk space.</p>'
    if bloated:
        html += '<table><thead><tr><th>Title</th><th>Year</th><th>Size</th><th>Bitrate</th><th>Potential Savings</th><th>Action</th></tr></thead><tbody>'
        for iid, t, e, bitrate, sz in bloated[:50]:
            sz_str = f"{sz/1073741824:.1f} GB"
            # Estimate: re-encode to ~8 Mbps target
            target_sz = (8_000_000 * int(t.get("runtime", 90) or 90) * 60) / 8
            savings = sz - target_sz
            savings_str = f"{savings/1073741824:.1f} GB" if savings > 0 else "—"
            html += f'<tr><td><a href="{BASE}/title/{iid}">{t.get("title","?")}</a></td>'
            html += f'<td>{t.get("year","")}</td><td>{sz_str}</td>'
            html += f'<td style="color:#f39c12;font-weight:bold">{bitrate:.1f} Mbps</td>'
            html += f'<td style="color:var(--accent2)">{savings_str}</td>'
            html += f'<td><a href="{BASE}/library/suggestions/{user}?action=transcode&iid={iid}" style="color:var(--accent)">🔄 Transcode</a></td></tr>'
        html += '</tbody></table>'
    else:
        html += '<p style="color:var(--accent2)">✅ No bloated files found.</p>'

    # === TV QUALITY INCONSISTENCY ===
    episodes = library.get("_episodes", {})
    shows_by_name = {}
    for k, v in episodes.items():
        if not isinstance(v, dict): continue
        show = v.get("showtitle", k.split("|")[0])
        shows_by_name.setdefault(show, []).append(v)
    inconsistent_shows = []
    for show, ep_list in shows_by_name.items():
        heights = [e.get("video_height", 0) for e in ep_list if e.get("video_height")]
        if len(heights) < 2: continue
        max_h, min_h = max(heights), min(heights)
        if max_h >= 720 and min_h < max_h * 0.7:
            bad_eps = [e for e in ep_list if (e.get("video_height") or 0) < max_h * 0.7]
            inconsistent_shows.append((show, len(ep_list), len(bad_eps), max_h, min_h, bad_eps))
    inconsistent_shows.sort(key=lambda x: -x[2])

    html += f'<h3 style="margin-top:30px">📉 TV Quality Inconsistency ({len(inconsistent_shows)} shows)</h3>'
    html += '<p style="color:var(--muted);font-size:.85em">Shows where some episodes are significantly lower quality than the rest. Upgrade candidates for consistent viewing.</p>'
    if inconsistent_shows:
        html += '<table><thead><tr><th>Show</th><th>Episodes</th><th>Low Quality</th><th>Best</th><th>Worst</th><th>Upgrade</th></tr></thead><tbody>'
        for show, total, bad_count, max_h, min_h, bad_eps in inconsistent_shows[:30]:
            pct = bad_count * 100 // total
            color = "#e74c3c" if pct > 50 else "#f39c12" if pct > 20 else "#f1c40f"
            html += f'<tr><td><b>{show}</b></td><td>{total}</td>'
            html += f'<td style="color:{color};font-weight:bold">{bad_count} ({pct}%)</td>'
            html += f'<td>{max_h}p</td><td style="color:#e74c3c">{min_h}p</td>'
            html += f'<td><a href="{BASE}/library/suggestions/{user}?action=flag_upgrade&show={urllib.parse.quote(show)}" style="color:var(--accent)">🔍 Find {max_h}p</a></td></tr>'
        html += '</tbody></table>'
    else:
        html += '<p style="color:var(--accent2)">✅ All shows have consistent quality.</p>'

    # === STAGING UPGRADES ===
    from logic import scan_staging_upgrades
    staging_upgrades = scan_staging_upgrades(user)
    html += f'<h3 style="margin-top:30px">📥 Upgrade Candidates from Staging ({len(staging_upgrades)})</h3>'
    html += '<p style="color:var(--muted);font-size:.85em">Better-quality files found in your staging/download folders that can replace existing library copies.</p>'
    if staging_upgrades:
        html += '<table><thead><tr><th>Title</th><th>Staged Quality</th><th>Current</th><th>Action</th></tr></thead><tbody>'
        for u in staging_upgrades[:30]:
            sq = u["staged_quality"]
            eq = u["existing_quality"]
            html += f'<tr><td><b>{u["title"]}</b><br><span style="font-size:.75em;color:var(--muted)">{u["staged_file"][:50]}</span></td>'
            html += f'<td style="color:var(--accent2)">{sq.get("resolution",0)}p (score {sq.get("score",0)})</td>'
            html += f'<td style="color:var(--warn)">{eq.get("resolution",0)}p</td>'
            html += f'<td><a href="{BASE}/library/suggestions/{user}?action=replace&path={urllib.parse.quote(u["staged_path"])}&target={urllib.parse.quote(u["existing_path"])}" style="color:var(--accent)">✅ Replace</a></td></tr>'
        html += '</tbody></table>'
    else:
        html += '<p style="color:var(--accent2)">✅ No upgrades waiting in staging folders.</p>'

    # === TV IN WRONG FOLDER ===
    html += f'<h3 style="margin-top:30px">📺 TV Shows in Movies Folder ({len(tv_misplaced)})</h3>'
    html += '<p style="color:var(--muted);font-size:.85em">TV series/miniseries filed under Movies. Should be moved to TVShows for proper media server indexing.</p>'
    if tv_misplaced:
        html += '<table><thead><tr><th>Title</th><th>Year</th><th>Type</th><th>Current Path</th></tr></thead><tbody>'
        for iid, t, e in tv_misplaced[:30]:
            path_short = e.get("path", "")[-60:]
            html += f'<tr><td><a href="{BASE}/title/{iid}">{t.get("title","?")}</a></td>'
            html += f'<td>{t.get("year","")}</td><td>{t.get("type","")}</td>'
            html += f'<td style="font-size:.8em;color:var(--muted)">...{path_short}</td></tr>'
        html += '</tbody></table>'
    else:
        html += '<p style="color:var(--accent2)">✅ All TV shows properly organized.</p>'

    return html

def render_library(user):
    """Library page with sub-navigation."""
    """Library curation: duplicates, quality comparison, cleanup suggestions."""
    library = _merge_agent_data(load_user_tmm(user), user)
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
            if not path or info.get("confirmed"): continue
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
    dupe_cards = '<form id="batch-delete" method="POST" action="' + BASE + '/library/' + user + '/batch-delete"><input type="hidden" name="confirm" value="yes_delete">'
    dupe_cards += '<div style="margin-bottom:12px"><button type="submit" class="btn btn-primary" onclick="return confirm(\'Delete \'+document.querySelectorAll(\\\'input[name=paths]:checked\\\').length+\\\' selected files?\\\')">🗑 Delete Selected</button> <span style="color:var(--muted);font-size:.85em">Check files to remove, then click delete</span></div>'
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
            thumb_html = '<img src="' + BASE + '/thumbnails/' + thumb + '" style="width:100%;border-radius:4px;margin-bottom:8px">' if thumb else '<div style="background:#333;height:80px;border-radius:4px;margin-bottom:8px;display:flex;align-items:center;justify-content:center;color:#666">no preview</div>'

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
            dupe_cards += '<div style="font-size:.7em;color:#666">' + iid + '</div>'
            dupe_cards += '<div style="font-size:1.2em;font-weight:bold;margin-bottom:4px">' + str(h) + 'p ' + codec_display + '</div>'
            dupe_cards += '<div style="font-size:.9em;color:#aaa">Size: ' + size_str + '</div>'
            dupe_cards += '<div style="font-size:.85em;color:#888;margin:4px 0">Audio: ' + audio_str + '</div>'
            dupe_cards += '<div style="font-size:.85em;color:#888">Subs: ' + sub_str + '</div>'
            dupe_cards += badge
            dupe_cards += '<div style="margin-top:6px">' + open_btn + '</div>'
            dupe_cards += '<label style="display:flex;align-items:center;gap:4px;margin-top:6px;cursor:pointer"><input type="checkbox" name="paths" value="' + path + '"> <span style="font-size:.75em;color:#f90">Select for deletion</span></label>'
            dupe_cards += '<div style="font-size:.75em;color:#8ab;margin-top:4px;word-break:break-all;font-family:monospace">' + path.split("/")[-2] + '/' + path.split("/")[-1] + '</div>'
            dupe_cards += '</div>'

        dupe_cards += '</div></div>'
    dupe_cards += '</form>'

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
    html += '<div class="bpp-legend"><span style="color:#e74c3c">● Starved</span><span style="color:#f39c12">● Low</span><span style="color:#f1c40f">● OK</span><span style="color:#2ecc71">● Good</span> <span style="color:var(--muted)">(quality dot = bits-per-pixel vs resolution)</span></div>'

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
    # Agent health check
    agent_last = agent.get("last_seen", "")
    agent_stale = False
    if agent_last:
        try:
            from datetime import datetime
            last_dt = datetime.strptime(agent_last, "%Y-%m-%d %H:%M:%S")
            agent_stale = (datetime.now() - last_dt).total_seconds() > 1800
        except: pass
    if agent_stale:
        html += '<div style="background:#d72;color:#fff;padding:8px 16px;border-radius:6px;margin-bottom:12px">⚠ Agent disconnected — last seen ' + agent_last + '</div>'
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

    # Background activity
    from datetime import datetime
    elog = safe_json_load(os.path.join(DATA_DIR, "enrichment_log.json")) or []
    recent_enriched = sum(1 for e in elog if e.get("ts","") > time.strftime("%Y-%m-%dT%H:", time.localtime(time.time()-3600)))
    titles_obj = load_titles()
    alt_done = sum(1 for v in titles_obj.values() if v.get("alt_titles"))
    alt_total = sum(1 for v in titles_obj.values() if v.get("tmdb_id"))
    alt_pct = alt_done * 100 // alt_total if alt_total else 0

    html += '<div class="grid" style="margin-bottom:20px">'
    html += '<div class="card"><b>🔄 Background Activity</b><br>'
    html += '<small>Enrichment: ' + str(recent_enriched) + ' changes last hour</small><br>'
    html += '<small>Alt titles: ' + str(alt_done) + '/' + str(alt_total) + ' (' + str(alt_pct) + '%)</small>'
    html += '<div style="background:#333;border-radius:3px;height:6px;margin-top:4px"><div style="background:#48f;height:6px;width:' + str(alt_pct) + '%;border-radius:3px"></div></div>'
    html += '<small style="color:var(--muted)">Pulling 10 every 5s from TMDB</small><br>'
    if elog:
        html += '<small style="color:var(--muted)">Last enrichment: ' + elog[-1].get("ts","")[:16] + '</small>'
    html += '</div>'
    # Recent enrichment changes
    html += '<div class="card"><b>📋 Recent Changes</b><br>'
    for e in reversed(elog[-5:]):
        changes = ", ".join(e.get("changes",{}).keys())
        html += '<small>' + e.get("title","")[:25] + ': ' + changes + '</small><br>'
    if not elog: html += '<small style="color:var(--muted)">No changes yet</small>'
    html += '<br><a href="' + BASE + '/updates" style="font-size:.8em">View all →</a></div>'
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
    title_to_iid = {t.get("title", "").lower(): iid for iid, t in titles.items() if _is_tv(t)}
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
    genre_count, director_count, decade_count, lang_count = {}, {}, {}, {}
    monthly = {}  # year-month -> count
    yearly_genre = {}  # year -> {genre: count}
    for iid, r in ratings.items():
        t = titles.get(iid, {})
        # Monthly tracking
        d = r.get("date", "")
        if d and len(d) >= 7:
            ym = d[:7]
            monthly[ym] = monthly.get(ym, 0) + 1
            yr = d[:4]
            yearly_genre.setdefault(yr, {})
        # Decade
        y = str(t.get("year", ""))[:4]
        if y.isdigit():
            dec = y[:3] + "0s"
            decade_count[dec] = decade_count.get(dec, 0) + 1
        for g in (t.get("genres") or "").split(","):
            g = g.strip()
            if g:
                genre_count[g] = genre_count.get(g, 0) + 1
                if d and len(d) >= 4: yearly_genre.setdefault(d[:4], {}).setdefault(g, 0); yearly_genre[d[:4]][g] += 1
        for dd in (t.get("directors") or []) if isinstance(t.get("directors"), list) else (t.get("directors") or "").split(","):
            dd = dd.strip()
            if dd: director_count[dd] = director_count.get(dd, 0) + 1
        # Language from original title presence
        ot = t.get("originalTitle") or t.get("original_title") or ""
        if ot and ot != t.get("title", ""):
            lang_count["Foreign"] = lang_count.get("Foreign", 0) + 1
        else:
            lang_count["English"] = lang_count.get("English", 0) + 1
    top_genres = sorted(genre_count.items(), key=lambda x: x[1], reverse=True)[:12]
    top_dirs = sorted(director_count.items(), key=lambda x: x[1], reverse=True)[:10]
    top_decades = sorted(decade_count.items())
    rating_dist = [sum(1 for s in scores if s == i) for i in range(1, 11)]
    max_bar = max(rating_dist) or 1
    # Rating streak
    dates = sorted(set(r.get("date","")[:10] for r in ratings.values() if r.get("date")))
    streak = 0
    if dates:
        from datetime import datetime, timedelta
        today = datetime.now().date()
        d = today
        for _ in range(365):
            if d.isoformat() in dates or (d - timedelta(days=1)).isoformat() in dates:
                streak += 1
                d -= timedelta(days=1)
            else:
                break
    dist_bars = "".join('<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:30px;text-align:right">' + str(i) + '</span><div style="background:#4fc3f7;height:18px;width:' + str(rating_dist[i-1]/max_bar*300) + 'px;border-radius:3px"></div><span style="color:#888;font-size:.85em">' + str(rating_dist[i-1]) + '</span></div>' for i in range(10, 0, -1))
    genre_bars = "".join('<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:100px;text-align:right;font-size:.85em">' + esc(g) + '</span><div style="background:#4fc3f7;height:16px;width:' + str(c/top_genres[0][1]*250) + 'px;border-radius:3px"></div><span style="color:#888;font-size:.85em">' + str(c) + '</span></div>' for g, c in top_genres)
    dir_list = "".join("<tr><td>" + esc(d) + "</td><td>" + str(c) + "</td></tr>" for d, c in top_dirs)
    decade_bars = "".join('<div style="display:flex;align-items:center;gap:8px;margin:2px 0"><span style="width:50px;text-align:right;font-size:.85em">' + d + '</span><div style="background:#f7a04f;height:16px;width:' + str(c/max(1,max(x[1] for x in top_decades))*250) + 'px;border-radius:3px"></div><span style="color:#888;font-size:.85em">' + str(c) + '</span></div>' for d, c in top_decades)
    # Monthly chart (last 24 months)
    sorted_months = sorted(monthly.items())[-24:]
    max_m = max((v for _, v in sorted_months), default=1)
    month_bars = "".join('<div style="display:flex;align-items:end;gap:0"><div style="background:#4fc3f7;width:20px;height:' + str(max(2, v/max_m*80)) + 'px;border-radius:2px 2px 0 0" title="' + m + ': ' + str(v) + '"></div></div>' for m, v in sorted_months)
    month_labels = '<div style="display:flex;gap:0;font-size:.6em;color:#888">' + "".join('<div style="width:20px;text-align:center;overflow:hidden">' + m[5:7] + '</div>' for m, _ in sorted_months) + '</div>'
    html = page_head(f"Stats - {user}")
    html += nav_bar("ratings", user)
    html += render_ratings_nav(user, "stats")
    html += '<div class="page">'
    # Taste personality
    personality = ""
    if _load_key("llm_url"):
        personality = taste_personality(user)
    if personality:
        html += '<div class="card" style="padding:12px;margin-bottom:16px;border-left:4px solid var(--accent,#4fc3f7);font-style:italic;line-height:1.5">' + esc(personality) + '</div>'
    html += '<h2>📊 ' + esc(user) + " Stats</h2>"
    # Summary cards
    html += '<div style="display:flex;gap:15px;margin-bottom:20px;flex-wrap:wrap">'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em">' + str(len(ratings)) + '</div>rated</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em">' + f"{avg:.1f}" + '</div>average</div>'
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em">🔥 ' + str(streak) + '</div>day streak</div>'
    foreign = lang_count.get("Foreign", 0)
    html += '<div class="card" style="text-align:center"><div style="font-size:2.5em">🌍 ' + str(foreign) + '</div>foreign films</div>'
    html += '</div>'
    # Monthly chart
    html += '<div class="card" style="margin-bottom:15px"><h3>📅 Ratings per Month</h3>'
    html += '<div style="display:flex;align-items:end;gap:1px;height:90px">' + month_bars + '</div>' + month_labels + '</div>'
    # Grid
    html += '<div class="grid">'
    html += '<div class="card"><h3>Rating Distribution</h3>' + dist_bars + '</div>'
    html += '<div class="card"><h3>Top Genres</h3>' + genre_bars + '</div>'
    html += '<div class="card"><h3>🎬 Top Directors</h3><table>' + dir_list + '</table></div>'
    html += '<div class="card"><h3>📅 By Decade</h3>' + decade_bars + '</div>'
    html += '</div></div>' + page_foot()
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
    html = page_head("Compare Users")
    html += nav_bar("social", "")
    html += render_social_nav("", "compare")
    html += '<div class="page">'
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
            poster = '<img src="<img src="' + l.get("poster","") + '" height=40 loading="lazy">" height=40 loading="lazy">' if l.get("poster") else ""
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
<tbody>{rows}</tbody></table></div>""" + page_foot()

