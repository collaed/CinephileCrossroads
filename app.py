#!/usr/bin/env python3
"""CineCross — Self-hosted multi-user movie & TV ratings dashboard."""
import csv, json, os, io, time, urllib.request, urllib.parse, threading, math
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn

from data import *
from data import _exec_results, _load_key, _merge_agent_data
from logic import *
from logic import _bg_auto_subs, _bg_catalog, _bg_enrich, _bg_history, _bg_trakt_sync, _bg_simkl_sync, _bg_anilist_sync
from logic import _discover_highly_rated, _is_tv, _rate_status, _canonical_path
from render import *

_routes = []
def route(pattern):
    def decorator(func):
        _routes.append((pattern, func))
        return func
    return decorator



# ── HTTP Server ───────────────────────────────────────────────────────
class H(BaseHTTPRequestHandler):
    """HTTP request handler. Routes are relative to BASE (stripped by reverse proxy).
    GET routes: /, /u/<user>, /recs/<user>, /catalog, /setup/<user>, /enrich, /jobs
    POST routes: /upload/<user>, /tmm/<user>, /keys"""
    def _user(self, path_parts):
        """Extract user from URL or default to ecb."""
        return DEFAULT_USER

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

        # Root / landing page
        if not parts or p == "/" or p == "/cinecross":
            self._html(render_getting_started())
            return

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
        if p == '/api/search':
            q = qs.get('q', [''])[0]
            user = qs.get('user', [''])[0] or (list_users() or [''])[0]
            limit = int(qs.get('limit', ['20'])[0])
            titles = load_titles()
            ratings = load_user_ratings(user) if user else {}
            library = load_user_tmm(user) if user else {}
            results = []
            if q:
                ids = search_fts(q, limit)
                if not ids:
                    ql = q.lower()
                    ids = [iid for iid, t in titles.items() if ql in (t.get("title","")+" "+t.get("originalTitle","")).lower()][:limit]
                for iid in ids:
                    t = titles.get(iid, {})
                    r = {"id": iid, "title": t.get("title",""), "year": t.get("year",""), "type": t.get("type",""),
                         "genres": t.get("genres",""), "imdb_rating": t.get("imdb_rating",""), "rt_score": t.get("rt_score",""),
                         "overview": t.get("overview","")[:200]}
                    if iid in ratings: r["user_rating"] = ratings[iid].get("rating")
                    if iid in library: r["in_library"] = True
                    results.append(r)
            self._json({"results": results, "count": len(results)})
            return
        if p == '/api/title':
            iid = qs.get('id', [''])[0]
            titles = load_titles()
            t = titles.get(iid, {})
            if not t:
                self._json({"error": "not found"})
                return
            user = qs.get('user', [''])[0] or (list_users() or [''])[0]
            ratings = load_user_ratings(user) if user else {}
            library = load_user_tmm(user) if user else {}
            info = dict(t)
            info["id"] = iid
            if iid in ratings: info["user_rating"] = ratings[iid].get("rating")
            if iid in library:
                lib_entry = library[iid]
                if isinstance(lib_entry, list):
                    info["library"] = [{"path": e.get("path",""), "quality": e.get("quality",""), "file_size": e.get("file_size",0)} for e in lib_entry]
                elif isinstance(lib_entry, dict):
                    info["library"] = [{"path": lib_entry.get("path",""), "quality": lib_entry.get("quality",""), "file_size": lib_entry.get("file_size",0)}]
            self._json(info)
            return
        if p == '/api/recommendations':
            user = qs.get('user', [''])[0] or (list_users() or [''])[0]
            n = int(qs.get('n', ['20'])[0])
            titles = load_titles()
            recs = get_5cat_recommendations(user, titles, n_per_cat=n//5 or 4)
            out = {}
            for cat, items in recs.items():
                out[cat] = [{"id": iid, "title": titles.get(iid,{}).get("title",""), "year": titles.get(iid,{}).get("year",""),
                             "score": round(sc, 2), "genres": titles.get(iid,{}).get("genres","")} for iid, sc in items]
            self._json(out)
            return
        if p == '/api/stats':
            user = qs.get('user', [''])[0] or (list_users() or [''])[0]
            titles = load_titles()
            ratings = load_user_ratings(user)
            library = load_user_tmm(user)
            genres = {}; decades = {}; quality = {}
            for iid, r in ratings.items():
                t = titles.get(iid, {})
                for g in (t.get("genres","") or "").split(","): 
                    g = g.strip()
                    if g: genres[g] = genres.get(g, 0) + 1
                y = t.get("year","")
                if y: decades[y[:3]+"0s"] = decades.get(y[:3]+"0s", 0) + 1
            for iid, info in library.items():
                if iid.startswith("_"): continue
                entries = info if isinstance(info, list) else [info] if isinstance(info, dict) else []
                for e in entries:
                    q = e.get("quality","")
                    if q: quality[q+"p" if q.isdigit() else q] = quality.get(q+"p" if q.isdigit() else q, 0) + 1
            self._json({"rated": len(ratings), "library": len([k for k in library if not k.startswith("_")]),
                        "genres": dict(sorted(genres.items(), key=lambda x:-x[1])[:15]),
                        "decades": dict(sorted(decades.items())), "quality": quality})
            return
        if p == '/api/queue_task':
            ttype = qs.get('type', [''])[0]
            params = json.loads(qs.get('params', ['{}'])[0])
            if ttype:
                tid = f"task_{int(time.time()*1000)}_{id(self)%1000}"
                db_enqueue_task(ttype, params, 0)
                self._json({"queued": tid})
            else:
                self._json({"error": "missing type"})
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
        elif p.startswith("/simkl/auth/"):
            u = parts[-1]
            os.makedirs(DATA_DIR, exist_ok=True)
            json.dump({"user": u}, open(f"{DATA_DIR}/_simkl_state.json", "w"))
            self._redirect(simkl_auth_url())
        elif p == "/simkl/callback":
            code = qs.get("code", [None])[0]
            state = json.load(open(f"{DATA_DIR}/_simkl_state.json")) if os.path.exists(f"{DATA_DIR}/_simkl_state.json") else {}
            u = state.get("user", user)
            if code:
                token = simkl_exchange_code(code)
                if token and "access_token" in token: save_user_simkl_token(u, token)
            self._redirect(f"{BASE}/")
        elif p.startswith("/simkl/sync/"):
            u = parts[-1]
            if not active_job()[1]: start_job("simkl_sync", _bg_simkl_sync, u)
            self._redirect(f"{BASE}/")
        elif p.startswith("/anilist/auth/"):
            u = parts[-1]
            os.makedirs(DATA_DIR, exist_ok=True)
            json.dump({"user": u}, open(f"{DATA_DIR}/_anilist_state.json", "w"))
            self._redirect(anilist_auth_url())
        elif p == "/anilist/callback":
            code = qs.get("code", [None])[0]
            state = json.load(open(f"{DATA_DIR}/_anilist_state.json")) if os.path.exists(f"{DATA_DIR}/_anilist_state.json") else {}
            u = state.get("user", user)
            if code:
                token = anilist_exchange_code(code)
                if token and "access_token" in token: save_user_anilist_token(u, token)
            self._redirect(f"{BASE}/")
        elif p.startswith("/anilist/sync/"):
            u = parts[-1]
            if not active_job()[1]: start_job("anilist_sync", _bg_anilist_sync, u)
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
            self._html(page_head("Activity Feed") + nav_bar("social", "") + render_social_nav("", "feed") + f"""<div class="page"><h2>📡 Activity Feed</h2><table>{rows}</table></div>""" + page_foot())
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
        elif p.startswith("/health/"):
            u = parts[-1]
            report = library_health_report(u)
            html = page_head("Library Health")
            html += nav_bar("library", u)
            html += render_library_nav(u, "health")
            html += '<div class="page">'
            html += '<h2>🏥 Library Health — Score: '
            sc = report["score"]
            color = "#22c55e" if sc >= 70 else "#f59e0b" if sc >= 40 else "#ef4444"
            html += '<span style="color:' + color + ';font-size:1.2em">' + str(sc) + '%</span></h2>'
            html += '<div style="background:#333;border-radius:8px;height:12px;margin-bottom:20px"><div style="background:' + color + ';height:100%;width:' + str(sc) + '%;border-radius:8px"></div></div>'
            for c in report["checks"]:
                p = int(c["ok"]/max(c["total"],1)*100)
                bc = "#22c55e" if p >= 70 else "#f59e0b" if p >= 40 else "#ef4444"
                html += '<div style="display:flex;align-items:center;gap:12px;margin:8px 0;padding:8px;background:var(--card,#16213e);border-radius:6px">'
                html += '<span style="font-size:1.3em">' + c["icon"] + '</span>'
                html += '<span style="width:120px">' + c["name"] + '</span>'
                html += '<div style="flex:1;height:8px;background:#333;border-radius:4px"><div style="width:' + str(p) + '%;height:100%;background:' + bc + ';border-radius:4px"></div></div>'
                html += '<span style="width:80px;text-align:right;font-size:.85em">' + str(c["ok"]) + '/' + str(c["total"]) + '</span>'
                html += '<span style="width:120px;font-size:.8em;color:var(--muted)">' + c["issue"] + '</span></div>'
            html += '</div>' + page_foot()
            self._page(html, "library", u)
            return
        elif p.startswith("/efficiency"):
            titles = load_titles()
            total = len(titles)
            has_poster = sum(1 for t in titles.values() if t.get("poster"))
            has_keywords = sum(1 for t in titles.values() if t.get("keywords"))
            has_rt = sum(1 for t in titles.values() if t.get("rotten_tomatoes"))
            has_streaming = sum(1 for t in titles.values() if t.get("providers"))
            has_trailer = sum(1 for t in titles.values() if t.get("trailer"))
            has_alt = sum(1 for t in titles.values() if t.get("alt_titles"))
            has_cast = sum(1 for t in titles.values() if t.get("cast"))
            has_similar = sum(1 for t in titles.values() if t.get("similar_tmdb"))
            def pbar(n, label):
                p = int(n/max(total,1)*100)
                color = "#22c55e" if p >= 70 else "#f59e0b" if p >= 40 else "#ef4444"
                return f'<div style="display:flex;align-items:center;gap:8px;margin:4px 0"><span style="width:120px;font-size:.85em">{label}</span><div style="flex:1;height:8px;background:#333;border-radius:4px"><div style="width:{p}%;height:100%;background:{color};border-radius:4px"></div></div><span style="width:60px;text-align:right;font-size:.8em;font-weight:600">{n}/{total}</span></div>'
            html = page_head("Enrichment Efficiency")
            html += nav_bar("setup", "")
            html += '<div class="page">'
            html += '<h2>📊 Enrichment Efficiency</h2>'
            html += '<div class="card" style="padding:16px">'
            html += pbar(has_poster, "Posters")
            html += pbar(has_keywords, "Keywords")
            html += pbar(has_cast, "Cast")
            html += pbar(has_rt, "RT Scores")
            html += pbar(has_streaming, "Streaming")
            html += pbar(has_trailer, "Trailers")
            html += pbar(has_alt, "Alt Titles")
            html += pbar(has_similar, "Similar")
            html += '</div>'
            # Source hit rates
            html += '<h3 style="margin-top:16px">API Source Coverage</h3>'
            html += '<div class="card" style="padding:16px;font-size:.85em">'
            html += '<table style="width:100%"><tr><th>Source</th><th>Provides</th><th>Coverage</th></tr>'
            html += f'<tr><td>TMDB</td><td>poster, keywords, cast, streaming, trailer, similar, alt titles</td><td>{int(has_poster/max(total,1)*100)}%</td></tr>'
            html += f'<tr><td>OMDB</td><td>RT score, Metacritic, plot</td><td>{int(has_rt/max(total,1)*100)}%</td></tr>'
            html += f'<tr><td>IMDB Dataset</td><td>title, year, genres, rating, votes</td><td>{int(total/max(total,1)*100)}%</td></tr>'
            html += '</table></div>'
            # Rate limiter status
            status = _rate_status()
            if any(v["fails"] > 0 for v in status.values()):
                html += '<h3 style="margin-top:16px">🚦 API Rate Limiter</h3>'
                html += '<div class="card" style="padding:16px;font-size:.85em"><table style="width:100%"><tr><th>Domain</th><th>Failures</th><th>Status</th></tr>'
                for domain, s in sorted(status.items()):
                    if s["fails"] == 0: continue
                    st = '<span style="color:#ef4444">⏸ Backed off ' + str(s["backoff_remaining"]) + 's</span>' if s["backed_off"] else '<span style="color:#f59e0b">⚠ ' + str(s["fails"]) + ' fails</span>'
                    html += '<tr><td>' + esc(domain) + '</td><td>' + str(s["fails"]) + '</td><td>' + st + '</td></tr>'
                html += '</table></div>'
            html += '<p style="margin-top:12px"><a href="' + BASE + '/enrich" class="btn">▶ Run Enrichment Now</a></p>'
            html += '</div>' + page_foot()
            self._page(html, "setup", "")
            return
        elif p.startswith("/search"):
            query = qs.get("q", [""])[0]
            results = []
            if query:
                fts_ids = search_fts(query)
                if fts_ids:
                    titles = load_titles()
                    results = [(iid, titles.get(iid, {})) for iid in fts_ids if iid in titles]
            html = page_head("Search")
            html += nav_bar("ratings", "")
            html += '<div class="page">'
            html += '<h2>🔍 Search</h2>'
            html += '<form method="GET" style="margin-bottom:16px"><input name="q" value="' + esc(query) + '" placeholder="Search titles, plots, cast, keywords..." style="width:400px;padding:8px"> <button class="btn">🔍</button></form>'
            if results:
                html += '<div class="poster-grid">'
                for iid, t in results[:40]:
                    poster = t.get("poster", "")
                    img = '<img src="' + poster + '" loading="lazy">' if poster else ""
                    html += '<a href="' + BASE + '/title/' + iid + '" style="text-decoration:none;color:var(--fg)"><div class="poster-card">' + img
                    html += '<div class="info"><div class="title">' + esc(t.get("title","")) + '</div>'
                    html += '<div class="meta">' + str(t.get("year","")) + '</div></div></div></a>'
                html += '</div>'
            elif query:
                html += '<p style="color:var(--muted)">No results for "' + esc(query) + '"</p>'
            html += '</div>' + page_foot()
            self._page(html, "ratings", "")
            return
        elif p.startswith("/companion/"):
            iid = parts[-1]
            question = qs.get("q", [""])[0]
            titles = load_titles()
            t = titles.get(iid, {})
            answer = movie_companion(iid, question, titles) if question else ""
            html = page_head("Movie Companion")
            html += '<div class="page">'
            html += '<h2>🎬 ' + esc(t.get("title","")) + ' — Companion</h2>'
            html += '<form method="GET" style="margin-bottom:12px"><input name="q" value="' + esc(question) + '" placeholder="Ask about this movie (no spoilers)..." style="width:400px;padding:8px"> <button class="btn">Ask</button></form>'
            if answer:
                html += '<div class="card" style="padding:16px;line-height:1.6">' + esc(answer).replace("\n","<br>") + '</div>'
            styles = [("eli5","🧒 ELI5"),("film_school","🎓 Film School"),("pitch","⚡ Pitch"),("debate","🥊 Debate")]
            html += '<h3 style="margin-top:20px">📝 Summaries</h3><div style="display:flex;gap:8px;flex-wrap:wrap">'
            for sid, label in styles:
                html += '<a href="' + BASE + '/summary/' + iid + '?style=' + sid + '" class="btn">' + label + '</a>'
            html += '</div>'
            html += '<p style="margin-top:12px"><a href="' + BASE + '/title/' + iid + '">← Back to title</a></p>'
            html += '</div>' + page_foot()
            self._page(html, "discover", "")
            return
        elif p.startswith("/summary/"):
            iid = parts[-1]
            style = qs.get("style", ["pitch"])[0]
            titles = load_titles()
            t = titles.get(iid, {})
            summary = movie_summary(iid, style, titles)
            style_labels = {"eli5":"🧒 Explain Like I'm 5","film_school":"🎓 Film School Analysis","pitch":"⚡ Elevator Pitch","debate":"🥊 Masterpiece or Overrated?"}
            html = page_head("Summary")
            html += '<div class="page">'
            html += '<h2>' + style_labels.get(style,"Summary") + '</h2>'
            html += '<h3>' + esc(t.get("title","")) + ' (' + str(t.get("year","")) + ')</h3>'
            html += '<div class="card" style="padding:16px;line-height:1.8;font-size:1.05em">' + esc(summary).replace("\n","<br>") + '</div>'
            html += '<p style="margin-top:12px"><a href="' + BASE + '/companion/' + iid + '">← Companion</a> · <a href="' + BASE + '/title/' + iid + '">Title</a></p>'
            html += '</div>' + page_foot()
            self._page(html, "discover", "")
            return
        elif p.startswith("/auto-tag/"):
            iid = parts[-1]
            titles = load_titles()
            tags = auto_tag_title(iid, titles)
            if tags:
                titles[iid]["ai_tags"] = tags
                save_titles(titles)
            self._redirect(f"{BASE}/title/{iid}")
            return
        elif p.startswith("/collections/"):
            u = parts[-2] if len(parts) >= 3 else parts[-1]
            action = parts[-1] if len(parts) >= 3 else ""
            colls = load_collections(u)
            titles = load_titles()
            if action == "add" and qs.get("name"):
                name = qs["name"][0]
                cid = "c_" + str(int(time.time()))
                colls[cid] = {"name": name, "items": [], "created": time.strftime("%Y-%m-%d")}
                save_collections(u, colls)
                self._redirect(f"{BASE}/collections/{u}")
                return
            if action == "item" and qs.get("cid") and qs.get("iid"):
                cid, iid = qs["cid"][0], qs["iid"][0]
                if cid in colls:
                    if iid not in colls[cid]["items"]:
                        colls[cid]["items"].append(iid)
                    save_collections(u, colls)
                self._redirect(f"{BASE}/title/{iid}")
                return
            html = page_head("Collections")
            html += nav_bar("ratings", u)
            html += '<div class="page">'
            html += '<h2>📂 Collections</h2>'
            html += '<form method="GET" action="' + BASE + '/collections/' + u + '/add" style="margin-bottom:16px"><input name="name" placeholder="New collection name..." style="padding:8px"> <button class="btn">+ Create</button></form>'
            for cid, c in colls.items():
                html += '<div class="card" style="margin-bottom:12px"><h3>' + esc(c["name"]) + ' <span style="color:var(--muted);font-size:.8em">(' + str(len(c["items"])) + ')</span></h3>'
                html += '<div class="poster-grid">'
                for iid in c["items"][:12]:
                    t = titles.get(iid, {})
                    poster = t.get("poster", "")
                    html += '<a href="' + BASE + '/title/' + iid + '"><div class="poster-card">'
                    if poster: html += '<img src="' + poster + '" loading="lazy">'
                    html += '<div class="info"><div class="title">' + esc(t.get("title","")) + '</div></div></div></a>'
                html += '</div></div>'
            if not colls:
                html += '<p style="color:var(--muted)">No collections yet. Create one above!</p>'
            html += '</div>' + page_foot()
            self._page(html, "ratings", u)
            return
        elif p.startswith("/watchlist-rss/"):
            u = parts[-1]
            rss = generate_watchlist_rss(u)
            self.send_response(200)
            self.send_header("Content-Type", "application/rss+xml")
            self.end_headers()
            self.wfile.write(rss.encode())
            return
        elif p.startswith("/fts/rebuild"):
            try:
                init_fts()
                rebuild_fts()
                self._json({"status": "ok"})
            except Exception as e:
                self._json({"error": str(e)})
            return
        elif p.startswith("/contribute/pull/"):
            u = parts[-2]
            source = parts[-1]
            titles = load_titles()
            ratings = load_user_ratings(u)
            library = load_user_tmm(u)
            updated = 0
            if source == "tmdb" and TMDB_KEY:
                # Pull alt_titles - use specific IDs if provided, else find from library
                specific = qs.get("ids", [""])[0].split(",") if qs.get("ids") else []
                specific = [i for i in specific if i.startswith("tt")]
                need = specific if specific else [iid for iid in library if not iid.startswith("_") and isinstance(library.get(iid), dict)
                        and titles.get(iid, {}).get("tmdb_id") and not titles.get(iid, {}).get("alt_titles")]
                for iid in need[:50]:
                    t = titles[iid]
                    tmdb_id = t["tmdb_id"]
                    kind = "tv" if _is_tv(t) else "movie"
                    alt = api_get(f"https://api.themoviedb.org/3/{kind}/{tmdb_id}/alternative_titles?api_key={TMDB_KEY}")
                    alt_list = (alt.get("titles") or alt.get("results") or []) if alt else []
                    t["alt_titles"] = [a["title"] for a in alt_list if a.get("title")][:15]
                    updated += 1
                    time.sleep(0.1)
                save_titles(titles)
            elif source == "tvdb" and TVDB_KEY:
                # Pull TV show data from TVDB
                for iid in list(library.keys())[:100]:
                    if iid.startswith("_"): continue
                    t = titles.get(iid, {})
                    if _is_tv(t) and not t.get("tvdb_id"):
                        data = tvdb_enrich(iid)
                        if data:
                            t.update({k:v for k,v in data.items() if v})
                            updated += 1
                    if updated >= 30: break
                save_titles(titles)
            elif source == "wikidata":
                # Pull multilingual titles from Wikidata
                need_wd = [iid for iid in ratings if not titles.get(iid, {}).get("alt_titles") and titles.get(iid, {}).get("title")]
                for iid in need_wd[:30]:
                    t = titles.get(iid, {})
                    title = t.get("title","")
                    try:
                        wd = api_get(f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={urllib.parse.quote(title)}&language=en&format=json&type=item&limit=1")
                        if wd and wd.get("search"):
                            qid = wd["search"][0]["id"]
                            entity = api_get(f"https://www.wikidata.org/w/api.php?action=wbgetentities&ids={qid}&format=json&props=labels")
                            if entity and entity.get("entities",{}).get(qid):
                                labels = entity["entities"][qid].get("labels",{})
                                alt = [v["value"] for v in labels.values() if v["value"] != title][:10]
                                if alt:
                                    t.setdefault("alt_titles", []).extend(alt)
                                    t["alt_titles"] = list(set(t["alt_titles"]))[:15]
                                    updated += 1
                    except: pass
                    time.sleep(0.2)
                    if updated >= 20: break
                save_titles(titles)
            self._html(f"<html><body>pulled={source}&count={updated}</body></html>")
            return
        elif p.startswith("/contribute/"):
            u = parts[-1]
            titles = load_titles()
            ratings = load_user_ratings(u)

            # Find titles with actual data gaps (not random)
            rated_ids = [iid for iid in ratings if titles.get(iid, {}).get("tmdb_id")]
            import random, hashlib
            # Rotate through titles based on time (different set each visit)
            seed = int(time.time()) // 60  # changes every minute
            random.seed(seed)
            gap_ids = [iid for iid in rated_ids if not titles.get(iid,{}).get("alt_titles")]
            ok_ids = [iid for iid in rated_ids if iid not in gap_ids]
            random.shuffle(gap_ids)
            random.shuffle(ok_ids)
            sample = gap_ids[:5] + ok_ids[:3]
            random.seed()  # reset

            rows = ""
            we_have_more = 0
            they_have_more = 0
            for iid in sample:
                t = titles.get(iid, {})
                tmdb_id = t.get("tmdb_id")
                our_fields = sum(1 for k in ("keywords","cast","directors","writers","overview","poster","trailer","rotten_tomatoes","metacritic") if t.get(k))
                # Check what TMDB has
                tmdb_data = api_get(f"https://api.themoviedb.org/3/movie/{tmdb_id}?api_key={TMDB_KEY}&append_to_response=credits,keywords,alternative_titles") if TMDB_KEY and tmdb_id else {}
                tmdb_fields = 0
                gaps_us = []  # we are missing
                gaps_them = []  # they are missing
                if tmdb_data:
                    if tmdb_data.get("overview"): tmdb_fields += 1
                    elif t.get("overview"): gaps_them.append("overview")
                    if tmdb_data.get("poster_path"): tmdb_fields += 1
                    kw_them = len(tmdb_data.get("keywords",{}).get("keywords",[]))
                    kw_us = len(t.get("keywords",[]))
                    if kw_them > kw_us + 5: gaps_us.append(f"keywords ({kw_us} vs {kw_them})")
                    elif kw_us > kw_them + 5: gaps_them.append(f"keywords ({kw_them} vs {kw_us})")
                    cast_them = len(tmdb_data.get("credits",{}).get("cast",[]))
                    cast_us = len((t.get("cast","") or "").split(","))
                    alt_them = len(tmdb_data.get("alternative_titles",{}).get("titles",[]))
                    alt_us = len(t.get("alt_titles",[]))
                    if alt_them and not alt_us: gaps_us.append(f"alt_titles (0 vs {alt_them})")
                    if t.get("rotten_tomatoes") and not tmdb_data.get("vote_average"): gaps_them.append("RT score")

                if gaps_them: we_have_more += 1
                if gaps_us: they_have_more += 1

                gap_us_html = " ".join(f'<span style="color:#f90;font-size:.8em">{g}</span>' for g in gaps_us) or '<span style="color:#4c8;font-size:.8em">complete</span>'
                gap_them_html = " ".join(f'<span style="color:#4c8;font-size:.8em">{g}</span>' for g in gaps_them) or "-"

                rows += f'<tr><td><a href="{BASE}/title/{iid}">{t.get("title","")}</a></td><td>{t.get("year","")}</td>'
                rows += f'<td>{our_fields}</td><td>{gap_us_html}</td><td>{gap_them_html}</td></tr>'
                time.sleep(0.15)  # Rate limit

            # Wikidata: check how many of our titles have Wikidata entries
            wd_count = 0
            wd_sample = random.sample(list(ratings.keys()), min(10, len(ratings)))
            for iid in wd_sample:
                try:
                    wd = api_get(f"https://www.wikidata.org/w/api.php?action=wbgetentities&sites=enwiki&format=json&props=labels&titles={urllib.parse.quote(titles.get(iid,{}).get('title',''))}")
                    if wd and wd.get("entities") and "-1" not in wd.get("entities",{}):
                        wd_count += 1
                except: pass

            html = page_head(f"Contribute - {u}")
            html += nav_bar("social", u)
            html += render_social_nav(u, "contribute")
            html += '<div class="page">'
            html += '<h2>🌍 Contribute to Movie Databases</h2>'
            html += f'<p style="color:var(--muted)">Comparing your data with TMDB for {len(sample)} titles. Wikidata coverage: ~{wd_count*10}% of your rated titles.</p>'

            html += '<div class="grid" style="margin-bottom:20px">'
            html += f'<div class="card" style="text-align:center"><div style="font-size:2em;color:#4c8">{we_have_more}</div>We have more</div>'
            html += f'<div class="card" style="text-align:center"><div style="font-size:2em;color:#f90">{they_have_more}</div>They have more</div>'
            html += f'<div class="card" style="text-align:center"><div style="font-size:2em">{len(sample)-we_have_more-they_have_more}</div>Equal</div>'
            html += '</div>'

            html += '<h3>📊 Data Comparison (sample of 20)</h3>'
            html += '<table><thead><tr><th>Title</th><th>Year</th><th>Our fields</th><th>We need</th><th>We can give</th></tr></thead>'
            html += '<tbody>' + rows + '</tbody></table>'

            html += '<h3 style="margin-top:20px">🔄 Actions</h3>'
            html += '<div class="grid">'
            html += f'<div class="card"><b>TMDB</b><br>'
            gap_ids = ','.join(iid for iid in sample if not titles.get(iid,{}).get('alt_titles'))
            html += '<button onclick="doPull(' + "'" + 'tmdb' + "'" + ',' + "'" + gap_ids + "'" + ')" class="btn" style="margin-top:8px">Pull (' + str(gap_ids.count('tt')) + ')</button>'
            html += f'<br><small style="color:var(--muted)">Fetches alternative titles + missing keywords</small><div id="tmdb_status"></div></div>'
            html += f'<div class="card"><b>TVDB</b><br>'
            html += f'<button onclick="doPull(\'tvdb\')" class="btn" style="margin-top:8px">⬇ Pull TV data</button>'
            html += f'<br><small style="color:var(--muted)">TV episode data, cross-references</small><div id="tvdb_status"></div></div>'
            html += f'<div class="card"><b>Wikidata</b><br>'
            html += f'<button onclick="doPull(\'wikidata\')" class="btn" style="margin-top:8px">⬇ Pull translations</button>'
            html += f'<br><small style="color:var(--muted)">Multilingual titles for matching</small><div id="wikidata_status"></div></div>'
            html += '</div>'
            html += '<script>function doPull(src,ids){var el=document.getElementById(src+"_status");el.innerHTML="<br>Pulling...";fetch("' + BASE + '/contribute/pull/' + u + '/"+src+(ids?"?ids="+ids:"")).then(function(r){return r.text()}).then(function(t){var m=t.match(/count=(\\d+)/);el.innerHTML="<br>Updated "+(m?m[1]:"0")+" titles"})}</script>'

            html += '</div>' + page_foot()
            self._page(html, "setup", u)
            return
        elif p.startswith("/incoming/"):
            u = parts[-1]
            incoming_file = os.path.join(DATA_DIR, "users", u, "incoming.json")
            incoming = safe_json_load(incoming_file) or []
            library = _merge_agent_data(load_user_tmm(u), u)
            titles = load_titles()
            pending = [f for f in incoming if f.get("status") == "pending"]

            # Split into movies and TV
            movies = [f for f in pending if not f.get("tmdb_match", {}).get("type") == "tv" and not parse_movie_filename(f.get("filename","")).get("is_tv")]
            tv = [f for f in pending if f.get("tmdb_match", {}).get("type") == "tv" or parse_movie_filename(f.get("filename","")).get("is_tv")]

            html = page_head(f"Incoming - {u}")
            html += nav_bar("library", u)
            html += render_library_nav(u, "incoming")
            html += '<div class="page">'
            html += f'<h2>📥 Incoming — {len(pending)} files</h2>'
            html += f'<div class="grid" style="margin-bottom:16px"><div class="card" style="text-align:center"><div style="font-size:2em">{len(movies)}</div>Movies</div><div class="card" style="text-align:center"><div style="font-size:2em">{len(tv)}</div>TV Episodes</div></div>'

            for section_name, section_items in [("🎬 Movies", movies), ("📺 TV Episodes", tv)]:
                if not section_items: continue
                html += f'<h3>{section_name} ({len(section_items)})</h3>'
                html += '<div class="poster-grid">'
                for f in section_items[:50]:
                    match = f.get("tmdb_match", {})
                    parsed = parse_movie_filename(f.get("filename", ""))
                    title = match.get("title", f.get("title_guess", "?"))
                    year = match.get("year", f.get("year_guess", ""))
                    poster_url = match.get("poster", "")
                    poster_img = f'<img src="{poster_url}" alt="">' if poster_url else '<div style="width:100%;aspect-ratio:2/3;background:var(--border);display:flex;align-items:center;justify-content:center;font-size:.7em;color:var(--muted);padding:8px;text-align:center">' + f.get("filename","")[:30] + '</div>'
                    ep_info = ""
                    if parsed.get("is_tv"):
                        ep_info = f' S{parsed.get("season","")}E{parsed.get("episode","")}'
                    size_gb = f.get("size", 0) / 1073741824
                    # Check if duplicate
                    tmdb_id = match.get("id")
                    is_dupe = False
                    lib_quality = ""
                    if tmdb_id:
                        for lib_iid, lib_info in library.items():
                            if isinstance(lib_info, dict) and titles.get(lib_iid, {}).get("tmdb_id") == tmdb_id:
                                is_dupe = True
                                lib_quality = str(lib_info.get("video_height", "")) + "p " + str(lib_info.get("video_codec", ""))
                                break
                    border = "2px solid #d72" if is_dupe else "1px solid var(--border)"
                    dupe_badge = '<div style="position:absolute;bottom:40px;left:4px;background:#d72;color:#fff;padding:1px 6px;border-radius:3px;font-size:.7em">DUPE ' + lib_quality + '</div>' if is_dupe else ""
                    enc_path = urllib.parse.quote(f.get("path", ""))
                    if is_dupe:
                        action = f'<a href="{BASE}/incoming-delete/{u}?path={enc_path}" style="color:#d72;font-size:.8em">🗑 Delete</a>'
                    elif match:
                        action = f'<a href="{BASE}/incoming-confirm/{u}?path={enc_path}" style="color:#4c8;font-size:.8em">✅ Import</a>'
                    else:
                        action = f'<a href="{BASE}/scraper-match/{u}/incoming?q={urllib.parse.quote(f.get("title_guess",""))}" style="font-size:.8em">🔍 Match</a>'
                    html += f'<div class="poster-card" style="border:{border}">{poster_img}{dupe_badge}<div class="info"><div class="title">{title}{ep_info}</div><div class="meta">{year} · {size_gb:.1f}GB</div><div>{action}</div></div></div>'
                html += '</div>'

            html += '</div>' + page_foot()
            self._page(html, "library", u)
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
            html += render_discover_nav(u, "ai")
            html += '<div class="page">'
            html += '<h2>🤖 My AI Friend Recommends</h2>'
            html += '<p style="color:var(--muted)">Based on your ' + str(len(ratings)) + ' ratings and ' + str(len(profile["keywords"])) + ' taste keywords.</p>'

            # Hidden Gems
            html += '<h3>💎 Hidden Gems in Your Library</h3>'
            html += '<p style="color:var(--muted);font-size:.85em">You own these but have not rated them. Your taste profile says you will love them.</p>'
            html += '<div class="poster-grid">'
            for iid, t, score, imdb_r in gems[:20]:
                poster_url = t.get("poster", "")
                poster_img = f'<img src="{poster_url}" alt="">' if poster_url else '<div style="width:100%;aspect-ratio:2/3;background:var(--border);display:flex;align-items:center;justify-content:center;color:var(--muted)">No poster</div>'
                rating_color = "#4c8" if score > 100 else "#f90" if score > 50 else "#888"
                html += f'<a href="{BASE}/title/{iid}" style="text-decoration:none;color:var(--fg)"><div class="poster-card">{poster_img}<div class="rating" style="color:{rating_color}">{score:.0f}</div><div class="badge">{imdb_r}</div><div class="info"><div class="title">{t["title"]}</div><div class="meta">{t.get("year","")}</div></div></div></a>'
            html += '</div>'

            # Why do I have this
            if why:
                html += '<h3 style="margin-top:30px">🤔 Why Do I Have This?</h3>'
                html += '<p style="color:var(--muted);font-size:.85em">Low taste match AND low IMDB rating. Prime candidates for cleanup.</p>'
                html += '<table><thead><tr><th>Title</th><th>Year</th><th>IMDB</th><th>TMDB Match</th></tr></thead><tbody>'
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
                    "subs": ", ".join(LANG_NAMES.get(s.get("language",""),"?") if isinstance(s,dict) else str(s) for s in (info.get("subtitles") or [])[:3]) or "no",
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
            library = load_user_tmm(u)
            titles = load_titles()
            # Analyze by drive/folder
            from collections import defaultdict
            by_drive = defaultdict(lambda: {"count": 0, "size": 0})
            by_decade = defaultdict(int)
            by_genre = defaultdict(int)
            for iid, info in library.items():
                if iid.startswith("_") or not isinstance(info, dict): continue
                path = info.get("path", "")
                t = titles.get(iid, {})
                # Drive = first path component after NFS root
                parts_p = path.replace("\\", "/").split("/")
                drive = "/".join(parts_p[:6]) if len(parts_p) > 5 else path[:30]
                by_drive[drive]["count"] += 1
                by_drive[drive]["size"] += info.get("file_size", 0) or 0
                # Decade
                year = str(t.get("year", ""))[:3] + "0s" if t.get("year") else "Unknown"
                by_decade[year] += 1
                # Genre
                for g in (t.get("genres", "") or "").split(","):
                    g = g.strip()
                    if g: by_genre[g] += 1

            html = page_head(f"Organize - {u}")
            html += nav_bar("library", u)
            html += render_library_nav(u, "org")
            html += '<div class="page">'
            html += '<h2>🗂 Organize Library</h2>'

            # By drive
            html += '<h3>By Location</h3><table><thead><tr><th>Path</th><th>Files</th><th>Size</th></tr></thead><tbody>'
            for drive, info in sorted(by_drive.items(), key=lambda x: x[1]["size"], reverse=True):
                size_str = f'{info["size"]/1073741824:.1f} GB' if info["size"] else "-"
                html += f'<tr><td style="font-family:monospace;font-size:.85em">{drive}</td><td>{info["count"]}</td><td>{size_str}</td></tr>'
            html += '</tbody></table>'

            # By decade
            html += '<h3>By Decade</h3><div style="display:flex;flex-wrap:wrap;gap:8px">'
            for dec in sorted(by_decade.keys()):
                html += f'<span class="card" style="padding:6px 12px">{dec}: <b>{by_decade[dec]}</b></span>'
            html += '</div>'

            # By genre
            html += '<h3>By Genre</h3><div style="display:flex;flex-wrap:wrap;gap:8px">'
            for g, c in sorted(by_genre.items(), key=lambda x: x[1], reverse=True)[:20]:
                html += f'<span class="card" style="padding:6px 12px">{g}: <b>{c}</b></span>'
            html += '</div>'

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
                    matches.append(f'<a href="{BASE}/scraper-apply/{u}/{iid}/{r["id"]}/{r.get("media_type","movie")}" style="display:flex;gap:8px;align-items:center;padding:8px;background:var(--card);border-radius:6px;margin:4px 0;text-decoration:none;color:var(--fg)"><img src="{poster}" height="60" loading="lazy">{title} ({year})</a>')
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
            # LLM batch check for 0% matches (translations)
            if qs.get("ai", [""])[0] == "1":
                mismatches = llm_batch_check_translations(mismatches)
                mismatches = [m for m in mismatches if m["match"] < 0.2]
                mismatches.sort(key=lambda x: x["match"])
            # LLM single check if requested
            llm_check = qs.get("llm", [""])[0]
            llm_result = ""
            if llm_check and _load_key("llm_url"):
                m = next((x for x in mismatches if x["iid"] == llm_check), None)
                if m:
                    prompt = f'Is "{m["path_title"]}" a translation or alternate title of the movie "{m["db_title"]}" ({m.get("year","")})? Answer YES or NO, then briefly explain in one sentence.'
                    answer = llm_ask(prompt, system="You are a multilingual movie expert. Be concise.", max_tokens=80)
                    llm_result = f'<div class="card" style="margin-bottom:12px;padding:12px"><b>🤖 AI says about {esc(m["db_title"])}:</b> {esc(answer)}</div>'
            rows = ""
            for m in mismatches[:200]:
                short_path = m["path"].split("/")[-1] if "/" in m["path"] else m["path"].split(chr(92))[-1]
                if short_path.lower() in ("video_ts.ifo","index.bdmv"):
                    parts_p = m["path"].replace(chr(92),"/").split("/")
                    short_path = "/".join(parts_p[-3:]) if len(parts_p)>=3 else short_path
                pct = int(m["match"]*100)
                match_color = "#4c8" if pct >= 50 else "#f90" if pct >= 20 else "#d72"
                via = m.get("via", "")
                via_badge = ""
                if via.startswith("alt:"): via_badge = ' <span style="font-size:.7em;color:#48f">🌍 ' + esc(via[4:]) + '</span>'
                elif via == "originalTitle": via_badge = ' <span style="font-size:.7em;color:#a8f">🔤 original</span>'
                rows += '<tr><td><a href="' + BASE + '/title/' + m["iid"] + '">' + esc(m["db_title"]) + '</a> (' + str(m.get('year','')) + ')' + via_badge + '</td>'
                rows += '<td style="font-size:.85em;color:var(--muted);max-width:300px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="' + esc(m["path"]) + '">' + esc(short_path) + '</td>'
                rows += '<td style="color:' + match_color + '">' + str(pct) + '%</td>'
                llm_btn = ' <a href="' + BASE + '/confirm/' + u + '?llm=' + m["iid"] + '" class="btn" style="background:#346" title="Ask AI">🤖</a>' if _load_key("llm_url") else ""
                rows += '<td><a href="' + BASE + '/confirm-ok/' + u + '/' + m["iid"] + '" class="btn" style="background:#2a5">✅</a> <a href="' + BASE + '/scraper-match/' + u + '/' + m["iid"] + '?q=' + urllib.parse.quote(m["db_title"]) + '" class="btn">🔍</a>' + llm_btn + '</td></tr>'
            html = page_head(f"To Be Confirmed - {u}")
            html += nav_bar("library", u)
            html += '<div class="page">'
            html += f'<h2>⚠ To Be Confirmed — {len(mismatches)} mismatches</h2>'
            html += '<p style="color:var(--muted)">Titles where the filename doesn\'t match any known title (IMDB, original, or translated). Low % = likely wrong match.</p>'
            if _load_key("llm_url"):
                html += '<a href="' + BASE + '/confirm/' + u + '?ai=1" class="btn" style="margin-bottom:12px;display:inline-block">🤖 AI Check Translations (top 20)</a> '
            html += llm_result
            html += '<table><thead><tr><th onclick="sortTable(0)">IMDB Title</th><th>Filename</th><th onclick="sortTable(2)">Match</th><th></th></tr></thead>'
            html += '<tbody>' + rows + '</tbody></table>'
            html += '<script>function sortTable(n){const tb=document.querySelector("tbody"),rows=[...tb.rows],dir=tb.dataset.sort==n?-1:1;tb.dataset.sort=dir==1?n:"";rows.sort((a,b)=>{let x=a.cells[n].textContent,y=b.cells[n].textContent;return(typeof x==="number"&&typeof y==="number"?(x-y):(String(x)).localeCompare(String(y),undefined,{numeric:true}))*dir});rows.forEach(r=>tb.appendChild(r))}</script>'
            html += '</div>' + page_foot()
            self._page(html, "library", u)
            return
        elif p.startswith("/incoming-delete/"):
            u = parts[-1]
            inc_path = qs.get("path", [""])[0]
            incoming_file = os.path.join(DATA_DIR, "users", u, "incoming.json")
            incoming = safe_json_load(incoming_file) or []
            for f in incoming:
                if f.get("path") == inc_path:
                    enqueue_task("delete_file", {"path": inc_path, "confirm": "yes_delete"}, priority=-1)
                    f["status"] = "deleted"
                    safe_json_save(incoming_file, incoming)
                    break
            self._redirect(f"{BASE}/confirm/{u}")
            return
        elif p.startswith("/incoming-confirm/"):
            u = parts[-1]
            inc_path = qs.get("path", [""])[0]
            incoming_file = os.path.join(DATA_DIR, "users", u, "incoming.json")
            incoming = safe_json_load(incoming_file) or []
            # Find the file
            for f in incoming:
                if f.get("path") == inc_path:
                    match = f.get("tmdb_match", {})
                    if match:
                        # Build destination path using convention
                        conv = detect_library_convention(u)
                        parsed = parse_movie_filename(f.get("filename", ""))
                        title = match.get("title", parsed.get("title_guess", "unknown"))
                        year = match.get("year", parsed.get("year_guess", ""))
                        quality = parsed.get("quality", "1080p")
                        source = detect_video_source(f.get("path", ""))
                        ext = f.get("filename", "").rsplit(".", 1)[-1] if "." in f.get("filename", "") else "mkv"
                        dest = build_destination_path(title, year, quality, "", "", source or "Webrip", ext, conv)
                        # Full NFS destination
                        nfs_base = "nfs://192.168.0.235/volume1/Movies/"
                        full_dest = nfs_base + dest
                        # Queue move task
                        enqueue_task("move_file", {"source": inc_path, "destination": full_dest}, priority=-1)
                        f["status"] = "confirmed"
                        f["destination"] = full_dest
                        safe_json_save(incoming_file, incoming)
                        print(f"[incoming] Confirmed: {f.get('filename','')} -> {dest}")
                    break
            self._redirect(f"{BASE}/confirm/{u}")
            return
        elif p.startswith("/confirm-ok/"):
            u = parts[-2]
            iid = parts[-1]
            library = load_user_tmm(u)
            if iid in library and isinstance(library[iid], dict):
                library[iid]["confirmed"] = True
                save_user_tmm(u, library)
            self._redirect(f"{BASE}/confirm/{u}")
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
                # BPP quality indicator with color
                bpp = lib_info.get("bpp", 0)
                if not bpp and lib_info.get("file_size") and lib_info.get("video_height") and lib_info.get("video_width"):
                    # Compute BPP on the fly
                    w, h = lib_info.get("video_width", 1920), lib_info.get("video_height", 1080)
                    runtime_s = lib_info.get("runtime", 0)
                    if not runtime_s and iid in titles:
                        runtime_s = int(titles[iid].get("runtime", 0) or 0) * 60
                    if runtime_s > 0:
                        vbr = (lib_info["file_size"] * 8 * 0.9) / runtime_s  # 90% video estimate
                        bpp = vbr / (w * h * 24)
                if bpp:
                    # Color: red (<0.04) -> orange (0.04-0.07) -> yellow (0.07-0.1) -> green (>0.1)
                    if bpp < 0.04: bpp_color = "#e74c3c"
                    elif bpp < 0.07: bpp_color = "#f39c12"
                    elif bpp < 0.12: bpp_color = "#f1c40f"
                    else: bpp_color = "#2ecc71"
                    local_html += f' · <span style="color:{bpp_color};font-weight:bold" title="Bits per pixel: {bpp:.3f}">●</span>'
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
            # L1 mood badges (instant, no LLM)
            l1 = movie_summary_l1(iid, titles)
            if l1.get("moods"):
                html += '<div style="margin-top:8px">'
                for mood in l1["moods"]:
                    html += '<span style="background:#234;padding:3px 10px;border-radius:12px;font-size:.8em;margin:2px">🎭 ' + esc(mood) + '</span>'
                html += '</div>'
            # AI tags
            if t.get("ai_tags"):
                html += '<div style="margin-top:6px">'
                for tag in t["ai_tags"]:
                    html += '<span style="background:#342;padding:3px 10px;border-radius:12px;font-size:.8em;margin:2px">🏷 ' + esc(tag) + '</span>'
                html += '</div>'
            # Collections
            u = self._user(parts)
            colls = load_collections(u) if u else {}
            if colls:
                html += '<div style="margin-top:10px"><select onchange="if(this.value)location=this.value" style="padding:4px;background:#16213e;color:#eee;border:1px solid #444;border-radius:4px"><option value="">📂 Add to collection...</option>'
                for cid, c in colls.items():
                    html += '<option value="' + BASE + '/collections/' + u + '/item?cid=' + cid + '&iid=' + iid + '">' + esc(c["name"]) + '</option>'
                html += '</select></div>'
            # Companion + auto-tag
            if _load_key("llm_url"):
                html += '<div style="margin-top:10px;display:flex;gap:8px;flex-wrap:wrap">'
                html += '<a href="' + BASE + '/companion/' + iid + '" class="btn">🎬 Companion</a>'
                if not t.get("ai_tags"):
                    html += '<a href="' + BASE + '/auto-tag/' + iid + '" class="btn" style="background:#346">🏷 Auto-Tag</a>'
                html += '</div>'
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
        elif p.startswith("/verify/"):
            u = parts[-1] if len(parts) > 1 else self._user(parts)
            self._page(render_verification(u), "library", u)
            return
        elif p.startswith("/library/backlog/"):
            u = parts[-1] if len(parts) > 1 else self._user(parts)
            self._page(render_backlog(u), "library", u)
            return
        elif p.startswith("/library/suggestions/"):
            u = parts[-1] if len(parts) > 1 else self._user(parts)
            action = qs.get("action", [""])[0]
            iid = qs.get("iid", [""])[0]
            msg = ""
            if action == "transcode" and iid:
                library = load_user_tmm(u)
                entry = library.get(iid)
                entries = entry if isinstance(entry, list) else [entry] if isinstance(entry, dict) else []
                for e in entries:
                    if isinstance(e, dict) and e.get("path"):
                        db_enqueue_task("transcode_dvd", {"path": e["path"], "crf": 22, "preset": "medium"}, PRIORITY_HUMAN)
                        msg = "✅ Transcode queued"
                        break
            elif action == "flag_upgrade" and iid:
                titles = load_titles()
                t = titles.get(iid, {})
                is_tv = t.get("type") in ("tvSeries", "tvMiniSeries")
                db_enqueue_task("search_upgrade", {
                    "imdb_id": iid, "tmdb_id": t.get("tmdb_id", 0), "tvdb_id": t.get("tvdb_id", 0),
                    "title": t.get("title", ""), "year": t.get("year", ""), "is_tv": is_tv
                }, PRIORITY_HUMAN)
                db = get_db()
                db.execute("INSERT OR REPLACE INTO agent_data (user,imdb_id,field,value,updated_at) VALUES (?,?,?,?,?)",
                    (u, iid, "upgrade_wanted", "1", time.strftime("%Y-%m-%d %H:%M:%S")))
                db.commit()
                msg = f"✅ Searching for upgrade: {t.get('title', iid)}"
            body = render_suggestions(u)
            if msg:
                body = body.replace('<h2>💡', f'<div style="background:#2d7;color:#1a1a2e;padding:10px 16px;border-radius:6px;margin-bottom:12px;font-weight:600">{msg}</div><h2>💡', 1)
            self._page(body, "library", u)
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
        elif p.startswith("/updates"):
            page_num = int(qs.get("p", ["1"])[0])
            log_path = os.path.join(DATA_DIR, "enrichment_log.json")
            changelog = safe_json_load(log_path) or []
            changelog.reverse()  # newest first
            per_page = 50
            total_pages = max(1, (len(changelog) + per_page - 1) // per_page)
            page_items = changelog[(page_num-1)*per_page : page_num*per_page]

            html = page_head("Recent Updates")
            html += nav_bar("discover", "")
            html += render_discover_nav("", "updates")
            html += '<div class="page">'
            html += f'<h2>📋 Recent Enrichment Updates</h2>'
            html += f'<p style="color:var(--muted)">{len(changelog)} changes tracked. Page {page_num}/{total_pages}.</p>'
            html += '<table><thead><tr><th>Time</th><th>Title</th><th>Changes</th></tr></thead><tbody>'
            for entry in page_items:
                change_tags = ""
                for k, v in entry.get("changes", {}).items():
                    colors = {"poster": "#48f", "keywords": "#4c8", "cast": "#f90", "directors": "#a6e",
                              "rotten_tomatoes": "#d72", "metacritic": "#d72", "providers": "#4fc3f7",
                              "genres": "#888", "trailer": "#ff0", "overview": "#888", "writers": "#a6e", "similar_tmdb": "#48f"}
                    c = colors.get(k, "#888")
                    change_tags += f'<span style="background:{c}22;color:{c};padding:1px 6px;border-radius:3px;font-size:.8em;margin:1px">{k}: {v}</span> '
                html += f'<tr><td style="white-space:nowrap;font-size:.8em;color:var(--muted)">{entry["ts"][5:16]}</td>'
                html += f'<td><a href="{BASE}/title/{entry["iid"]}">{entry["title"]}</a> ({entry.get("year","")})</td>'
                html += f'<td>{change_tags}</td></tr>'
            html += '</tbody></table>'

            # Pagination with lazy loading
            if total_pages > 1:
                html += '<div id="pager" style="text-align:center;margin:20px">'
                if page_num < total_pages:
                    html += f'<button onclick="loadMore()" class="btn" id="loadBtn">Load more</button>'
                html += '</div>'
                html += '<script>'
                html += 'var nextPage = ' + str(page_num + 1) + ';'
                html += 'var maxPage = ' + str(total_pages) + ';'
                html += 'function loadMore(){'
                html += '  if(nextPage>maxPage)return;'
                html += '  var btn=document.getElementById("loadBtn");btn.textContent="Loading...";'
                html += '  fetch("' + BASE + '/api/updates?p="+nextPage).then(r=>r.text()).then(html=>{'
                html += '    document.querySelector("tbody").insertAdjacentHTML("beforeend",html);'
                html += '    nextPage++;btn.textContent=nextPage>maxPage?"No more":"Load more";'
                html += '  });}'
                html += '</script>'

            html += '</div>' + page_foot()
            self._page(html, "discover", "")
            return
        elif p.startswith("/api/updates"):
            page_num = int(qs.get("p", ["1"])[0])
            log_path = os.path.join(DATA_DIR, "enrichment_log.json")
            changelog = safe_json_load(log_path) or []
            changelog.reverse()
            per_page = 50
            page_items = changelog[(page_num-1)*per_page : page_num*per_page]
            rows = ""
            for entry in page_items:
                change_tags = ""
                for k, v in entry.get("changes", {}).items():
                    colors = {"poster": "#48f", "keywords": "#4c8", "cast": "#f90", "directors": "#a6e",
                              "rotten_tomatoes": "#d72", "metacritic": "#d72", "providers": "#4fc3f7"}
                    c = colors.get(k, "#888")
                    change_tags += f'<span style="background:{c}22;color:{c};padding:1px 6px;border-radius:3px;font-size:.8em;margin:1px">{k}: {v}</span> '
                rows += f'<tr><td style="white-space:nowrap;font-size:.8em;color:var(--muted)">{entry["ts"][5:16]}</td>'
                rows += f'<td><a href="{BASE}/title/{entry["iid"]}">{entry["title"]}</a> ({entry.get("year","")})</td>'
                rows += f'<td>{change_tags}</td></tr>'
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(rows.encode())
            return
        else:
            if not parts or (len(parts) == 1 and parts[0] == "cinecross"):
                self._html(render_getting_started())
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
        # Batch delete from library duplicates page
        if "/batch-delete" in self.path:
            import urllib.parse as _up
            form_data = _up.parse_qs(body.decode())
            paths = form_data.get("paths", [])
            confirm = form_data.get("confirm", [""])[0]
            user = parts[1] if len(parts) > 1 else ""
            if confirm == "yes_delete" and paths:
                # Queue delete tasks for each path
                for path in paths:
                    tid = f"del_{int(time.time()*1000)}_{hash(path)%10000}"
                    db_enqueue_task("delete_file", {"path": path, "confirm": "yes_delete"}, 1)
                print(f"[batch-delete] Queued {len(paths)} deletions for {user}")
            self._redirect(f"{BASE}/library/{user}")
            return
        # Library push - handle early
        if self.path.startswith("/api/library/"):
            user = parts[-1]
            try:
                data = json.loads(body.decode())
                library = load_user_tmm(user)
                old_library = set(library.keys())
                # Merge: keep existing fields (file_size, file_hash, nfo_matched, etc.)
                for iid, info in data.get("library", {}).items():
                    if iid in library and isinstance(library[iid], dict) and isinstance(info, dict):
                        existing_path = library[iid].get("path", "")
                        new_path = info.get("path", "")
                        if existing_path and new_path and _canonical_path(existing_path) != _canonical_path(new_path):
                            # Same IMDB ID, different paths = true duplicate
                            library[iid] = [library[iid], info]
                        else:
                            library[iid].update(info)
                    elif iid in library and isinstance(library[iid], list) and isinstance(info, dict):
                        # Already a list, add if new path
                        canon_paths = [_canonical_path(e.get("path", "")) for e in library[iid]]
                        if _canonical_path(info.get("path", "")) not in canon_paths:
                            library[iid].append(info)
                        else:
                            for e in library[iid]:
                                if _canonical_path(e.get("path", "")) == _canonical_path(info.get("path", "")): e.update(info); break
                    else:
                        library[iid] = info
                save_user_tmm(user, library)
                # Only regenerate tasks if new items were added
                new_items = sum(1 for iid in data.get("library", {}) if iid not in old_library)
                task_count = 0
                if new_items > 0:
                    task_count = generate_tasks_for_library(user)
                self._json({"status": "ok", "count": len(library), "tasks_generated": task_count, "new_items": new_items})
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
            for k in ("tmdb", "omdb", "tvdb", "opensubs", "opensubs_user", "opensubs_pass", "agent_token", "incoming_path", "staging_paths", "sub_language", "audio_language", "llm_url", "llm_token", "webhook_url"):
                v = params.get(k, [""])[0]
                if v: existing[k] = v.strip()
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

def _supervised(name, fn, interval):
    """Run a scheduled task with supervision — restart on crash, log errors."""
    time.sleep(10 + hash(name) % 20)
    while True:
        try:
            fn()
        except Exception as e:
            print(f"[scheduler] {name} error: {e}")
        time.sleep(interval)

def _sched_enrichment():
    global _enrichment_running
    _enrichment_running = True
    try:
        print("[scheduler] enrichment")
        enrich_titles(fast=False)
    finally:
        _enrichment_running = False

_enrichment_running = False

def _sched_alt_titles():
    tmdb_key = _load_key("tmdb") or TMDB_KEY
    if not tmdb_key: return
    # Skip if enrichment is actively running (it hogs TMDB rate limit)
    if _enrichment_running: return
    titles_db = load_titles()
    need = [iid for iid, t in titles_db.items() if t.get("tmdb_id") and t["tmdb_id"] and "alt_titles" not in t]
    if not need: return
    for iid in need[:5]:
        t = titles_db[iid]
        kind = "tv" if _is_tv(t) else "movie"
        alt = api_get(f"https://api.themoviedb.org/3/{kind}/{t['tmdb_id']}/alternative_titles?api_key={tmdb_key}")
        if alt:
            alt_list = alt.get("titles") or alt.get("results") or []
            t["alt_titles"] = [a["title"] for a in alt_list if a.get("title")][:20]
    save_titles(titles_db)
    remaining = len(need) - 5
    if remaining % 1000 < 5: print(f"[scheduler] alt titles: {remaining} remaining")

def _sched_catalog():
    import datetime
    now = datetime.datetime.now()
    if now.weekday() == 6 and now.hour == 4:
        print("[scheduler] catalog refresh")
        fetch_streaming_catalog()

def _sched_discovery():
    import datetime
    now = datetime.datetime.now()
    if now.weekday() == 6 and now.hour == 5:
        print("[scheduler] discovery sweep")
        _discover_highly_rated()


def _sched_verification():
    """Pipeline: pick unverified files, queue validate_match and identify_movie tasks."""
    db = get_db()
    # Ensure table exists
    db.execute("""CREATE TABLE IF NOT EXISTS verification (
        path TEXT, step TEXT, imdb_id TEXT,
        status TEXT, result TEXT, version INTEGER DEFAULT 1, ts TEXT, PRIMARY KEY (path, step))""")
    db.commit()
    
    titles = load_titles()
    users = [u for u in list_users() if load_user_ratings(u)]
    if not users: return
    user = users[0]
    library = load_user_tmm(user)
    
    # Find files not yet verified at current version
    verified = set(r[0] for r in db.execute(
        "SELECT path FROM verification WHERE version >= ? AND step='duration'", (VERIFY_VERSION,)).fetchall())
    
    # Pick candidates: have path + file_size, not yet verified
    candidates = []
    for iid, info in library.items():
        if iid.startswith("_"): continue
        t = titles.get(iid, {})
        runtime = int(t.get("runtime", 0) or 0)
        if not runtime: continue
        entries = info if isinstance(info, list) else [info] if isinstance(info, dict) else []
        for e in entries:
            if not isinstance(e, dict): continue
            path = e.get("path", "")
            if path and e.get("file_size", 0) > 50000000 and path not in verified:
                candidates.append({"path": path, "imdb_id": iid, "expected_runtime": runtime})
    
    if not candidates:
        return
    
    # Queue batch of 100 per cycle
    batch = candidates[:100]
    tid = f"verify_{int(time.time()*1000)}"
    db_enqueue_task("validate_match", {"items": batch}, 20)
    print(f"[verification] Queued {len(batch)} files ({len(candidates)} remaining)")


def _sched_reconcile():
    """Flush agent_data into library JSON every 10 min. Re-verify oldest 20 hourly. Regenerate tasks every 6h."""
    for user in list_users():
        n = reconcile_agent_data(user)
        if n: print(f"[scheduler] reconciled {n} titles for {user}")
    # Every ~hour (6th call at 600s interval), re-queue 20 oldest entries for re-verification
    if not hasattr(_sched_reconcile, "_count"): _sched_reconcile._count = 0
    _sched_reconcile._count += 1
    if _sched_reconcile._count % 6 == 0:
        db = get_db()
        # Oldest file_size entries (NULL updated_at sorts first, then oldest)
        oldest = db.execute("""SELECT imdb_id FROM agent_data WHERE user='ecb' AND field='file_size'
            ORDER BY COALESCE(updated_at, '2000-01-01') LIMIT 20""").fetchall()
        if oldest:
            library = load_user_tmm("ecb")
            paths = []
            iids = []
            for row in oldest:
                iid = row[0]
                if iid not in library: continue
                entries = library[iid] if isinstance(library[iid], list) else [library[iid]]
                for e in entries:
                    if isinstance(e, dict) and e.get("path"):
                        paths.append(e["path"])
                        iids.append(iid)
                        break
            if paths:
                db_enqueue_task("size_files", {"paths": paths[:20], "imdb_ids": iids[:20]}, PRIORITY_SUBS)
                print(f"[scheduler] re-verify: queued 20 oldest sizes")
    # Every ~6 hours (36th call), regenerate full task queue if it's nearly empty
    if _sched_reconcile._count % 36 == 0:
        db = get_db()
        pending = db.execute("SELECT count(*) FROM task_queue WHERE status='pending'").fetchone()[0]
        if pending < 50:
            for user in list_users():
                n = generate_tasks_for_library(user)
                if n: print(f"[scheduler] regenerated {n} tasks for {user}")
        else:
            print(f"[scheduler] skip regen: {pending} tasks still pending")

def _sched_trakt():
    """Refresh Trakt watch history every 6 hours."""
    for user in list_users():
        try:
            trakt_fetch_history(user)
            print(f"[scheduler] trakt history refreshed for {user}")
        except Exception as e:
            print(f"[scheduler] trakt failed: {e}")

def _sched_simkl():
    """Refresh Simkl watch history every 6 hours."""
    for user in list_users():
        if not load_user_simkl_token(user): continue
        try:
            history = simkl_fetch_history(user)
            if history:
                existing = load_user_history(user)
                seen = {h["id"] for h in existing}
                for h in history:
                    if h["id"] not in seen: existing.append(h)
                save_user_history(user, existing)
            print(f"[scheduler] simkl history refreshed for {user}")
        except Exception as e:
            print(f"[scheduler] simkl failed: {e}")

def _sched_anilist():
    """Refresh AniList anime ratings every 6 hours."""
    for user in list_users():
        if not load_user_anilist_token(user): continue
        try:
            ar = anilist_fetch_ratings(user)
            if ar:
                ratings = load_user_ratings(user)
                titles = load_titles()
                for iid, r in ar.items():
                    if iid not in ratings: ratings[iid] = r
                    if iid not in titles: titles[iid] = {"title": "", "_enriched": False}
                save_user_ratings(user, ratings); save_titles(titles)
            print(f"[scheduler] anilist refreshed {len(ar)} ratings for {user}")
        except Exception as e:
            print(f"[scheduler] anilist failed: {e}")

def _scheduler():
    """Supervised scheduler — each task independent, staggered, crash-resilient."""
    tasks = [
        ("enrichment", _sched_enrichment, 7200),
        ("alt_titles", _sched_alt_titles, 3),
        ("catalog", _sched_catalog, 600),
        ("discovery", _sched_discovery, 600),
        ("verification", _sched_verification, 300),
        ("reconcile", _sched_reconcile, 600),
        ("trakt", _sched_trakt, 21600),
        ("simkl", _sched_simkl, 21600),
        ("anilist", _sched_anilist, 21600),
    ]
    threads = []
    for name, fn, interval in tasks:
        t = threading.Thread(target=_supervised, args=(name, fn, interval), daemon=True)
        t.start()
        threads.append(t)
    print(f"[scheduler] Started {len(tasks)} supervised tasks")
    for t in threads: t.join()

def _migrate_json_to_db():
    """One-time migration of JSON files to SQLite."""
    db = get_db()
    # Migrate task queue
    if db.execute("SELECT COUNT(*) FROM task_queue").fetchone()[0] == 0:
        tq = safe_json_load(os.path.join(DATA_DIR, "task_queue.json")) or []
        for t in tq:
            try:
                db.execute("INSERT OR IGNORE INTO task_queue (id,type,params,priority,status,created,completed,result) VALUES (?,?,?,?,?,?,?,?)",
                    (t.get("id",""), t.get("type",""), json.dumps(t.get("params",{})), t.get("priority",0),
                     t.get("status","pending"), t.get("created",""), t.get("completed"),
                     json.dumps(t.get("result")) if t.get("result") else None))
            except: pass
        db.commit()
        if tq: print(f"  Migrated {len(tq)} tasks to SQLite")

    # Migrate agent_data
    if db.execute("SELECT COUNT(*) FROM agent_data").fetchone()[0] == 0:
        for user in list_users():
            ad = safe_json_load(os.path.join(DATA_DIR, "users", user, "agent_data.json")) or {}
            count = 0
            for iid, fields in ad.items():
                if isinstance(fields, dict):
                    for field, value in fields.items():
                        db.execute("INSERT OR IGNORE INTO agent_data (user,imdb_id,field,value) VALUES (?,?,?,?)",
                            (user, iid, field, str(value)))
                        count += 1
            db.commit()
            if count: print(f"  Migrated {count} agent_data entries for {user}")

    # Migrate enrichment log
    if db.execute("SELECT COUNT(*) FROM enrichment_log").fetchone()[0] == 0:
        elog = safe_json_load(os.path.join(DATA_DIR, "enrichment_log.json")) or []
        for e in elog:
            try:
                db.execute("INSERT INTO enrichment_log (imdb_id,title,year,ts,changes) VALUES (?,?,?,?,?)",
                    (e.get("iid",""), e.get("title",""), e.get("year",""), e.get("ts",""), json.dumps(e.get("changes",{}))))
            except: pass
        db.commit()
        if elog: print(f"  Migrated {len(elog)} enrichment log entries")

    # Migrate incoming
    if db.execute("SELECT COUNT(*) FROM incoming").fetchone()[0] == 0:
        for user in list_users():
            inc = safe_json_load(os.path.join(DATA_DIR, "users", user, "incoming.json")) or []
            for f in inc:
                try:
                    db.execute("INSERT OR IGNORE INTO incoming (user,path,filename,size,title_guess,year_guess,quality,tmdb_match,status) VALUES (?,?,?,?,?,?,?,?,?)",
                        (user, f.get("path",""), f.get("filename",""), f.get("size",0),
                         f.get("title_guess",""), f.get("year_guess",""), f.get("quality",""),
                         json.dumps(f.get("tmdb_match")) if f.get("tmdb_match") else None,
                         f.get("status","pending")))
                except: pass
            db.commit()
            if inc: print(f"  Migrated {len(inc)} incoming entries for {user}")

if __name__ == "__main__":
    os.makedirs(DATA_DIR, exist_ok=True)
    if os.path.exists(KEYS_FILE):
        keys = json.load(open(KEYS_FILE))
        TMDB_KEY = keys.get("tmdb", TMDB_KEY).strip()
        OMDB_KEY = keys.get("omdb", OMDB_KEY).strip()
        TVDB_KEY = keys.get("tvdb", TVDB_KEY).strip()
        AGENT_TOKEN = keys.get("agent_token", AGENT_TOKEN)
    init_db()
    try: init_fts()
    except: pass
    # Migrate JSON data to SQLite if needed
    _migrate_json_to_db()
    migrate_old_data()
    users = list_users()
    titles = load_titles()
    load_imdb_cache()
    print(f"CineCross — {len(titles)} titles, users: {users}")
    print(f"  TMDB:{'✓' if TMDB_KEY else '✗'} OMDB:{'✓' if OMDB_KEY else '✗'} TVDB:{'✓' if TVDB_KEY else '✗'} Trakt:{'✓' if TRAKT_ID else '✗'} Simkl:{'✓' if SIMKL_ID else '✗'} AniList:{'✓' if ANILIST_ID else '✗'} Region:{WATCH_COUNTRY}")
    threading.Thread(target=_scheduler, daemon=True).start()
    class ThreadedServer(ThreadingMixIn, HTTPServer):
        daemon_threads = True
    ThreadedServer(("0.0.0.0", PORT), H).serve_forever()
