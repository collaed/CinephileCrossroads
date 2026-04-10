# 🎬 CinephileCrossroads

A self-hosted, zero-dependency movie & TV ratings dashboard that aggregates data from multiple sources, shows streaming availability, and provides taste-based recommendations.

![Python](https://img.shields.io/badge/python-3.12-blue) ![Docker](https://img.shields.io/badge/docker-ready-blue) ![License](https://img.shields.io/badge/license-MIT-green) ![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen)

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/template/new?repo=collaed/CinephileCrossroads) [![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/collaed/CinephileCrossroads)

<!-- Add a screenshot: replace this with an actual screenshot URL -->
<!-- ![Screenshot](https://your-domain.com/screenshot.png) -->

## Features

### 📊 Ratings Dashboard
- Import IMDB ratings (CSV export)
- Enrich with posters, plot summaries, and scores from TMDB, OMDB, and TVDB
- Searchable, sortable, filterable by genre, rating, streaming provider, and decade
- Hover titles for plot summaries
- 💾 indicator for titles in your local library (Plex, Jellyfin, Emby, Kodi, Radarr, Sonarr, TMM)
- Leaving soon detection — tracks titles disappearing from streaming services
- 🏆 Awards badge on Oscar winners
- ▶️ Trailer links (YouTube via TMDB)
- 🔗 TasteDive "similar" suggestions
- 🌓 Dark/light theme toggle
- 📱 Mobile-responsive layout

### 📺 Streaming Availability
- See which titles are on Netflix, Prime, Disney+, Max, etc. in your country
- Filter your ratings by streaming provider
- Browse the full streaming catalog for your region
- Per-user provider selection (pick your subscriptions)
- 🆕 "New on streaming" — detect titles that just appeared
- Powered by TMDB watch providers (supports 50+ countries)

### 🎯 Taste-Based Recommendations
- Builds a weighted taste profile from your highly-rated titles using TMDB keywords and genres
- Inspired by Jinni's "Movie Genome" approach — goes beyond genres to match on themes, moods, and plot elements
- **5 recommendation categories:**
  - 🧬 **DNA** — keyword/genre deep match (your taste fingerprint)
  - 🎬 **Director's Chair** — from creators and actors you love
  - 👥 **Community** — loved by users with similar taste (via TMDB similar)
  - ✅ **Unanimous Hits** — high scores across all platforms, no divergence
  - 💫 **Blast from the Past** — favorites you haven't seen in 2+ years
- Score divergence detector — flags titles where IMDB/TMDB/RT/Metacritic disagree (likely manipulation)
- Filtered to what's available on your streaming services
- 🎲 "What should I watch tonight?" — random pick with trailer embed
- ★ Inline rating — rate directly from the recommendations page
- ❤️ Watchlist toggle on recommendations

### 📊 Stats & Social
- Rating distribution, genre breakdown, top directors
- 👥 Compare users — agreement/disagreement analysis
- 📡 RSS feed of recent ratings
- ⬇ Export ratings as CSV

### 👥 Multi-User Support
- Shared title metadata store (posters, scores, keywords, streaming) — ~2MB for 1000 titles
- Per-user ratings, Trakt tokens, and local libraries — ~20KB per user
- Adding a new user is a marginal cost of a few KB
- Profile switcher bar on every page — no passwords needed

### 🔄 Integrations
| Service | What it provides | Auth |
|---------|-----------------|------|
| **IMDB** | Ratings import (CSV) + bulk datasets (200K titles) | None |
| **TMDB** | Posters, keywords, streaming, similar, trailers, cast | API key |
| **OMDB** | Rotten Tomatoes 🍅, Metacritic scores | API key |
| **TVDB** | TV show metadata | API key |
| **Trakt** | Bidirectional rating sync, watch history | OAuth |
| **TasteDive** | "If you liked X" similar suggestions | None (free) |
| **OpenSubtitles** | Subtitle search with hash matching | API key |
| **Plex** | Local library sync (resolution, codecs, audio, subs) | URL + token |
| **Jellyfin** | Local library sync (full media info) | URL + API key |
| **Emby** | Local library sync (full media info) | URL + API key |
| **Kodi** | Local library sync (streamdetails) | JSON-RPC URL |
| **Radarr** | Movie library + wanted/downloaded status | URL + API key |
| **Sonarr** | TV library + wanted/downloaded status | URL + API key |
| **TMM** | Local library (tinyMediaManager HTTP API or CSV upload) | API key |

### 🖥️ Media Server Sync

Two ways to sync your local media library:

**Browser LAN Scan** — click "Scan LAN" in Setup. Your browser probes common LAN IPs for media servers, prompts for API tokens, and syncs directly. No server-side config needed.

**LAN Agent** (`agent.py`) — a standalone zero-dependency Python script for unattended sync:
```bash
# First run creates agent.json config
python3 agent.py --server https://your-domain.com/imdb --user yourname

# Edit agent.json with your server URLs and tokens, then:
python3 agent.py --server https://your-domain.com/imdb --user yourname

# Automate with cron (every 30 min):
*/30 * * * * python3 /path/to/agent.py --server https://your-domain.com/imdb --user yourname
```

#### Running the Agent on Windows

1. Install Python 3.12+ from [python.org](https://www.python.org/downloads/) (check "Add to PATH")
2. Download `agent.py` from this repo
3. Open PowerShell and run:
```powershell
# First run — creates agent.json config file
python agent.py --server https://your-domain.com/imdb --user yourname

# Edit agent.json in Notepad — enable your servers, add URLs and tokens:
notepad agent.json

# Run the sync
python agent.py --server https://your-domain.com/imdb --user yourname
```
4. To automate, create a scheduled task:
```powershell
# Open Task Scheduler or run:
schtasks /create /tn "CineCross Sync" /tr "python C:\path\to\agent.py --server https://your-domain.com/imdb --user yourname" /sc hourly
```

#### Windows Prerequisites (optional)

For file sizes, hashes, and video thumbnails, run the setup script as Administrator:
```powershell
powershell -ExecutionPolicy Bypass -File setup-windows.ps1
```
This installs:
- **NFS client** — access NFS shares from Windows (optional if using SMB)
- **ffmpeg** — video thumbnails for library curation

#### Path Mapping (NFS/SMB)

If your media server (Kodi) uses NFS paths but your Windows machine accesses them via SMB, configure path mappings in `agent.json`:
```json
{
    "_path_mappings": {
        "nfs://192.168.0.235/volume1/Movies": "//zeus/Movies",
        "nfs://192.168.0.235/volume1/TVShows": "//zeus/TVShows"
    }
}
```
The agent auto-converts forward slashes to backslashes on Windows. On first run, it checks if paths are accessible and prompts to set up mappings interactively.

The agent supports: Plex, Jellyfin, Emby, Kodi, Radarr, Sonarr, and tinyMediaManager. It computes OpenSubtitles file hashes for sync-accurate subtitle matching.

## Quick Start

```bash
git clone https://github.com/collaed/CinephileCrossroads.git
cd CinephileCrossroads
docker compose up -d
```

Open `http://localhost:8000`, go to ⚙ Setup, and upload your IMDB CSV export.

### With API keys (recommended)

```yaml
# docker-compose.override.yml
services:
  cinephile:
    environment:
      - TMDB_KEY=your_tmdb_key
      - OMDB_KEY=your_omdb_key
      - TVDB_KEY=your_tvdb_key
      - TRAKT_ID=your_trakt_client_id
      - TRAKT_SECRET=your_trakt_client_secret
      - TRAKT_REDIRECT=https://your-domain.com/trakt/callback
      - WATCH_COUNTRY=LU
```

## API Keys

All optional — the app works with just an IMDB CSV. Each key unlocks more features:

| Service | Get a key | What it unlocks |
|---------|-----------|----------------|
| **TMDB** | [themoviedb.org/settings/api](https://www.themoviedb.org/settings/api) | Posters, keywords, streaming, recommendations |
| **OMDB** | [omdbapi.com/apikey.aspx](https://www.omdbapi.com/apikey.aspx) | Rotten Tomatoes, Metacritic (1000 req/day free) |
| **TVDB** | [thetvdb.com/dashboard/account/apikey](https://thetvdb.com/dashboard/account/apikey) | TV show cross-referencing |
| **Trakt** | [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications) | Rating sync, watch history |
| **OpenSubtitles** | [opensubtitles.com/consumers](https://www.opensubtitles.com/consumers) | Subtitle search + download |

Keys can be set via environment variables or pasted in the ⚙ Setup page at runtime (saved to `/data/api_keys.json`).

## Architecture

```
/data/
├── titles.json              # Shared: metadata for all known titles (~2MB/1000 titles)
├── catalog.json             # Streaming catalog for WATCH_COUNTRY
├── catalog_prev.json        # Previous catalog snapshot (for "leaving soon" detection)
├── api_keys.json            # Saved API keys
├── tvdb_token.json          # TVDB session token
├── imdb_datasets/           # IMDB bulk data (title.basics.tsv, title.ratings.tsv)
└── users/
    ├── alice/
    │   ├── ratings.json     # {imdb_id: {rating, date}} (~20KB)
    │   ├── trakt_token.json # Trakt OAuth token
    │   ├── tmm_library.json # Local library (TMM + media servers)
    │   ├── media_servers.json # Media server connection config
    │   ├── providers.json   # Streaming subscriptions
    │   └── watchlist.json   # Watchlisted titles
    └── bob/
        └── ratings.json
```

### Data Flow

```
IMDB CSV ──→ import_csv() ──→ titles.json (shared metadata)
                            └→ users/X/ratings.json (personal ratings)

Enrich ──→ TMDB (poster, keywords, streaming, similar, cast, trailer)
        ├→ OMDB (RT, Metacritic)
        └→ TVDB (TV cross-ref)
        ──→ titles.json (updated)

Recommend ──→ build_taste_profile() from user's high-rated titles
           ├→ score all unrated titles against profile
           ├→ filter by streaming availability
           └→ return top matches in 5 categories

Media Sync ──→ Browser LAN scan OR agent.py
            ├→ Plex / Jellyfin / Emby / Kodi / Radarr / Sonarr / TMM
            └→ POST /api/library/<user> ──→ users/X/tmm_library.json

Catalog ──→ TMDB discover API (per provider, per country)
         ├→ catalog.json (current snapshot)
         ├→ catalog_prev.json (previous, for diff)
         ├→ "leaving soon" = titles in prev but not in current
         └→ seeds titles.json with unrated titles for recommendations

Discovery ──→ Weekly: TMDB 8.0+ movies in EN/FR/PT/ES
           └→ seeds titles.json with highly-rated films
```

### Recommendation Algorithm

1. **Taste profile**: For each title rated 6+, extract TMDB keywords, genres, directors, and actors. Weight by rating: `(rating - 5) / 5` — so a 10/10 contributes 5x more than a 6/10.

2. **Scoring**: Each candidate title is scored by summing keyword matches (full weight) and genre matches (half weight), boosted by critical ratings (IMDB/TMDB). Cast/director matches add additional weight.

3. **Score validation**: Titles where IMDB/TMDB/RT/Metacritic diverge by >2.0 points are flagged as potentially manipulated and excluded from the "Unanimous Hits" category.

4. **Filtering**: Only titles available on the user's streaming services in their country are shown.

5. **FIFO re-enrichment**: Every daily run re-enriches the 50 oldest titles, keeping metadata fresh. OMDB calls capped at 500/run (half of daily quota).

6. **Seasonal boost**: Keywords matching the current season (e.g., "christmas" in December) get a scoring boost.

### Scalability

| Users | Disk | RAM | Architecture |
|---|---|---|---|
| 1-50 | <100 MB | ~200 MB | Current design (JSON files) |
| 50-500 | ~60 MB | ~200 MB | Add gunicorn (4 workers) |
| 500-5000 | ~400 MB | ~250 MB | SQLite for titles (WAL mode) |
| 5000+ | ~800 MB | ~650 MB | PostgreSQL + Redis |

## Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TMDB_KEY` | TMDB API key | |
| `OMDB_KEY` | OMDB API key | |
| `TVDB_KEY` | TVDB API key | |
| `TRAKT_ID` | Trakt OAuth client ID | |
| `TRAKT_SECRET` | Trakt OAuth client secret | |
| `TRAKT_REDIRECT` | Trakt OAuth redirect URI | |
| `WATCH_COUNTRY` | ISO 3166-1 country code | `LU` |

### Scheduled Jobs

| Schedule | Job | Description |
|---|---|---|
| Daily 3am | Enrichment | Keywords, posters, cast, streaming for new/stale titles |
| Sunday 4am | Catalog refresh | Full streaming catalog + seeding |
| Sunday 5am | Discovery | TMDB 8.0+ movies in EN/FR/PT/ES |

### Reverse Proxy

Behind Caddy with a subpath:

```
handle_path /movies/* {
    reverse_proxy cinecross:8000
}
```

Set the `BASE` constant in `app.py` to match your subpath (default: `/imdb`).

### URL Routes

| Route | Auth | Description |
|-------|------|-------------|
| `/` | Public | Default user's ratings |
| `/u/<user>` | Public | Specific user's ratings |
| `/recs/<user>` | Public | 5-category recommendations |
| `/tonight/<user>` | Public | Random pick with trailer |
| `/stats/<user>` | Public | Rating stats and charts |
| `/catalog` | Public | Streaming catalog browser |
| `/new` | Public | New on streaming |
| `/random/<user>` | Public | Random unwatched title |
| `/compare/` | Public | Compare users |
| `/rss/<user>` | Public | RSS feed |
| `/similar/<id>` | Public | TasteDive similar titles |
| `/export/<user>` | Public | Download ratings CSV |
| `/setup/<user>` | 🔒 Login | Setup & configuration |
| `/enrich` | 🔒 Login | Trigger enrichment |
| `/keys` | 🔒 Login | Save API keys |
| `/trakt/sync/<user>` | 🔒 Login | Sync with Trakt |
| `/media/sync/<user>` | 🔒 Login | Sync media servers |
| `/datasets/download` | 🔒 Login | Download IMDB datasets |
| `/api/library/<user>` | Public | POST: receive library from agent |
| `/jobs` | Public | Background job status (JSON) |
| `/api` | Public | Stats endpoint (JSON) |

## Tech Stack

- **Zero dependencies** — pure Python 3.12 standard library
- **Single file** — `app.py` (~2000 lines) + optional `agent.py` (~150 lines)
- **~50MB Docker image** (python:alpine)
- **IMDB bulk datasets** — 200K titles loaded in memory for instant lookups
- **Background job queue** with progress tracking
- **Incremental saves** during enrichment (no data loss on interruption)
- **Browser-side LAN scanning** for media server discovery
- **FIFO re-enrichment** — 50 oldest titles refreshed daily
- **Zero API calls** for anonymous users (all public pages serve cached data)
- **Multi-user** with minimal per-user overhead (~75KB)

# Roadmap - v2

### Resident Agent + Server Task Queue
- Agent daemon mode, server-controlled task queue
- Priority: duplicates > quality > subs > rest  
- Dry-run mode, trash folder, undo log

### Movie Scraper (TMM Replacement)
- Identify movies from filenames via IMDB dataset + TMDB
- Runtime validation, background auto-matching + human review queue
- Write NFO, download artwork, smart rename, 3D detection (SBS/HSBS/TAB/MVC)

### Library Organization
- Auto-organize by genre/decade/rating, merge across drives, orphan detection

### TV Show Intelligence
- Episode gaps, quality consistency, season completion, next episode prediction

### Recommendations v2
- Collaborative filtering, mood/time-based, seasonal boost, anti-recommendation

### Streaming + Social + Technical
- Trakt scrobbling, GDPR CSV import, available/leaving alerts
- Shared watchlist, activity feed, taste compatibility
- SQLite migration, WebSocket, plugin system, multi-instance federation

## License

MIT
