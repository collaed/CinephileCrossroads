# 🎬 Cinephile Crossroads

A self-hosted, zero-dependency movie & TV ratings dashboard with taste-based recommendations, streaming availability, media server integration, and a LAN agent for library management.

![Python](https://img.shields.io/badge/python-3.12-blue) ![Docker](https://img.shields.io/badge/docker-ready-blue) ![License](https://img.shields.io/badge/license-MIT-green) ![Dependencies](https://img.shields.io/badge/dependencies-zero-brightgreen)

[![Deploy on Railway](https://railway.com/button.svg)](https://railway.com/template/new?repo=collaed/CinephileCrossroads) [![Deploy to Render](https://render.com/images/deploy-to-render-button.svg)](https://render.com/deploy?repo=https://github.com/collaed/CinephileCrossroads)

## Features

### ⭐ Ratings Dashboard
- Import IMDB ratings (CSV export)
- Enrich with posters, plots, scores from TMDB, OMDB, TVDB
- Searchable, sortable, filterable by genre, rating, streaming provider, decade, video source
- 💾 Local library indicator with source icons (💿 Blu-ray, 📀 DVD, 🌐 Web, 💎 Remux)
- 🏆 Awards badges, ▶️ trailer links, 🔗 TasteDive similar
- Mood picker: ☀️ Light, 🔥 Intense, 😂 Funny, 🌀 Mind-Bending, 🌑 Dark, ⚔️ Epic, 💕 Romantic, 👻 Scary, ✨ Inspiring
- 🌓 Dark/light theme, 📱 mobile responsive

### 🎯 Discover
- **Taste-Based Recommendations** — Jinni-inspired "Movie Genome" using TMDB keywords, genres, directors, actors, writers
  - 🧬 DNA Match, 🎬 Director's Chair, 👥 Community, ✅ Unanimous Hits, 💫 Blast from the Past
  - Weighted scoring: keywords 1x, directors 2x, actors 1.5x, writers 1.5x, genres 0.5x
  - Fully-watched unrated TV shows count as implicit 7/10
- **🤖 AI Friend** — personalized insights from your library:
  - 💎 Hidden Gems (unrated titles matching your taste)
  - 🤔 Why Do I Have This? (low taste + low IMDB)
  - 😈 Guilty Pleasures (you loved it, critics hated it)
  - 🎬 Directors You Love
- **🎲 Tonight / 🎰 Random** — quick picks with trailer embeds
- **📺 Streaming Catalog** — browse what's available in your country
- **🆕 New on Streaming** — detect titles that just appeared
- **📋 Enrichment Updates** — changelog of what data changed, with color-coded tags

### 📚 Library Management
- **Library Dashboard** — progress bars for sized/hashed/subtitled/quality-checked files
  - Agent status, task queue with ETA, resolution & codec breakdown
- **📖 Browse** — paginated, searchable, sortable view of all 18K+ library items
- **📺 TV Shows** — episode gap detection, quality consistency, completion tracking, sortable by watched %
- **🔍 Scraper** — filename parsing, TMDB search matching, IMDB dataset proposals, one-click match
- **🗂 Organize** — library breakdown by location, decade, genre
- **⚠ To Be Confirmed** — fuzzy title/filename mismatch detection
  - Checks IMDB title, originalTitle, and TMDB alternative titles (15+ languages)
  - German transliterations (ä→ae, ö→oe, ü→ue), substring matching
  - ✅ Confirm or 🔍 Re-match buttons
- **💾 Save Space** — titles furthest from your taste profile, sorted by match score
- **Duplicate Detection** — side-by-side comparison with thumbnails, resolution, codec, size
  - ✅ KEEP / ❌ REMOVE / ⚖️ REVIEW suggestions
  - Video frame thumbnails extracted via ffmpeg

### 👥 Social
- **📡 Activity Feed** — recent ratings across all users
- **🤝 Compare** — agreement/disagreement analysis between users
- **🔔 Alerts** — watchlisted titles now available on streaming
- **🌍 Contribute** — compare your data vs TMDB/TVDB/Wikidata
  - One-click pull of alternative titles, TV data, multilingual names
  - Data gap analysis showing what you can contribute back

### ⚙ Setup & Integrations

| Service | What it provides | Auth |
|---------|-----------------|------|
| **IMDB** | Ratings import (CSV) + bulk datasets (200K titles) | None |
| **TMDB** | Posters, keywords, streaming, similar, trailers, cast, alt titles | API key |
| **OMDB** | Rotten Tomatoes 🍅, Metacritic scores | API key |
| **TVDB** | TV show metadata | API key |
| **Trakt** | Bidirectional rating sync, watch history, episode scrobbles | OAuth |
| **TasteDive** | "If you liked X" similar suggestions | None |
| **OpenSubtitles** | Subtitle search with hash matching | API key |
| **Plex** | Local library sync | URL + token |
| **Jellyfin** | Local library sync | URL + API key |
| **Emby** | Local library sync | URL + API key |
| **Kodi** | Local library sync (JSON-RPC) | URL |
| **Radarr** | Movie library + wanted status | URL + API key |
| **Sonarr** | TV library + wanted status | URL + API key |
| **TMM** | tinyMediaManager library | API key |

### 🖥️ LAN Agent (`agent.py`)

A standalone zero-dependency Python script for unattended media server sync and library management:

- **Daemon mode** with wrapper (`agent-wrapper.py`) for auto-restart on crash or self-update
- **Threaded execution**: background tasks (exec_code) run alongside foreground tasks (sizing, hashing)
- **Task types**: size_files, hash_files, check_quality, download_subs, find_duplicates, exec_code, update_agent, diag
- **Self-update**: server pushes new agent code, agent writes it and exits 42, wrapper restarts
- **5-second timeout** on SMB file operations to prevent hangs
- **Offline buffering**: failed results saved to `agent_buffer.json`, retried on reconnect
- **NFO scanning**: reads .nfo sidecar files for IMDB IDs, batched reporting
- **Thumbnail extraction**: ffmpeg frame capture for duplicate comparison
- **Path mapping**: NFS ↔ SMB automatic conversion

```bash
# With wrapper (recommended)
python agent-wrapper.py --server https://your-domain.com/cinecross --user yourname --daemon

# Direct
python agent.py --server https://your-domain.com/cinecross --user yourname --daemon
```

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
  cinecross:
    environment:
      - TMDB_KEY=your_tmdb_key
      - OMDB_KEY=your_omdb_key
      - TVDB_KEY=your_tvdb_key
      - TRAKT_ID=your_trakt_client_id
      - TRAKT_SECRET=your_trakt_client_secret
      - TRAKT_REDIRECT=https://your-domain.com/cinecross/trakt/callback
      - WATCH_COUNTRY=US
```

The Docker image includes NFS client for direct media access. See `docker-compose.yml` for NFS volume mount examples.

## Navigation

| Section | Pages |
|---------|-------|
| **⭐ Ratings** | Ratings · Stats · Unrated · History |
| **🎯 Discover** | Recommendations · AI Friend · Tonight · Random · Catalog · New · Updates |
| **📚 Library** | Dashboard · Browse · TV Shows · Scraper · Organize · Confirm |
| **👥 Social** | Feed · Compare · Alerts · Contribute |
| **⚙ Setup** | Config · Trakt · Export · Import · RSS |

## Architecture

```
/data/
├── titles.json              # Shared metadata (~2MB/1000 titles)
├── catalog.json             # Streaming catalog
├── enrichment_log.json      # Change tracking
├── api_keys.json            # Saved API keys
├── thumbnails/              # Video frame captures (.jpg)
├── imdb_datasets/           # title.basics.tsv, title.ratings.tsv
└── users/
    └── alice/
        ├── ratings.json     # Personal ratings (~20KB)
        ├── tmm_library.json # Local library (media servers)
        ├── trakt_token.json # Trakt OAuth token
        ├── providers.json   # Streaming subscriptions
        └── watchlist.json   # Watchlisted titles
```

### Scheduled Jobs

| Schedule | Job |
|---|---|
| Every 2 hours | Enrichment (TMDB keywords/cast/streaming, OMDB RT/Metacritic, capped 80 OMDB/run) |
| Every 5 seconds | Alt titles pull from TMDB (10 titles/cycle, prioritizes confirm mismatches) |
| Every 4 hours | Kodi/media server sync (agent) |
| Sunday 4am | Streaming catalog refresh + discovery sweep |

### Recommendation Algorithm

1. **Taste profile**: For each title rated 6+, extract keywords, genres, directors, actors, writers. Weight by rating: `(rating - 5) / 5`. Fully-watched unrated TV shows count as implicit 7.
2. **Scoring**: keyword match (1x) + genre (0.5x) + director (2x) + actor (1.5x) + writer (1.5x), boosted by IMDB/TMDB ratings.
3. **Seasonal boost**: Keywords matching current season get a scoring boost.
4. **Filtering**: Only titles on user's streaming services in their country.
5. **5 categories**: DNA, Director's Chair, Community (TMDB similar), Unanimous Hits, Blast from the Past.

## Tech Stack

- **Zero dependencies** — pure Python 3.12 standard library
- **Single file** — `app.py` (~4000 lines) + `agent.py` (~1000 lines)
- **~50MB Docker image** (python:alpine + nfs-utils)
- **Thread-safe JSON** with file locking + atomic writes
- **Concurrent requests** via ThreadingMixIn
- **Background job queue** with progress tracking
- **Auto-enrichment** every 2 hours with OMDB rate limiting
- **20K+ title capacity** tested with full library

## License

MIT
