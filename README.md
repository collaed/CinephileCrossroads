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
- Searchable, sortable, filterable by genre, rating, and streaming provider
- Hover titles for plot summaries
- 💾 indicator for titles in your local library (TMM integration)

### 📺 Streaming Availability
- See which titles are on Netflix, Prime, Disney+, Max, etc. in your country
- Filter your ratings by streaming provider
- Browse the full streaming catalog for your region
- Powered by TMDB watch providers (supports 50+ countries)

### 🎯 Taste-Based Recommendations
- Builds a weighted taste profile from your highly-rated titles using TMDB keywords and genres
- Inspired by Jinni's "Movie Genome" approach — goes beyond genres to match on themes, moods, and plot elements
- Scores all unrated titles against your profile
- Filtered to what's available on your streaming services
- "What should I watch tonight?" — answered

### 👥 Multi-User Support
- Shared title metadata store (posters, scores, keywords, streaming) — ~2MB for 1000 titles
- Per-user ratings, Trakt tokens, and local libraries — ~20KB per user
- Adding a new user is a marginal cost of a few KB

### 🔄 Integrations
| Service | What it provides | Auth |
|---------|-----------------|------|
| **IMDB** | Ratings import (CSV) | None |
| **TMDB** | Posters, keywords, streaming availability, similar titles | API key |
| **OMDB** | Rotten Tomatoes 🍅, Metacritic scores | API key |
| **TVDB** | TV show metadata | API key |
| **Trakt** | Bidirectional rating sync, watch history | OAuth |
| **TMM** | Local library indicator (tinyMediaManager) | CSV/file upload |

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

Keys can be set via environment variables or pasted in the ⚙ Setup page at runtime (saved to `/data/api_keys.json`).

## Architecture

```
/data/
├── titles.json              # Shared: metadata for all known titles (~2MB/1000 titles)
├── catalog.json             # Streaming catalog for WATCH_COUNTRY
├── api_keys.json            # Saved API keys
├── tvdb_token.json          # TVDB session token
└── users/
    ├── alice/
    │   ├── ratings.json     # {imdb_id: {rating, date}} (~20KB)
    │   ├── trakt_token.json # Trakt OAuth token
    │   └── tmm_library.json # Local library IDs
    └── bob/
        └── ratings.json
```

### Data Flow

```
IMDB CSV ──→ import_csv() ──→ titles.json (shared metadata)
                            └→ users/X/ratings.json (personal ratings)

Enrich ──→ TMDB (poster, keywords, streaming, similar)
        ├→ OMDB (RT, Metacritic)
        └→ TVDB (TV cross-ref)
        ──→ titles.json (updated)

Recommend ──→ build_taste_profile() from user's high-rated titles
           ├→ score all unrated titles against profile
           ├→ filter by streaming availability
           └→ return top matches
```

### Recommendation Algorithm

1. **Taste profile**: For each title rated 6+, extract TMDB keywords and genres. Weight by rating: `(rating - 5) / 5` — so a 10/10 contributes 5x more than a 6/10.

2. **Scoring**: Each candidate title is scored by summing keyword matches (full weight) and genre matches (half weight), then boosted by critical ratings (IMDB/TMDB).

3. **Filtering**: Only titles available on the user's streaming services in their country are shown.

4. **Smart enrichment**: Titles with poor metadata are re-enriched first. Richness score (0-8) determines priority.

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

### Reverse Proxy

Behind Caddy with a subpath:

```
handle_path /movies/* {
    reverse_proxy cinephile:8000
}
```

Set the `BASE` constant in `app.py` to match your subpath (default: `/imdb`).

### URL Routes

| Route | Description |
|-------|-------------|
| `/` | Default user's ratings |
| `/u/<user>` | Specific user's ratings |
| `/recs/<user>` | Recommendations for user |
| `/catalog` | Streaming catalog browser |
| `/setup/<user>` | User setup (import, API keys, Trakt) |
| `/setup/new` | Create new user |
| `/enrich` | Trigger background enrichment |
| `/trakt/sync/<user>` | Sync ratings with Trakt |
| `/jobs` | Background job status (JSON) |
| `/api` | Stats endpoint (JSON) |

## Tech Stack

- **Zero dependencies** — pure Python 3.12 standard library
- **Single file** — `app.py` (~750 lines)
- **~50MB Docker image** (python:alpine)
- **Background job queue** with progress tracking
- **Incremental saves** during enrichment (no data loss on interruption)

## Roadmap

- [ ] Taste.io / TasteDive integration for "if you liked X" recommendations
- [ ] Episode tracking for TV shows
- [ ] Watchlist management
- [ ] Export ratings to CSV
- [ ] Dark/light theme toggle
- [ ] Mobile-optimized layout

## License

MIT
