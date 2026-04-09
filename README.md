# 🎬 CinephileCrossroads

A self-hosted, single-file movie & TV ratings dashboard that aggregates data from multiple sources into one searchable interface.

![Python](https://img.shields.io/badge/python-3.12-blue) ![Docker](https://img.shields.io/badge/docker-ready-blue) ![License](https://img.shields.io/badge/license-MIT-green)

## What it does

- **Import** your IMDB ratings (CSV export)
- **Enrich** with posters, plot summaries, and scores from TMDB, OMDB, and TVDB
- **Stream availability** — see which titles are on Netflix, Prime, Disney+, etc. in your country
- **Sync with Trakt** — push/pull ratings bidirectionally
- **Local library** — import your tinyMediaManager (TMM) library to see what you own
- **Streaming catalog** — browse the full catalog of your streaming services, sorted by rating
- **Search, filter, sort** — by genre, rating, streaming provider, and more

## Screenshots

```
🎬 My Ratings — 507 titles
[Search...] [All genres ▼] [Min ★ ▼] [All streams ▼] ⚡ Enrich 📺 Catalog ⚙

     Title              Year  ★   IMDB  Scores         Stream    Genres
🖼️  Vigil               2021  9   7.4   🍅89% Ⓜ72    🟥        Thriller, Crime
🖼️  What's Cooking...   1966  10  6.7                  📦        Action, Comedy
🖼️  The Outsider        2020  8   7.4   🍅82%         🟥 🏰     Crime, Drama
```

## Quick Start

```bash
git clone https://github.com/collaed/CinephileCrossroads.git
cd CinephileCrossroads
cp docker-compose.yml docker-compose.override.yml
# Edit docker-compose.override.yml with your API keys
docker compose up -d
```

Then open `http://localhost:8000` and upload your IMDB CSV export.

## API Keys

All optional — the app works with just an IMDB CSV. Each key unlocks more features:

| Service | What it adds | Get a key |
|---------|-------------|-----------|
| **TMDB** | Posters, plot summaries, TMDB ratings, streaming availability | [themoviedb.org/settings/api](https://www.themoviedb.org/settings/api) |
| **OMDB** | Rotten Tomatoes 🍅, Metacritic scores | [omdbapi.com/apikey.aspx](https://www.omdbapi.com/apikey.aspx) |
| **TVDB** | Additional TV show metadata | [thetvdb.com/dashboard/account/apikey](https://thetvdb.com/dashboard/account/apikey) |
| **Trakt** | Bidirectional rating sync, watch history | [trakt.tv/oauth/applications](https://trakt.tv/oauth/applications) |

Keys can be set via environment variables or pasted in the ⚙ Setup page at runtime.

## Configuration

### Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `IMDB_UR` | Your IMDB user ID (ur format) | `ur1234567` |
| `TMDB_KEY` | TMDB API key | |
| `OMDB_KEY` | OMDB API key | |
| `TVDB_KEY` | TVDB API key | |
| `TRAKT_ID` | Trakt OAuth client ID | |
| `TRAKT_SECRET` | Trakt OAuth client secret | |
| `TRAKT_REDIRECT` | Trakt OAuth redirect URI | `https://your-domain.com/trakt/callback` |
| `WATCH_COUNTRY` | ISO country code for streaming | `LU`, `US`, `GB`, `DE`, `FR`... |

### Finding your IMDB user ID

Your profile URL looks like `imdb.com/user/p.xxxxx/`. The app needs the `ur` ID. Go to ⚙ Setup — it will resolve it automatically, or use the IMDB GraphQL API:

```
userProfile(input: {profileId: "p.xxxxx"}) { userId }
```

### Reverse Proxy (Caddy example)

If running behind a reverse proxy under a subpath:

```
handle_path /movies/* {
    reverse_proxy cinephile:8000
}
```

The app uses the `BASE` constant (default `/imdb`) for all internal links. Adjust in `app.py` if your subpath differs.

## Features

### Ratings Table
- Sortable columns (click headers)
- Filter by genre, minimum rating, streaming provider
- Full-text search
- Hover titles for plot summaries
- 💾 icon for titles in your local library

### Streaming Catalog (`/catalog`)
- Full browsable catalog of movies & TV shows available on your streaming services
- Filtered by your country
- Shows which titles you've already rated

### TMM Integration
Upload a tinyMediaManager CSV export or a plain text file with IMDB IDs (one per line) via ⚙ Setup. Matched titles get a 💾 local indicator.

### Trakt Sync
- Pushes all your IMDB ratings to Trakt
- Pulls any Trakt-only ratings back
- OAuth flow — click "Connect Trakt" in the UI

## Tech Stack

- **Zero dependencies** — pure Python 3.12 standard library
- **Single file** — `app.py` (~700 lines)
- **~50MB Docker image** (python:alpine)
- **Persistent data** in `/data` volume (ratings, API keys, tokens)

## License

MIT
