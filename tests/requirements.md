# CinephileCrossroads Testing Requirements

## User Stories

### US-01: First-time Setup
As a new user, I want to upload my IMDB CSV and see my ratings dashboard so I can start exploring.
- Upload CSV at /setup/<user>
- Ratings page shows all imported titles with posters
- Enrichment starts automatically

### US-02: Browse Ratings
As a user, I want to search, filter, and sort my ratings by genre, decade, rating, and streaming provider.
- Search by title
- Filter by genre, min rating, decade, streaming service, video source
- Sort by any column header
- Mood picker filters by theme

### US-03: Discover Recommendations
As a user, I want taste-based recommendations split into categories so I can find what to watch.
- 5 categories: DNA, Director's Chair, Community, Unanimous, Blast from Past
- Poster grid with match scores
- Taste profile visible in collapsible section

### US-04: AI Friend Insights
As a user, I want personalized insights about my library.
- Hidden Gems: unrated titles matching my taste
- Why Do I Have This: low taste + low IMDB
- Guilty Pleasures: I loved it, critics hated it
- Directors You Love

### US-05: Library Dashboard
As a user, I want to see my library health at a glance.
- Progress bars: sized, hashed, subtitles, media info
- Agent status with version and uptime
- Task queue with ETA
- Resolution and codec breakdown

### US-06: Incoming Files
As a user, I want new downloads identified and organized.
- Scan incoming folder for video files
- Auto-match via TMDB (movies + TV shows)
- Split view: Movies vs TV Episodes
- Duplicate detection with quality comparison
- Import or Delete buttons

### US-07: Title Matching
As a user, I want mismatched titles detected and fixable.
- Fuzzy matching with transliterations
- TMDB alternative titles in 15+ languages
- Confirm or Re-match buttons
- Confirmed titles excluded from future checks

### US-08: Contribute Data
As a user, I want to compare my data with external databases.
- TMDB/TVDB/Wikidata comparison
- Pull alt titles with one click
- Gap analysis showing what I can contribute

### US-09: Multi-user
As a household, we want separate ratings but shared library.
- Each user has own ratings, watchlist, preferences
- Shared titles.json and library
- User switcher in nav bar

### US-10: Agent Operations
As a power user, I want the agent to manage my library files.
- Size, hash, quality check, subtitle search
- Thumbnail generation for duplicates
- File move/delete with confirmation
- Self-update via wrapper

## Test Matrix

| Page | Route | Auth | Status |
|------|-------|------|--------|
| Ratings | /u/<user> | No | Must show |
| Stats | /stats/<user> | No | Must show |
| Unrated | /unrated/<user> | No | Must show |
| Recommendations | /recs/<user> | No | Must show |
| AI Friend | /ai-friend/<user> | No | Must show |
| Tonight | /tonight/<user> | No | Must show |
| Random | /random/<user> | No | Must show |
| Catalog | /catalog | No | Must show |
| New | /new | No | Must show |
| Updates | /updates | No | Must show |
| Library | /library/<user> | No | Must show |
| Browse | /library/browse/<user> | No | Must show |
| TV Shows | /tvshows/<user> | No | Must show |
| Scraper | /scraper/<user> | No | Must show |
| Organize | /library/org/<user> | No | Must show |
| Confirm | /confirm/<user> | No | Must show |
| Incoming | /incoming/<user> | No | Must show |
| Feed | /feed | No | Must show |
| Compare | /compare/ | No | Must show |
| Alerts | /alerts/<user> | No | Must show |
| Contribute | /contribute/<user> | No | Must show |
| Setup | /setup/<user> | Yes | Must show |
| Title Detail | /title/<imdb_id> | No | Must show |
| API Tasks | /api/tasks | No | JSON |
| API Status | /api | No | JSON |

## Acceptance Criteria

### Navigation
- All pages have Cinephile Crossroads banner
- All pages have top nav with 5 sections
- All pages have contextual sub-nav
- Active section highlighted in nav

### Data Integrity
- Kodi sync does not overwrite agent data (file_size, hash, thumbnail)
- Task regeneration preserves exec_code and priority -1 tasks
- Enrichment changelog tracks all changes
- Alt titles survive enrichment cycles

### Agent
- Wrapper restarts on crash (exit != 0)
- Wrapper restarts on update (exit 42)
- Background tasks don't block foreground
- 5s timeout on SMB file operations
- Offline buffering works
