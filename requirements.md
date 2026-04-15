# CinephileCrossroads — Requirements Specification

Version: 2.1 | Generated: 2026-04-15 | Source: `app.py` (4980 LOC), `agent.py` (1284 LOC), `agent-wrapper.py` (26 LOC)

---

## 1. Stakeholders

| Role | Description |
|------|-------------|
| **Cinephile** | Primary user. Imports ratings, browses recommendations, manages library. |
| **Household member** | Secondary user on same instance. Has own ratings, providers, watchlist. |
| **LAN Agent** | Automated daemon on media server. Executes tasks, syncs library. |
| **External APIs** | TMDB, OMDB, TVDB, Trakt, TasteDive, OpenSubtitles, Internet Archive. |

---

## 2. User Stories (EARS Notation)

### 2.1 Ratings Management

**REQ-RAT-001** — Import IMDB CSV
> When the user uploads an IMDB CSV export on the Setup page, the system shall parse the CSV, split data into shared title metadata (`titles.json`) and per-user ratings (`ratings.json`), and redirect to the user's ratings page.

**REQ-RAT-002** — Import Letterboxd CSV
> When the user uploads a Letterboxd CSV (diary.csv or ratings.csv) on the Setup page, the system shall parse the CSV, match titles to IMDB IDs via TMDB search, convert Letterboxd 0.5–5.0 scale to 1–10, and merge into the user's ratings.

**REQ-RAT-003** — Import Streaming History
> When the user uploads a Netflix, Prime, Disney+, or HBO Max viewing history CSV, the system shall parse service-specific formats (Netflix: "Title: Season X: Episode", Prime: date/title columns, Disney+: date/title, HBO: date/title), match to IMDB IDs via TMDB, and store as watch history entries.

**REQ-RAT-004** — Display Ratings
> While the user is on the ratings page, the system shall display all rated titles in a searchable, sortable, filterable grid showing: poster, title, year, user rating, IMDB rating, RT score, Metacritic score, genres, streaming availability, library indicator, video source icon, awards badges, trailer link, and TasteDive similar link.

**REQ-RAT-005** — Rate a Title
> When the user clicks a rating value (1–10) on a title page, the system shall store the rating with the current date and redirect back to the title page.

**REQ-RAT-006** — Filter by Mood
> When the user selects a mood (☀️ Light, 🔥 Intense, 😂 Funny, 🌀 Mind-Bending, 🌑 Dark, ⚔️ Epic, 💕 Romantic, 👻 Scary, ✨ Inspiring), the system shall filter unrated titles by keyword overlap with mood-specific keyword sets and display matching titles.

**REQ-RAT-007** — View Rating History
> While the user is on the History page, the system shall display all ratings sorted by date descending, showing the title, rating value, and date rated.

**REQ-RAT-008** — View Unrated Titles
> While the user is on the Unrated page, the system shall display titles present in the user's library or watch history that have no rating, sorted by IMDB rating descending.

**REQ-RAT-009** — Dark/Light Theme Toggle
> When the user clicks the theme toggle, the system shall switch between dark and light CSS themes and persist the choice via cookie.

### 2.2 Discovery & Recommendations

**REQ-REC-001** — Taste Profile Construction
> When the system builds recommendations, it shall construct a taste profile from all titles rated 6+ by extracting keywords (weight: `(rating-5)/5 × 1.0`), genres (`× 0.5`), directors (`× 2.0`), actors (`× 1.5`), and writers (`× 1.5`). Fully-watched unrated TV shows shall count as implicit rating 7.

**REQ-REC-002** — Five-Category Recommendations
> While the user is on the Recommendations page, the system shall display recommendations in 5 categories:
> - 🧬 DNA Match: highest taste profile score
> - 🎬 Director's Chair: titles by directors the user rates highly
> - 👥 Community: TMDB "similar" titles to user's top-rated
> - ✅ Unanimous Hits: high IMDB + high TMDB + high taste score
> - 💫 Blast from the Past: titles older than 20 years matching taste

**REQ-REC-003** — Streaming Filter
> When the user has configured streaming providers, the system shall filter recommendations to only show titles available on the user's subscribed services in their configured country (WATCH_COUNTRY).

**REQ-REC-004** — Seasonal Boost
> When scoring recommendations, the system shall boost keywords matching the current season (e.g., "christmas" in December, "summer" in June–August) by adding seasonal keyword weights.

**REQ-REC-005** — Score Divergence Detection
> When displaying a title, if the IMDB rating and TMDB rating diverge by more than 2.0 points (on normalized 10-point scale), the system shall flag the title as having suspicious score manipulation.

**REQ-REC-006** — AI Friend Insights
> While the user is on the AI Friend page, the system shall display:
> - 💎 Hidden Gems: unrated library titles with high taste score but low IMDB rating (<7.0)
> - 🤔 Why Do I Have This?: library titles with low taste score AND low IMDB rating
> - 😈 Guilty Pleasures: titles rated 8+ by user but IMDB < 6.5
> - 🎬 Directors You Love: directors with 3+ titles rated 7+ by user

**REQ-REC-007** — AI Chat
> When the user submits a question on the AI Friend page and an LLM URL is configured, the system shall send the question to the LLM with the user's top 20 rated titles as context and display the response.

**REQ-REC-008** — Tonight Quick Pick
> While the user is on the Tonight page, the system shall display a single random recommendation from the user's streaming services with an embedded trailer (YouTube iframe).

**REQ-REC-009** — Random Pick
> When the user visits the Random page, the system shall display a random title from the user's library or rated titles with trailer embed.

**REQ-REC-010** — Anti-Recommendations
> When computing anti-recommendations, the system shall identify genres the user consistently rates low (average < 5) and surface highly-rated titles in those genres that the user has not seen.

**REQ-REC-011** — Collaborative Recommendations
> When computing collaborative recommendations, the system shall use TMDB "similar" data from the user's top-rated titles as a pseudo-collaborative signal, scoring by frequency of appearance.

**REQ-REC-012** — Save Space
> While the user is on the Save Space page, the system shall display library titles with the lowest taste profile scores (only titles with enough metadata to score), sorted ascending, to help identify candidates for removal.

### 2.3 Streaming Catalog

**REQ-CAT-001** — Fetch Streaming Catalog
> When triggered (weekly Sunday 4am or manually), the system shall query TMDB discover API for all movies and TV shows available on the user's streaming providers in WATCH_COUNTRY, storing results in `catalog.json`.

**REQ-CAT-002** — Browse Catalog
> While the user is on the Catalog page, the system shall display all titles in the streaming catalog, filterable by provider, genre, and decade.

**REQ-CAT-003** — New on Streaming
> While the user is on the New page, the system shall compare current catalog against previous catalog (`catalog_prev.json`) and display titles that appeared since the last refresh.

**REQ-CAT-004** — Leaving Soon
> When the system detects titles present in the previous catalog but absent from the current catalog, it shall display them as "Leaving Soon" on the New page.

**REQ-CAT-005** — Streaming Alerts
> While the user is on the Alerts page, the system shall check if any titles on the user's watchlist have become available on their streaming services and display matches.

### 2.4 Library Management

**REQ-LIB-001** — Library Dashboard
> While the user is on the Library page, the system shall display: total titles, progress bars for sized/hashed/subtitled files, agent status (online/offline with last seen), task queue with ETA, resolution breakdown (4K/1080p/720p/SD), codec breakdown (HEVC/AVC), and duplicate count.

**REQ-LIB-002** — Browse Library
> While the user is on the Browse page, the system shall display all library items in a paginated (50/page), searchable, sortable table showing: title, year, quality, codec, file size, and path.

**REQ-LIB-003** — TV Shows
> While the user is on the TV Shows page, the system shall group episodes by show and season, detect episode gaps, check quality consistency across episodes, calculate completion percentage, and allow sorting by watched percentage.

**REQ-LIB-004** — Scraper
> While the user is on the Scraper page, the system shall display unmatched library files, parse filenames to extract title/year/quality, search TMDB for matches, and allow one-click matching to an IMDB ID.

**REQ-LIB-005** — Organize
> While the user is on the Organize page, the system shall display library breakdown by: storage location, decade, genre, and video source.

**REQ-LIB-006** — Confirm Mismatches
> While the user is on the Confirm page, the system shall display titles where the filename does not fuzzy-match the IMDB title, originalTitle, or any TMDB alternative title (using German transliterations, substring matching, `/`→space, `&`→and, `-`→space normalization, stripping standalone numbers). The user can ✅ Confirm or 🔍 Re-match each.

**REQ-LIB-007** — Duplicate Detection
> When the system detects multiple library entries for the same IMDB ID, it shall display side-by-side comparison cards showing: resolution, codec, file size, video source, and thumbnail (if available), with ✅ KEEP / ❌ REMOVE / ⚖️ REVIEW suggestions.

**REQ-LIB-008** — Incoming Folder
> When an incoming folder path is configured, the system shall periodically scan it via the agent, parse filenames, auto-match to TMDB, and display results on the Incoming page with Import/Delete/Match buttons. Movies and TV shows are displayed in separate sections.

**REQ-LIB-009** — Library Convention Detection
> When analyzing library paths, the system shall detect the naming convention by examining path components (collection folders, year folders, title format, quality/codec/bitrate/source suffixes, separator characters) and generate a template string.

**REQ-LIB-010** — Destination Path Builder
> When importing a file, the system shall generate a destination path following the detected convention template, substituting title, year, quality, codec, bitrate, source, and extension.

**REQ-LIB-011** — NFO Generation
> When the user applies a scraper match, the system shall generate a Kodi-compatible `.nfo` XML file containing: title, year, IMDB ID, plot, genre tags, director tags, runtime, MPAA rating, IMDB rating, and votes.

### 2.5 Media Server Integration

**REQ-MED-001** — Plex Sync
> When Plex is configured (URL + token), the system shall fetch all movie library sections via `/library/sections`, iterate items, and extract IMDB IDs from GUIDs.

**REQ-MED-002** — Jellyfin Sync
> When Jellyfin is configured (URL + API key), the system shall auto-discover the first user if no user_id is provided, then fetch all Movie and Series items recursively.

**REQ-MED-003** — Emby Sync
> When Emby is configured, the system shall use the same API as Jellyfin (identical endpoint structure).

**REQ-MED-004** — Kodi Sync
> When Kodi is configured (JSON-RPC URL), the system shall fetch movies via `VideoLibrary.GetMovies` (with properties: imdbnumber, file, year, playcount, lastplayed, runtime, streamdetails) and TV shows via `VideoLibrary.GetTVShows` + `VideoLibrary.GetEpisodes` per show.

**REQ-MED-005** — Radarr Sync
> When Radarr is configured (URL + API key), the system shall fetch all movies via `/api/v3/movie` and extract IMDB IDs, paths, quality, and size.

**REQ-MED-006** — Sonarr Sync
> When Sonarr is configured (URL + API key), the system shall fetch all series via `/api/v3/series` and extract TVDB IDs, paths, and episode counts.

**REQ-MED-007** — TMM Sync
> When tinyMediaManager is configured, the system shall support two modes: path-based (scan NFO files in TMM data directory) and API-based (trigger CSV export via HTTP, parse result).

**REQ-MED-008** — Folder Scan
> When a folder path is configured, the system shall recursively scan for media files (`.mkv`, `.mp4`, `.avi`, `.m4v`), parse filenames, and match to TMDB.

**REQ-MED-009** — Library Merge Preservation
> When syncing from any media server, the system shall merge new data while preserving existing fields (file_size, file_hash, nfo_matched, confirmed, thumbnail) to prevent agent data loss.

### 2.6 Social Features

**REQ-SOC-001** — Activity Feed
> While the user is on the Feed page, the system shall display recent ratings across all users, sorted by date descending, showing user, title, rating, and date.

**REQ-SOC-002** — Compare Users
> When the user selects another user on the Compare page, the system shall calculate taste compatibility (0–100% based on shared ratings), display agreement/disagreement analysis, and list titles where ratings diverge most.

**REQ-SOC-003** — Watchlist
> When the user clicks "Add to Watchlist" on a title, the system shall store the IMDB ID in the user's watchlist. When clicking "Remove", it shall delete it.

### 2.7 Data Enrichment

**REQ-ENR-001** — TMDB Enrichment
> When enriching a title from TMDB, the system shall fetch: poster URL, overview, TMDB rating, TMDB vote count, streaming providers (by country), keywords, cast (top 5 actors), crew (director, writer), trailer (YouTube key), similar titles (first page), alternative titles (up to 15 languages), and TMDB ID.

**REQ-ENR-002** — OMDB Enrichment
> When enriching a title from OMDB, the system shall fetch: Rotten Tomatoes score, Metacritic score, and plot summary. OMDB calls are capped at 80 per enrichment run (12 runs/day × 80 = 960 < 1000 daily limit).

**REQ-ENR-003** — TVDB Enrichment
> When enriching a title from TVDB, the system shall fetch the TVDB ID for cross-referencing TV show data.

**REQ-ENR-004** — Enrichment Scheduling
> While the server is running, the system shall run enrichment every 2 hours, prioritizing: (1) never-enriched titles, (2) visible titles with missing data, (3) partially enriched titles, (4) stale titles (enriched > 30 days ago).

**REQ-ENR-005** — Enrichment Logging
> When any title field changes during enrichment, the system shall log the change to the enrichment_log SQLite table with: IMDB ID, title, year, timestamp, and a JSON dict of changed fields.

**REQ-ENR-006** — Alt Titles Scheduler
> While the server is running and TMDB_KEY is set, the system shall pull alternative titles for 10 titles every 5 seconds from TMDB, prioritizing titles that appear in the Confirm mismatches list.

**REQ-ENR-007** — IMDB Dataset Import
> When the user triggers dataset download, the system shall download `title.basics.tsv.gz` and `title.ratings.tsv.gz` from IMDB, decompress them, and use them for bulk title resolution and seeding.

**REQ-ENR-008** — Discovery Sweep
> When triggered (weekly Sunday 5am), the system shall discover highly-rated movies in EN/FR/PT/ES from TMDB discover API and add them to the title store.

### 2.8 Trakt Integration

**REQ-TRK-001** — OAuth Authentication
> When the user clicks "Connect Trakt" on the Setup page, the system shall redirect to Trakt OAuth authorization URL with the configured client ID and redirect URI.

**REQ-TRK-002** — Token Exchange
> When Trakt redirects back with an authorization code, the system shall exchange it for an access token and store it in `trakt_token.json`.

**REQ-TRK-003** — Pull Ratings
> When the user triggers Trakt sync, the system shall pull all movie and show ratings from Trakt and merge them into the user's ratings.

**REQ-TRK-004** — Push Ratings
> When the user triggers Trakt sync, the system shall push the user's ratings to Trakt, converting 1–10 scale to Trakt's 1–10 scale, batched by type (movies/shows).

**REQ-TRK-005** — Watch History
> When pulling from Trakt, the system shall fetch watch history and store it as user history entries with timestamps.

### 2.9 Subtitle Management

**REQ-SUB-001** — OpenSubtitles Search
> When searching for subtitles, the system shall query OpenSubtitles.org XML-RPC API by IMDB ID and optionally by file hash + file size for sync-accurate results.

**REQ-SUB-002** — Subtitle Download
> When the user selects a subtitle result, the system shall download the subtitle file and save it alongside the video file with matching name and language suffix.

**REQ-SUB-003** — Auto-Subtitle Search
> When triggered as a background job, the system shall search subtitles for all local titles that have no subtitle streams detected, using the configured subtitle language.

### 2.10 LAN Agent

**REQ-AGT-001** — Daemon Mode
> When started with `--daemon`, the agent shall run two parallel loops: a sync loop (Kodi sync every 4 hours) and a task loop (poll server every 10 seconds for pending tasks).

**REQ-AGT-002** — Task Polling
> While in daemon mode, the agent shall POST to `/api/tasks` every 10 seconds, receive pending tasks, and execute them sequentially (foreground) or in a background thread (exec_code).

**REQ-AGT-003** — Size Files
> When the agent receives a `size_files` task, it shall stat each file path (with 5-second SMB timeout), report file sizes back to the server, and batch results in groups of 50 using PowerShell `Get-Item`.

**REQ-AGT-004** — Hash Files
> When the agent receives a `hash_files` task, it shall compute OpenSubtitles hashes (file size + checksum of first and last 64KB) for each file and report results.

**REQ-AGT-005** — Check Quality
> When the agent receives a `check_quality` task, it shall check file existence, size, and report basic quality info for each path.

**REQ-AGT-006** — Find Duplicates
> When the agent receives a `find_duplicates` task, it shall check file existence and size for all paths, enabling server-side duplicate comparison.

**REQ-AGT-007** — Download Subtitles
> When the agent receives a `download_subs` task, it shall search OpenSubtitles by hash+size+IMDB ID, download the best match, and save alongside the video file.

**REQ-AGT-008** — Execute Code
> When the agent receives an `exec_code` task, it shall execute the provided Python code string in a background thread with access to a `progress()` callback for status reporting.

**REQ-AGT-009** — Self-Update
> When the agent receives an `update_agent` task containing new agent code, it shall write the code to `agent.py` and exit with code 42. The wrapper shall detect exit code 42 and restart after 1 second.

**REQ-AGT-010** — Scan Incoming
> When the agent receives a `scan_incoming` task, it shall scan the configured incoming folder, parse filenames, compute file sizes, and report results to the server.

**REQ-AGT-011** — Move File
> When the agent receives a `move_file` task, it shall move the source file to the destination path, creating parent directories as needed.

**REQ-AGT-012** — Delete File
> When the agent receives a `delete_file` task, it shall delete the file and remove the parent directory if empty.

**REQ-AGT-013** — Generate Thumbnail
> When the agent receives a `generate_thumb` task, it shall use ffmpeg to capture a frame at 30% of the video duration, base64-encode the JPEG, and POST it to the server.

**REQ-AGT-014** — Diagnostics
> When the agent receives a `diag` task, it shall report: OS, Python version, path mappings, path accessibility, agent version, and agent file path.

**REQ-AGT-015** — Offline Buffering
> When the agent cannot reach the server to report a task result, it shall save the result to `agent_buffer.json`. On the next successful connection, it shall flush all buffered results.

**REQ-AGT-016** — Path Mapping
> When the agent accesses files, it shall convert NFS paths (from Kodi) to local SMB paths using configured path mappings. `unmap_path()` shall perform the reverse conversion for reporting to the server.

**REQ-AGT-017** — NFO Scanning
> When syncing from Kodi, the agent shall scan for `.nfo` sidecar files in movie directories, extract IMDB IDs from `<uniqueid>` or `<id>` XML tags, and report matches in batches.

**REQ-AGT-018** — Agent Wrapper
> While the wrapper is running, it shall: start the agent process, restart after 10 seconds on crash (non-zero exit), restart after 1 second on self-update (exit code 42), and exit cleanly on exit code 0.

### 2.11 Export & RSS

**REQ-EXP-001** — CSV Export
> When the user visits `/export/<user>`, the system shall generate a CSV file with columns: imdb_id, title, year, rating, date, genres.

**REQ-EXP-002** — RSS Feed
> When any client requests `/rss/<user>`, the system shall return an RSS 2.0 XML feed of the user's recent ratings, with each item containing title, rating, link to IMDB, and publication date.

### 2.12 Contribute

**REQ-CON-001** — Data Comparison
> While the user is on the Contribute page, the system shall compare local title data against TMDB, TVDB, and Wikidata, showing data gaps and mismatches.

**REQ-CON-002** — One-Click Pull
> When the user clicks "Pull" for a data source, the system shall fetch the latest data from that source and update the local title store.

### 2.13 Public Domain Movies

**REQ-PUB-001** — Internet Archive Search
> When the user searches on the Free Movies page, the system shall query Internet Archive's advanced search API for movies matching the query, sorted by downloads descending, and display results in a poster grid with thumbnails and direct streaming links.

### 2.14 Title Detail Page

**REQ-TIT-001** — Title Page
> When the user visits `/title/<imdb_id>`, the system shall display: poster, title, year, genres, plot, IMDB/TMDB/RT/Metacritic scores, streaming availability, cast, director, trailer embed, similar titles, TasteDive recommendations, user's rating (if any), and a rating widget.

---

## 3. Edge Cases (from error handling logic)

| ID | Edge Case | Handling |
|----|-----------|----------|
| EC-001 | Corrupt JSON file on disk | `safe_json_load()` catches `JSONDecodeError` and `ValueError`, returns empty dict/list |
| EC-002 | TMDB API returns no results for IMDB ID | `tmdb_enrich()` returns empty dict, title remains unenriched |
| EC-003 | OMDB daily limit exceeded | OMDB calls capped at 80/run; `api_get()` catches exceptions and returns None |
| EC-004 | Agent cannot reach server | `buffer_result()` saves to `agent_buffer.json`; `flush_buffer()` retries on reconnect |
| EC-005 | SMB file stat hangs | `_safe_stat()` uses 5-second thread timeout, returns 0 on timeout |
| EC-006 | File path contains non-ASCII characters | Filename parsing uses `unicodedata.normalize("NFD")` to strip accents |
| EC-007 | German transliterations in filenames | `_normalize()` converts ä→ae, ö→oe, ü→ue, ß→ss before matching |
| EC-008 | Duplicate IMDB IDs with different file paths | Library stores as list of dicts when same IMDB ID has multiple paths |
| EC-009 | Kodi sync overwrites agent data | Agent data stored in separate SQLite table (`agent_data`), merged at display time |
| EC-010 | Race condition on JSON file writes | `safe_json_save()` uses `threading.Lock` per file path via `_get_lock()` |
| EC-011 | SQLite concurrent access | WAL mode + `busy_timeout=5000` + thread-local connections via `threading.local()` |
| EC-012 | Agent self-update during task execution | Background tasks (exec_code) run in separate thread; update_agent writes and exits 42 |
| EC-013 | Multipart form parsing without library | Manual boundary splitting of `body.split(b"--" + boundary)` |
| EC-014 | TVDB token expiration | `tvdb_token()` checks file age, re-authenticates if expired |
| EC-015 | Trakt token stored but invalid | `trakt_headers()` returns None if no token, callers check before use |
| EC-016 | Empty library (no ratings) | `build_taste_profile()` returns empty profile; `score_title()` returns 0 for empty profile |
| EC-017 | Title with no TMDB ID | Alt titles scheduler skips titles without `tmdb_id` |
| EC-018 | Letterboxd half-star ratings | Converts 0.5–5.0 scale to 1–10 via `int(float(rating) * 2)` |
| EC-019 | Stack:// URLs from Kodi | `map_path()` extracts first file path from `stack://` URLs |
| EC-020 | Agent log file grows unbounded | Log file trimmed to last 200 lines when exceeding 50KB |
| EC-021 | POST body with zero content length | `body = self.rfile.read(cl) if cl > 0 else b""` |
| EC-022 | Concurrent enrichment of same title | File-level locking via `_get_lock()` prevents concurrent writes |
| EC-023 | IMDB dataset decompression failure | `download_imdb_datasets()` wraps gzip decompression in try/except |
| EC-024 | TV episode format variations | Regex handles `S01E03`, `s01e03`, `1x03`, `1X03` formats |
| EC-025 | Bitrate remnants in filenames | `_normalize()` strips standalone numbers to avoid false matches |
| EC-026 | Missing ffmpeg for thumbnails | `generate_thumb` catches subprocess errors, returns error result |
| EC-027 | Task queue corruption | Migrated to SQLite with ACID transactions; `db_trim_done()` prevents unbounded growth |
| EC-028 | API key not configured | Each enrichment function checks for key presence before making calls |
| EC-029 | User directory doesn't exist | `user_dir()` calls `os.makedirs(exist_ok=True)` |
| EC-030 | HTTP request timeout to external API | `api_get()` and `api_post()` use `timeout=10` (default) |

---

## 4. Security Constraints

| ID | Constraint | Implementation |
|----|-----------|----------------|
| SEC-001 | Agent authentication | POST to `/api/library/` requires `X-Agent-Token` header matching `AGENT_TOKEN` config. Returns 403 on mismatch. |
| SEC-002 | No user authentication | The application has no login system. All users are accessible by URL path (`/u/<username>`). **This is a known limitation for LAN-only deployment.** |
| SEC-003 | API key storage | Keys stored in plaintext JSON (`/data/api_keys.json`). Loaded at startup and on save. |
| SEC-004 | XSS prevention | `esc()` function wraps `html.escape()` for user-facing output. **Not yet applied to all output paths — technical debt.** |
| SEC-005 | CORS | `Access-Control-Allow-Origin: *` on all JSON responses and OPTIONS preflight. Open by design for agent communication. |
| SEC-006 | Path traversal | User paths constructed via `user_dir()` which joins `DATA_DIR/users/<username>`. No explicit path traversal validation on username. |
| SEC-007 | File upload parsing | Manual multipart boundary parsing. No size limit enforcement on uploads. |
| SEC-008 | Trakt OAuth | Standard OAuth2 code exchange flow. Tokens stored per-user in `trakt_token.json`. |
| SEC-009 | LLM token | Optional Bearer token for LLM API calls, stored in `api_keys.json`. |
| SEC-010 | Agent code execution | `exec_code` task type executes arbitrary Python on the agent machine. Controlled by server-side task queue only. |
| SEC-011 | SQLite injection | All SQLite queries use parameterized statements (`?` placeholders). |
| SEC-012 | Atomic file writes | `safe_json_save()` writes to temp file then renames, preventing partial writes on crash. |
| SEC-013 | OpenSubtitles credentials | Username and password stored in plaintext in `api_keys.json`. Used for XML-RPC authentication. |

---

## 5. Non-Functional Requirements

| ID | Requirement |
|----|-------------|
| NFR-001 | Zero external Python dependencies — stdlib only |
| NFR-002 | Single-file architecture for server (`app.py`) and agent (`agent.py`) |
| NFR-003 | Docker image < 60MB (python:alpine + nfs-utils) |
| NFR-004 | Support 20,000+ titles without degradation |
| NFR-005 | Thread-safe concurrent request handling via `ThreadingMixIn` |
| NFR-006 | OMDB API usage < 1000 calls/day (80/run × 12 runs) |
| NFR-007 | Agent SMB operations timeout after 5 seconds |
| NFR-008 | SQLite WAL mode for concurrent read/write |
| NFR-009 | Mobile-responsive CSS layout |
| NFR-010 | Deployable behind reverse proxy at subpath (`BASE = "/cinecross"`) |
