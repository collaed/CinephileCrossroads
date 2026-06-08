"""Local crew-based movie identification using IMDB datasets.

Uses title.crew.tsv.gz and name.basics.tsv.gz to match OCR-extracted
director/actor names to IMDB title IDs without any API calls.

Usage:
    from crew_match import build_crew_index, identify_by_crew
    index = build_crew_index("/path/to/datasets")
    results = identify_by_crew(["Peter Jackson", "Fran Walsh"], index)
"""

import gzip
import os
import sqlite3
import unicodedata
import re


def _normalize_name(name):
    """Normalize a person name for fuzzy matching."""
    name = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode()
    name = re.sub(r"[^a-z\s]", "", name.lower()).strip()
    return " ".join(name.split())


def build_crew_index(dataset_dir, db_path=None):
    """Build SQLite index from IMDB name.basics + title.crew datasets.
    
    Returns path to the SQLite database.
    ~700MB compressed input → ~200MB SQLite DB.
    """
    if db_path is None:
        db_path = os.path.join(dataset_dir, "crew_index.db")

    if os.path.exists(db_path) and os.path.getsize(db_path) > 1000000:
        return db_path

    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")

    # Table: names (nconst → name)
    conn.execute("CREATE TABLE IF NOT EXISTS names (nconst TEXT PRIMARY KEY, name TEXT, name_norm TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS crew (tconst TEXT, nconst TEXT, role TEXT)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_crew_nconst ON crew(nconst)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_names_norm ON names(name_norm)")

    name_file = os.path.join(dataset_dir, "name.basics.tsv.gz")
    crew_file = os.path.join(dataset_dir, "title.crew.tsv.gz")

    if not os.path.exists(name_file) or not os.path.exists(crew_file):
        raise FileNotFoundError(f"Need name.basics.tsv.gz and title.crew.tsv.gz in {dataset_dir}")

    # Load names
    print("[crew_match] Loading name.basics.tsv.gz...")
    batch = []
    with gzip.open(name_file, "rt", encoding="utf-8", errors="ignore") as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 2:
                continue
            nconst, name = parts[0], parts[1]
            batch.append((nconst, name, _normalize_name(name)))
            if len(batch) >= 50000:
                conn.executemany("INSERT OR REPLACE INTO names VALUES (?,?,?)", batch)
                batch = []
    if batch:
        conn.executemany("INSERT OR REPLACE INTO names VALUES (?,?,?)", batch)
    conn.commit()
    print(f"[crew_match] Names loaded: {conn.execute('SELECT COUNT(*) FROM names').fetchone()[0]}")

    # Load crew
    print("[crew_match] Loading title.crew.tsv.gz...")
    batch = []
    with gzip.open(crew_file, "rt", encoding="utf-8", errors="ignore") as f:
        next(f)  # skip header
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) < 3:
                continue
            tconst = parts[0]
            directors = parts[1].split(",") if parts[1] != "\\N" else []
            writers = parts[2].split(",") if parts[2] != "\\N" else []
            for nc in directors:
                batch.append((tconst, nc.strip(), "director"))
            for nc in writers:
                batch.append((tconst, nc.strip(), "writer"))
            if len(batch) >= 100000:
                conn.executemany("INSERT INTO crew VALUES (?,?,?)", batch)
                batch = []
    if batch:
        conn.executemany("INSERT INTO crew VALUES (?,?,?)", batch)
    conn.commit()
    print(f"[crew_match] Crew loaded: {conn.execute('SELECT COUNT(*) FROM crew').fetchone()[0]}")

    conn.close()
    return db_path


def identify_by_crew(names, db_path, duration_min=None):
    """Identify a movie/show by matching OCR'd crew names against IMDB dataset.
    
    Args:
        names: list of person names extracted from credits OCR
        db_path: path to crew_index.db
        duration_min: optional runtime in minutes for filtering
    
    Returns:
        list of {imdb_id, title_count, matched_names, score} sorted by score desc
    """
    if not names or not os.path.exists(db_path):
        return []

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Normalize input names and look them up
    matched_nconsts = []
    for name in names:
        norm = _normalize_name(name)
        if len(norm) < 4:
            continue
        # Exact normalized match
        row = conn.execute("SELECT nconst, name FROM names WHERE name_norm = ?", (norm,)).fetchone()
        if row:
            matched_nconsts.append((row["nconst"], name))
            continue
        # Partial match (last name)
        parts = norm.split()
        if len(parts) >= 2:
            row = conn.execute(
                "SELECT nconst, name FROM names WHERE name_norm LIKE ? LIMIT 1",
                (f"%{parts[-1]}%{parts[0]}%" if len(parts[0]) > 2 else f"%{norm}%",)
            ).fetchone()
            if row:
                matched_nconsts.append((row["nconst"], name))

    if not matched_nconsts:
        conn.close()
        return []

    # Find titles where multiple matched people worked together
    from collections import Counter
    title_counts = Counter()
    for nconst, _ in matched_nconsts:
        rows = conn.execute("SELECT tconst FROM crew WHERE nconst = ?", (nconst,)).fetchall()
        for r in rows:
            title_counts[r["tconst"]] += 1

    conn.close()

    # Score: titles with most crew intersections win
    results = []
    for tconst, count in title_counts.most_common(10):
        if count >= 2:  # At least 2 crew members must match
            results.append({
                "imdb_id": tconst,
                "matched_count": count,
                "total_names": len(matched_nconsts),
                "score": round(count / len(matched_nconsts), 2)
            })

    return results
