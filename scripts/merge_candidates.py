"""
Fetch per-sport headshot candidate CSVs from GitHub and merge new players
into data/players_to_fetch.csv. Safe to re-run; skips duplicates.

To add a new sport: append its raw GitHub URL to CANDIDATE_SOURCES.
"""
from __future__ import annotations

import csv
import re
import unicodedata
from io import StringIO
from pathlib import Path

import requests

# ── Config ────────────────────────────────────────────────────────────────────

FETCH_CSV = Path(__file__).parent.parent / "data" / "players_to_fetch.csv"

CANDIDATE_SOURCES = [
    "https://raw.githubusercontent.com/jeffwarr4/mlb-awards-predictor/main/predictions/mlb_candidates.csv",
    # Add more sports here as their predictor repos come online:
    # "https://raw.githubusercontent.com/jeffwarr4/nba-predictor/main/predictions/nba_candidates.csv",
    # "https://raw.githubusercontent.com/jeffwarr4/nfl-predictor/main/predictions/nfl_candidates.csv",
    # "https://raw.githubusercontent.com/jeffwarr4/nhl-predictor/main/predictions/nhl_candidates.csv",
]

HEADERS = {"User-Agent": "stinger-assets-sync/1.0"}


# ── Helpers ───────────────────────────────────────────────────────────────────

def normalize(name: str) -> str:
    """Must match normalize_name() in sync_espn_headshots.py."""
    name = unicodedata.normalize("NFKD", name)
    name = "".join(ch for ch in name if not unicodedata.combining(ch))
    name = name.lower().strip().replace("'", "").replace(".", "").replace("-", " ")
    return re.sub(r"\s+", " ", name)


def fetch_candidates(url: str) -> list[dict]:
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        rows = [
            row for row in csv.DictReader(StringIO(r.text))
            if row.get("player_name") and row.get("sport")
        ]
        print(f"  fetched {len(rows):3d} candidates  ← {url.split('/')[-1]}")
        return rows
    except Exception as exc:
        print(f"  [WARN] skipping {url.split('/')[-1]}: {exc}")
        return []


def load_existing() -> set[tuple[str, str]]:
    """Return (sport_upper, normalized_name) pairs already in players_to_fetch.csv."""
    if not FETCH_CSV.exists():
        return set()
    with FETCH_CSV.open("r", newline="", encoding="utf-8-sig") as f:
        return {
            (row["sport"].strip().upper(), normalize(row["player_name"]))
            for row in csv.DictReader(f)
            if row.get("sport") and row.get("player_name")
        }


def append_rows(rows: list[dict]) -> None:
    FETCH_CSV.parent.mkdir(parents=True, exist_ok=True)
    is_empty = not FETCH_CSV.exists() or FETCH_CSV.stat().st_size == 0
    with FETCH_CSV.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["sport", "player_name", "player_key"])
        if is_empty:
            writer.writeheader()
        writer.writerows(rows)


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    print("Fetching headshot candidates...")
    all_candidates: list[dict] = []
    for url in CANDIDATE_SOURCES:
        all_candidates.extend(fetch_candidates(url))

    if not all_candidates:
        print("No candidates fetched — nothing to merge.")
        return

    existing = load_existing()
    seen = set(existing)
    new_rows: list[dict] = []

    for row in all_candidates:
        sport = row["sport"].strip().upper()
        name  = row["player_name"].strip()
        key   = (sport, normalize(name))
        if key not in seen:
            new_rows.append({
                "sport":       sport,
                "player_name": name,
                "player_key":  row.get("player_key", "").strip(),
            })
            seen.add(key)

    if not new_rows:
        print(f"No new players to add — all {len(all_candidates)} candidates already in list.")
        return

    append_rows(new_rows)
    print(f"Added {len(new_rows)} new player(s) to {FETCH_CSV}:")
    for r in new_rows:
        print(f"  + [{r['sport']}] {r['player_name']}")


if __name__ == "__main__":
    main()
