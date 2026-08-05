"""
Download ESPN team logos for one or all sports.

Usage:
    python fetch_team_logos.py                        # all sports
    python fetch_team_logos.py --sport MLB
    python fetch_team_logos.py --sport NHL --sport NBA
    python fetch_team_logos.py --push                 # download all + commit + push
"""
from __future__ import annotations

import argparse
import time

import requests

from sync_espn_headshots import (
    SPORT_TEAM_URLS, REQUEST_TIMEOUT, REQUEST_SLEEP_SECONDS,
    ASSETS_REPO_PATH, parse_team_parts, git_commit_and_push,
)

LOGO_BASE = "https://a.espncdn.com/i/teamlogos/{sport}/500/{slug}.png"

LOGO_OUTPUT_DIRS = {
    "MLB": ASSETS_REPO_PATH / "mlb" / "logos",
    "NBA": ASSETS_REPO_PATH / "nba" / "logos",
    "NFL": ASSETS_REPO_PATH / "nfl" / "logos",
    "NHL": ASSETS_REPO_PATH / "nhl" / "logos",
}

# ESPN URL slug → canonical abbreviation used in the dataset.
# Only entries that differ from the slug need to be listed.
SLUG_RENAME: dict[str, dict[str, str]] = {
    "MLB": {
        "sd":  "sdp",   # San Diego Padres
        "sf":  "sfg",   # San Francisco Giants
        "tb":  "tbr",   # Tampa Bay Rays
        "kc":  "kcr",   # Kansas City Royals
        # "cws": "chw" — roster URL now uses chw directly, no rename needed
        "wsh": "wsn",   # Washington Nationals
        "ath": "oak",   # Athletics
    },
    "NBA": {
        "gs":   "gsw",  # Golden State Warriors
        "no":   "nop",  # New Orleans Pelicans
        "ny":   "nyk",  # New York Knicks
        "sa":   "sas",  # San Antonio Spurs
        "utah": "uta",  # Utah Jazz
        "wsh":  "was",  # Washington Wizards
    },
    "NFL": {},
    "NHL": {
        "nj":   "njd",  # New Jersey Devils
        "sj":   "sjs",  # San Jose Sharks
        "tb":   "tbl",  # Tampa Bay Lightning
        "utah": "uta",  # Utah Hockey Club
    },
}


def fetch_logos(sport: str, session: requests.Session) -> None:
    urls = SPORT_TEAM_URLS.get(sport.upper(), [])
    if not urls:
        print(f"[WARN] No team URLs found for {sport}")
        return

    out_dir = LOGO_OUTPUT_DIRS[sport.upper()]
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"\n{sport} — {len(urls)} teams → {out_dir}")

    for url in urls:
        slug, _ = parse_team_parts(url)
        if not slug:
            continue

        filename = SLUG_RENAME.get(sport.upper(), {}).get(slug, slug)
        dest = out_dir / f"{filename}.png"
        logo_url = LOGO_BASE.format(sport=sport.lower(), slug=slug)
        try:
            r = session.get(logo_url, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            dest.write_bytes(r.content)
            label = f"{slug} → {filename}" if filename != slug else slug
            print(f"  [ok]   {label}.png")
        except Exception as exc:
            print(f"  [fail] {slug}: {exc}")

        time.sleep(REQUEST_SLEEP_SECONDS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sport",
        action="append",
        choices=["MLB", "NBA", "NFL", "NHL"],
        metavar="SPORT",
        help="Sport to fetch (can repeat). Defaults to all.",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Commit and push updated logos after download.",
    )
    args = parser.parse_args()
    sports = [s.upper() for s in args.sport] if args.sport else list(LOGO_OUTPUT_DIRS)

    session = requests.Session()
    for sport in sports:
        fetch_logos(sport, session)

    print("\nDone.")

    if args.push:
        git_commit_and_push(ASSETS_REPO_PATH, "Update team logos")


if __name__ == "__main__":
    main()
