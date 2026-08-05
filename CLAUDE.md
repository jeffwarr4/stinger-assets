# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Static asset repo for Jeff's sports award prediction pipelines. Scrapes player headshots and team logos
from ESPN for MLB/NBA/NFL/NHL, stores them as PNGs, and serves them to sibling projects over
`raw.githubusercontent.com`. There is no application here — it is a scraper plus a committed asset store.

Consumers reference assets by **URL, not by local path**. `data/espn_headshots_map.csv` is the contract:
`sport, player_key, github_raw_url, status`. The sibling `mlb-awards-predictor` repo pulls those URLs into a
Google Sheet (`Headshots` tab) via XLOOKUP on `player_key`, and `render_graphics.py` composites them into
Instagram graphics. Changing a filename silently breaks a graphic three repos away — see Player keys below.

## Environment

- Pure Python, one dependency: `requests`. No venv is pinned to this repo; the `mlb-awards-predictor` venv at
  `C:\DevVenvs2\mlb-awards-venv\Scripts\python.exe` works fine.
- No credentials of any kind. Every ESPN endpoint used here is public.

## Commands

```bash
# 1. Scrape ESPN rosters for all four leagues -> data/espn_player_index.csv
python sync_espn_headshots.py build-index

# 2. Match data/players_to_fetch.csv against that index and download missing PNGs
python sync_espn_headshots.py download
python sync_espn_headshots.py download --push      # also git add/commit/push when done

# Team logos (separate script, same ESPN CDN)
python fetch_team_logos.py

# Pull new headshot candidates from the prediction repos into players_to_fetch.csv
python scripts/merge_candidates.py

# Diagnose ESPN 403s — prints a header/endpoint matrix, changes nothing
python probe_espn.py
python probe_espn.py --burst        # also checks for rate limiting
```

`build-index` must run before `download`; the latter errors out if `data/espn_player_index.csv` is missing.

## Architecture

**Two-stage design, deliberately separated.** `build-index` hits ESPN ~150 times (30-32 teams x 4 leagues,
plus MLB injuries pages so IL players stay indexed) and writes a full roster snapshot. `download` does no
scraping of rosters at all — it reads that snapshot, fuzzy-matches requested players against it, and fetches
only the images it doesn't already have. Keep them separate; merging them would re-scrape every roster on
every run.

**Despite the URLs in `SPORT_TEAM_URLS` looking like HTML roster pages, they are never fetched.**
`scrape_roster_page()` parses the team code out of the URL and calls the JSON API instead:
`site.api.espn.com/apis/site/v2/sports/{sport_path}/{league}/teams/{code}/roster`. The `www.espn.com` URLs are
effectively a list of team codes in disguise. Two hosts are actually contacted: `site.api.espn.com` (roster
JSON) and `a.espncdn.com` (images).

**Matching is fuzzy and can silently mismatch.** `best_match()` scores `difflib` similarity on normalized
names, filtered to the requested sport, and preferentially narrowed to `team_code`. `MIN_MATCH_SCORE = 0.84`
is the accept threshold. Setting `team` in `players_to_fetch.csv` is the only defence against two players
sharing a name — use it whenever a request is ambiguous.

**`team` is a preference, never a hard filter.** It records the team a player was on when the request was
written; ESPN indexes his *current* team. Treating it as a filter meant a traded player matched nobody on his
old roster and was dropped as `NO_MATCH` — and because `write_headshot_map()` rewrites the map from successful
results only, **a `NO_MATCH` deletes that player's key from `espn_headshots_map.csv` entirely**, silently
breaking the downstream XLOOKUP. That is the failure mode to watch: a missing graphic, not an error. So
`best_match()` tries the requested team first and falls back to the whole sport if that can't clear the
threshold; genuine trades print `[TRADED]` but keep the original `player_key`.

**`TEAM_CODE_ALIASES` translates FanGraphs abbreviations to ESPN's.** The prediction repos write `KCR`, `SDP`,
`SFG`, `TBR`, `WSN`, `OAK`; ESPN uses `KC`, `SD`, `SF`, `TB`, `WSH`, `ATH`. Six clubs, and without the map the
team narrowing matches nothing for any of them.

**The roster endpoint returns two different JSON shapes, by sport.** MLB/NFL/NHL return `athletes[]` as
position groups each holding an `items[]` list; **NBA returns a flat `athletes[]` list**. The parser originally
handled only the grouped shape, so NBA indexed zero players indefinitely and every NBA request came back
`NO_MATCH`. `build_player_index()` now logs `[ERROR] {sport}: indexed 0 players` when a whole league yields
nothing — treat that as a parser or endpoint change, never as an empty league.

**Injured players are not on the roster endpoint.** `/teams/{code}/roster` returns the active roster only (26
for MLB), so anyone on the IL is missing. `scrape_injuries()` pulls the league-wide `/injuries` endpoint —
one request, ~265 MLB players — and the athlete objects there carry no top-level `id`, so it is recovered by
regex from the headshot href or the player link. An earlier attempt rewrote roster URLs to
`.../team/injuries/...`, which did nothing at all: `scrape_roster_page()` only parses the team code out of the
URL and rebuilds the roster API endpoint, so it just re-fetched the same 30 rosters.

**Player keys are the integration contract.** `resolve_output_name()` uses `player_key` from
`players_to_fetch.csv` verbatim if present, otherwise falls back to a safe-filename of the player's name. The
convention across projects is `firstname_lastname` or `firstname_lastname_team`. The output filename is
`{key}.png`, and the `github_raw_url` written to `espn_headshots_map.csv` is derived from it. Renaming a key
orphans every downstream XLOOKUP.

**`mlb/Headshots` is capitalised; the other three are lowercase.** `SPORT_OUTPUT_DIRS` maps MLB to
`mlb/Headshots` but NBA/NFL/NHL to `{sport}/headshots`. This is load-bearing on the GitHub runner, which is
case-sensitive, and invisible on Windows, which is not. Don't "normalise" it without also renaming the
directory in git and updating every stored URL.

**Soft entry point from the prediction repos.** `add_players_if_new(names, sport, teams)` appends new players
to `players_to_fetch.csv`, skipping duplicates by key. `mlb-awards-predictor/src/predict_awards.py` imports it
in a try/except, so newly-ranked players get registered for headshot sync automatically. It only queues them —
the next `download` run actually fetches.

**Regenerated files are gitignored.** `data/espn_player_index.csv` and `data/espn_headshot_results.csv` rebuild
every run and are not tracked. `data/players_to_fetch.csv` (the input queue) and `data/espn_headshots_map.csv`
(the output contract) **are** tracked and matter.

## Do not send spoofed browser headers to ESPN

This repo previously set a hardcoded `Chrome/122` `User-Agent` on every request. In August 2026 that started
returning blanket 403s from the roster API. It was never an IP block or a rate limit. Measured with
`probe_espn.py`, same machine, same minute:

| headers sent | roster API | image CDN |
|---|---|---|
| none (requests default) | **200** | 200 |
| `Chrome/122` + Accept-Language | 403 | 200 |
| `Chrome/122` alone | 403 | 200 |
| `Chrome/150` alone | 403 | 200 |
| `Chrome/150` + Accept-Language | **200** | 200 |
| Accept-Language alone | **200** | 200 |

ESPN checks that the header set is *internally consistent* with the browser it claims to be — a real Chrome
never sends a User-Agent without an Accept-Language — **and** that the claimed version is plausibly current.
A stale UA fails; a modern UA on its own also fails. Sending nothing passes both checks and requires no
maintenance, which is the point: a pinned Chrome version rots from the day it is written, and that is exactly
how this broke.

`_KNOWN_BAD_HEADERS` is retained in `sync_espn_headshots.py` purely as documentation. Nothing imports it.
If ESPN 403s again, run `probe_espn.py` and read the table before theorising.

**Only 403/429 indicate throttling.** A 400 from the roster API means a bad team code, not a limit. The burst
test in `probe_espn.py` originally misreported bad abbreviations (`cws`, `oak` — ESPN uses `chw`, `ath`) as
rate limiting. At ~150 requests with `REQUEST_SLEEP_SECONDS = 0.4` there is no throttling.

## Automation

`.github/workflows/sync_headshots.yml` runs Mondays 15:00 UTC — two hours after `mlb-awards-predictor`
commits its predictions, so newly-ranked players queued by `add_players_if_new()` are picked up in the same
cycle. It runs `merge_candidates.py`, `build-index`, `download`, then commits.

`.github/workflows/probe_espn.yml` is manual-only (`workflow_dispatch`) and commits nothing. It exists so the
header matrix can be run *on a runner* and compared against a local run — the environment difference is the
diagnostic, not either table alone.

**The sync workflow commits with `git add -A`**, so anything the run produces is committed, including
zero-byte or partial PNGs from a failed download. `download_file()` streams to disk without validating size or
content type. Worth a guard before staging.

**`git_commit_and_push()` retries with `git pull --no-rebase` on rejection**, which creates merge commits. The
bot pushes to `main` directly.
