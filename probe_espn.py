#!/usr/bin/env python3
"""Diagnose ESPN 403s by testing header variants against both endpoints.

The point of this script is to produce a TABLE, not a theory. Run it in the
same environment that is failing (i.e. on the GitHub runner via the
probe_espn workflow) and compare against a run from a machine that works.

Two things are being separated:

  1. Which HEADER SET does ESPN accept?      (rows of the table)
  2. Does it differ by ENVIRONMENT?          (runner vs local vs browser)

Known-good baseline, captured from a real browser on 2026-08-05:
    site.api.espn.com  .../teams/nyy/roster        -> 200, 26 athletes
    a.espncdn.com      .../full/33192.png          -> 200, 600x436 PNG
So both endpoints are public and alive. Any 403 is client-side.

Usage:
    python probe_espn.py            # header matrix
    python probe_espn.py --burst    # also test rate limiting
"""

import argparse
import sys
import time

import requests

# The header set sync_espn_headshots.py currently sends.
CURRENT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
CURRENT_HEADERS = {
    "User-Agent": CURRENT_UA,
    "Accept-Language": "en-US,en;q=0.9",
}

# A real, current Chrome UA observed in the browser on 2026-08-05.
MODERN_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/150.0.0.0 Safari/537.36"
)

ROSTER_API = ("https://site.api.espn.com/apis/site/v2/sports"
              "/baseball/mlb/teams/nyy/roster")
HEADSHOT = "https://a.espncdn.com/i/headshots/mlb/players/full/33192.png"

VARIANTS = [
    ("none (requests default)", None),
    ("CURRENT (Chrome/122)",    CURRENT_HEADERS),
    ("UA only Chrome/122",      {"User-Agent": CURRENT_UA}),
    ("UA only Chrome/150",      {"User-Agent": MODERN_UA}),
    ("Chrome/150 + Lang",       {"User-Agent": MODERN_UA,
                                 "Accept-Language": "en-US,en;q=0.9"}),
    ("Accept-Language only",    {"Accept-Language": "en-US,en;q=0.9"}),
    ("no-UA explicit",          {"User-Agent": ""}),
]


def hit(url: str, headers: dict | None) -> str:
    """Return a short status string for one request."""
    try:
        s = requests.Session()
        if headers is not None:
            # Only override the keys under test. Clearing the whole default
            # header set would also drop Accept-Encoding/Connection, so a
            # variant would differ from the baseline in more than one way and
            # the comparison would prove nothing.
            s.headers.update(headers)
            if headers.get("User-Agent") == "":
                s.headers.pop("User-Agent", None)
        r = s.get(url, timeout=25)
        if r.status_code == 200:
            return f"200 ({len(r.content)}b)"
        return str(r.status_code)
    except Exception as exc:
        return f"ERR {type(exc).__name__}"


def matrix() -> int:
    print(f"python-requests {requests.__version__}\n")
    print(f"  {'header variant':<26} {'roster API':<16} {'headshot CDN':<16}")
    print("  " + "-" * 58)

    results = []
    for label, headers in VARIANTS:
        api = hit(ROSTER_API, headers)
        time.sleep(1)
        cdn = hit(HEADSHOT, headers)
        time.sleep(1)
        print(f"  {label:<26} {api:<16} {cdn:<16}")
        results.append((label, api, cdn))

    print()
    api_ok = [r[0] for r in results if r[1].startswith("200")]
    cdn_ok = [r[0] for r in results if r[2].startswith("200")]

    if not api_ok and not cdn_ok:
        print("  Nothing worked. If the browser baseline still returns 200,")
        print("  this environment is being blocked as a whole (IP reputation),")
        print("  not the headers. Compare against a local run.")
        return 2

    print(f"  roster API accepts:   {', '.join(api_ok) or 'NOTHING'}")
    print(f"  headshot CDN accepts: {', '.join(cdn_ok) or 'NOTHING'}")

    both = [lbl for lbl in api_ok if lbl in cdn_ok]
    if both:
        print(f"\n  Use: {both[0]}")
    else:
        print("\n  No single variant satisfies both — they may need different headers.")
    return 0


def burst(n: int = 40) -> None:
    """Distinguish a hard block from rate limiting.

    build_player_index() makes ~150 sequential requests. If ESPN starts
    refusing partway through rather than immediately, the problem is volume,
    not headers — and the fix is throttling, not a User-Agent.
    """
    print(f"\nBurst test: {n} sequential roster requests, default headers")
    print("  (looking for the request number where it starts failing)\n")

    # ESPN's own team abbreviations. An earlier version of this list used
    # "cws" and "oak", which ESPN does not recognise — those returned 400 and
    # were misreported as rate limiting. Only 429/403 indicate throttling;
    # a 400 means the URL was wrong.
    teams = ["nyy", "bos", "tor", "bal", "tb", "cle", "det", "kc", "min",
             "chw", "hou", "sea", "tex", "laa", "ath", "atl", "phi", "nym",
             "mia", "wsh", "mil", "chc", "cin", "pit", "stl", "lad", "sd",
             "sf", "ari", "col"]

    first_throttle = None
    codes = {}
    bad_urls = []
    for i in range(n):
        team = teams[i % len(teams)]
        url = ("https://site.api.espn.com/apis/site/v2/sports"
               f"/baseball/mlb/teams/{team}/roster")
        try:
            code = requests.get(url, timeout=20).status_code
        except Exception as exc:
            code = f"ERR {type(exc).__name__}"
        codes[code] = codes.get(code, 0) + 1

        if code == 400:
            if team not in bad_urls:
                bad_urls.append(team)
        elif code in (403, 429) and first_throttle is None:
            first_throttle = i + 1
            print(f"  first throttle at request #{first_throttle}: {code}")

    print(f"  status counts: {codes}")
    if bad_urls:
        print(f"  400s came from unrecognised team codes: {bad_urls}")
        print("  (a probe bug, not an ESPN limit — ignore those)")
    if first_throttle is None:
        print("  No 403/429 at this volume — NOT rate limited.")
    elif first_throttle == 1:
        print("  Throttled from the first request — a hard block.")
    else:
        print(f"  Served {first_throttle - 1} requests before throttling —")
        print("  add a delay between requests rather than changing headers.")


def main() -> int:
    ap = argparse.ArgumentParser(description="Diagnose ESPN 403s")
    ap.add_argument("--burst", action="store_true",
                    help="Also run the rate-limit test")
    args = ap.parse_args()

    rc = matrix()
    if args.burst:
        burst()
    return rc


if __name__ == "__main__":
    sys.exit(main())
