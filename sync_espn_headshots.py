from __future__ import annotations

import argparse
import csv
import re
import time
import unicodedata
import subprocess
from dataclasses import dataclass, asdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup


# =========================================================
# CONFIG
# =========================================================

ASSETS_REPO_PATH = Path(r"C:\Users\jeffw\OneDrive\DevProj\stinger-assets")
ASSETS_BRANCH = "main"
GITHUB_OWNER = "jeffwarr4"
GITHUB_REPO = "stinger-assets"

INPUT_CSV = Path("data/players_to_fetch.csv")
INDEX_CSV = Path("data/espn_player_index.csv")
RESULTS_CSV = Path("data/espn_headshot_results.csv")

REQUEST_TIMEOUT = 20
REQUEST_SLEEP_SECONDS = 0.4
MIN_MATCH_SCORE = 0.84

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept-Language": "en-US,en;q=0.9",
}

SPORT_OUTPUT_DIRS = {
    "MLB": Path("mlb/headshots"),
    "NBA": Path("nba/headshots"),
    "NFL": Path("nfl/headshots"),
    "NHL": Path("nhl/headshots"),
}

# ESPN roster URLs
SPORT_TEAM_URLS = {
    "NFL": [
        "https://www.espn.com/nfl/team/roster/_/name/ari/arizona-cardinals",
        "https://www.espn.com/nfl/team/roster/_/name/atl/atlanta-falcons",
        "https://www.espn.com/nfl/team/roster/_/name/bal/baltimore-ravens",
        "https://www.espn.com/nfl/team/roster/_/name/buf/buffalo-bills",
        "https://www.espn.com/nfl/team/roster/_/name/car/carolina-panthers",
        "https://www.espn.com/nfl/team/roster/_/name/chi/chicago-bears",
        "https://www.espn.com/nfl/team/roster/_/name/cin/cincinnati-bengals",
        "https://www.espn.com/nfl/team/roster/_/name/cle/cleveland-browns",
        "https://www.espn.com/nfl/team/roster/_/name/dal/dallas-cowboys",
        "https://www.espn.com/nfl/team/roster/_/name/den/denver-broncos",
        "https://www.espn.com/nfl/team/roster/_/name/det/detroit-lions",
        "https://www.espn.com/nfl/team/roster/_/name/gb/green-bay-packers",
        "https://www.espn.com/nfl/team/roster/_/name/hou/houston-texans",
        "https://www.espn.com/nfl/team/roster/_/name/ind/indianapolis-colts",
        "https://www.espn.com/nfl/team/roster/_/name/jax/jacksonville-jaguars",
        "https://www.espn.com/nfl/team/roster/_/name/kc/kansas-city-chiefs",
        "https://www.espn.com/nfl/team/roster/_/name/lv/las-vegas-raiders",
        "https://www.espn.com/nfl/team/roster/_/name/lac/los-angeles-chargers",
        "https://www.espn.com/nfl/team/roster/_/name/lar/los-angeles-rams",
        "https://www.espn.com/nfl/team/roster/_/name/mia/miami-dolphins",
        "https://www.espn.com/nfl/team/roster/_/name/min/minnesota-vikings",
        "https://www.espn.com/nfl/team/roster/_/name/ne/new-england-patriots",
        "https://www.espn.com/nfl/team/roster/_/name/no/new-orleans-saints",
        "https://www.espn.com/nfl/team/roster/_/name/nyg/new-york-giants",
        "https://www.espn.com/nfl/team/roster/_/name/nyj/new-york-jets",
        "https://www.espn.com/nfl/team/roster/_/name/phi/philadelphia-eagles",
        "https://www.espn.com/nfl/team/roster/_/name/pit/pittsburgh-steelers",
        "https://www.espn.com/nfl/team/roster/_/name/sf/san-francisco-49ers",
        "https://www.espn.com/nfl/team/roster/_/name/sea/seattle-seahawks",
        "https://www.espn.com/nfl/team/roster/_/name/tb/tampa-bay-buccaneers",
        "https://www.espn.com/nfl/team/roster/_/name/ten/tennessee-titans",
        "https://www.espn.com/nfl/team/roster/_/name/wsh/washington-commanders",
    ],
    "NBA": [
        "https://www.espn.com/nba/team/roster/_/name/atl/atlanta-hawks",
        "https://www.espn.com/nba/team/roster/_/name/bos/boston-celtics",
        "https://www.espn.com/nba/team/roster/_/name/bkn/brooklyn-nets",
        "https://www.espn.com/nba/team/roster/_/name/cha/charlotte-hornets",
        "https://www.espn.com/nba/team/roster/_/name/chi/chicago-bulls",
        "https://www.espn.com/nba/team/roster/_/name/cle/cleveland-cavaliers",
        "https://www.espn.com/nba/team/roster/_/name/dal/dallas-mavericks",
        "https://www.espn.com/nba/team/roster/_/name/den/denver-nuggets",
        "https://www.espn.com/nba/team/roster/_/name/det/detroit-pistons",
        "https://www.espn.com/nba/team/roster/_/name/gs/golden-state-warriors",
        "https://www.espn.com/nba/team/roster/_/name/hou/houston-rockets",
        "https://www.espn.com/nba/team/roster/_/name/ind/indiana-pacers",
        "https://www.espn.com/nba/team/roster/_/name/lac/la-clippers",
        "https://www.espn.com/nba/team/roster/_/name/lal/los-angeles-lakers",
        "https://www.espn.com/nba/team/roster/_/name/mem/memphis-grizzlies",
        "https://www.espn.com/nba/team/roster/_/name/mia/miami-heat",
        "https://www.espn.com/nba/team/roster/_/name/mil/milwaukee-bucks",
        "https://www.espn.com/nba/team/roster/_/name/min/minnesota-timberwolves",
        "https://www.espn.com/nba/team/roster/_/name/no/new-orleans-pelicans",
        "https://www.espn.com/nba/team/roster/_/name/ny/new-york-knicks",
        "https://www.espn.com/nba/team/roster/_/name/okc/oklahoma-city-thunder",
        "https://www.espn.com/nba/team/roster/_/name/orl/orlando-magic",
        "https://www.espn.com/nba/team/roster/_/name/phi/philadelphia-76ers",
        "https://www.espn.com/nba/team/roster/_/name/phx/phoenix-suns",
        "https://www.espn.com/nba/team/roster/_/name/por/portland-trail-blazers",
        "https://www.espn.com/nba/team/roster/_/name/sac/sacramento-kings",
        "https://www.espn.com/nba/team/roster/_/name/sa/san-antonio-spurs",
        "https://www.espn.com/nba/team/roster/_/name/tor/toronto-raptors",
        "https://www.espn.com/nba/team/roster/_/name/utah/utah-jazz",
        "https://www.espn.com/nba/team/roster/_/name/wsh/washington-wizards",
    ],
    "NHL": [
        "https://www.espn.com/nhl/team/roster/_/name/ana/anaheim-ducks",
        "https://www.espn.com/nhl/team/roster/_/name/bos/boston-bruins",
        "https://www.espn.com/nhl/team/roster/_/name/buf/buffalo-sabres",
        "https://www.espn.com/nhl/team/roster/_/name/cgy/calgary-flames",
        "https://www.espn.com/nhl/team/roster/_/name/car/carolina-hurricanes",
        "https://www.espn.com/nhl/team/roster/_/name/chi/chicago-blackhawks",
        "https://www.espn.com/nhl/team/roster/_/name/col/colorado-avalanche",
        "https://www.espn.com/nhl/team/roster/_/name/cbj/columbus-blue-jackets",
        "https://www.espn.com/nhl/team/roster/_/name/dal/dallas-stars",
        "https://www.espn.com/nhl/team/roster/_/name/det/detroit-red-wings",
        "https://www.espn.com/nhl/team/roster/_/name/edm/edmonton-oilers",
        "https://www.espn.com/nhl/team/roster/_/name/fla/florida-panthers",
        "https://www.espn.com/nhl/team/roster/_/name/la/los-angeles-kings",
        "https://www.espn.com/nhl/team/roster/_/name/min/minnesota-wild",
        "https://www.espn.com/nhl/team/roster/_/name/mtl/montreal-canadiens",
        "https://www.espn.com/nhl/team/roster/_/name/nsh/nashville-predators",
        "https://www.espn.com/nhl/team/roster/_/name/nj/new-jersey-devils",
        "https://www.espn.com/nhl/team/roster/_/name/nyi/new-york-islanders",
        "https://www.espn.com/nhl/team/roster/_/name/nyr/new-york-rangers",
        "https://www.espn.com/nhl/team/roster/_/name/ott/ottawa-senators",
        "https://www.espn.com/nhl/team/roster/_/name/phi/philadelphia-flyers",
        "https://www.espn.com/nhl/team/roster/_/name/pit/pittsburgh-penguins",
        "https://www.espn.com/nhl/team/roster/_/name/sj/san-jose-sharks",
        "https://www.espn.com/nhl/team/roster/_/name/sea/seattle-kraken",
        "https://www.espn.com/nhl/team/roster/_/name/stl/st-louis-blues",
        "https://www.espn.com/nhl/team/roster/_/name/tb/tampa-bay-lightning",
        "https://www.espn.com/nhl/team/roster/_/name/tor/toronto-maple-leafs",
        "https://www.espn.com/nhl/team/roster/_/name/utah/utah-hockey-club",
        "https://www.espn.com/nhl/team/roster/_/name/van/vancouver-canucks",
        "https://www.espn.com/nhl/team/roster/_/name/vgk/vegas-golden-knights",
        "https://www.espn.com/nhl/team/roster/_/name/wsh/washington-capitals",
        "https://www.espn.com/nhl/team/roster/_/name/wpg/winnipeg-jets",
    ],
    "MLB": [
        "https://www.espn.com/mlb/team/roster/_/name/ari/arizona-diamondbacks",
        "https://www.espn.com/mlb/team/roster/_/name/atl/atlanta-braves",
        "https://www.espn.com/mlb/team/roster/_/name/bal/baltimore-orioles",
        "https://www.espn.com/mlb/team/roster/_/name/bos/boston-red-sox",
        "https://www.espn.com/mlb/team/roster/_/name/chc/chicago-cubs",
        "https://www.espn.com/mlb/team/roster/_/name/cws/chicago-white-sox",
        "https://www.espn.com/mlb/team/roster/_/name/cin/cincinnati-reds",
        "https://www.espn.com/mlb/team/roster/_/name/cle/cleveland-guardians",
        "https://www.espn.com/mlb/team/roster/_/name/col/colorado-rockies",
        "https://www.espn.com/mlb/team/roster/_/name/det/detroit-tigers",
        "https://www.espn.com/mlb/team/roster/_/name/hou/houston-astros",
        "https://www.espn.com/mlb/team/roster/_/name/kc/kansas-city-royals",
        "https://www.espn.com/mlb/team/roster/_/name/laa/los-angeles-angels",
        "https://www.espn.com/mlb/team/roster/_/name/lad/los-angeles-dodgers",
        "https://www.espn.com/mlb/team/roster/_/name/mia/miami-marlins",
        "https://www.espn.com/mlb/team/roster/_/name/mil/milwaukee-brewers",
        "https://www.espn.com/mlb/team/roster/_/name/min/minnesota-twins",
        "https://www.espn.com/mlb/team/roster/_/name/nym/new-york-mets",
        "https://www.espn.com/mlb/team/roster/_/name/nyy/new-york-yankees",
        "https://www.espn.com/mlb/team/roster/_/name/ath/athletics",
        "https://www.espn.com/mlb/team/roster/_/name/phi/philadelphia-phillies",
        "https://www.espn.com/mlb/team/roster/_/name/pit/pittsburgh-pirates",
        "https://www.espn.com/mlb/team/roster/_/name/sd/san-diego-padres",
        "https://www.espn.com/mlb/team/roster/_/name/sf/san-francisco-giants",
        "https://www.espn.com/mlb/team/roster/_/name/sea/seattle-mariners",
        "https://www.espn.com/mlb/team/roster/_/name/stl/st-louis-cardinals",
        "https://www.espn.com/mlb/team/roster/_/name/tb/tampa-bay-rays",
        "https://www.espn.com/mlb/team/roster/_/name/tex/texas-rangers",
        "https://www.espn.com/mlb/team/roster/_/name/tor/toronto-blue-jays",
        "https://www.espn.com/mlb/team/roster/_/name/wsh/washington-nationals",
    ],
}


# =========================================================
# DATA MODELS
# =========================================================

@dataclass
class PlayerIndexRow:
    sport: str
    team_code: str
    team_slug: str
    player_name: str
    player_name_norm: str
    espn_athlete_id: str
    headshot_url: str
    player_url: str


@dataclass
class RequestRow:
    sport: str
    player_name: str
    player_key: str


@dataclass
class MatchResult:
    sport: str
    requested_name: str
    player_key: str
    matched_name: str
    match_score: float
    espn_athlete_id: str
    headshot_url: str
    local_repo_path: str
    github_raw_url: str
    status: str


# =========================================================
# HELPERS
# =========================================================

def normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKD", value)
    value = "".join(ch for ch in value if not unicodedata.combining(ch))
    value = value.lower().strip()
    value = value.replace("'", "")
    value = value.replace(".", "")
    value = value.replace("-", " ")
    value = re.sub(r"\s+", " ", value)
    return value


def safe_filename(value: str) -> str:
    value = normalize_name(value)
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def parse_team_parts(url: str) -> tuple[str, str]:
    m = re.search(r"/name/([^/]+)/([^/?#]+)$", url)
    if not m:
        return "", ""
    return m.group(1), m.group(2)


def extract_athlete_id(text: str) -> Optional[str]:
    if not text:
        return None
    for pattern in [
        r"/full/(\d+)\.(?:png|jpg|jpeg|webp)",
        r"/player/_/id/(\d+)",
        r"/athlete/_/id/(\d+)",
        r"/id/(\d+)",
    ]:
        m = re.search(pattern, text)
        if m:
            return m.group(1)
    return None


def build_github_raw_url(relative_path: Path) -> str:
    rel = relative_path.as_posix()
    return (
        f"https://raw.githubusercontent.com/"
        f"{GITHUB_OWNER}/{GITHUB_REPO}/{ASSETS_BRANCH}/{rel}"
    )


def resolve_output_name(req: RequestRow, match_row: PlayerIndexRow) -> str:
    """
    Uses current sport-specific key conventions.
    Returned value is used as filename base (before .png).
    """
    if req.player_key.strip():
        return safe_filename(req.player_key)

    if req.sport == "NBA":
        return safe_filename(req.player_name)   # firstname_lastname

    if req.sport == "MLB":
        return safe_filename(req.player_name)   # firstname_lastname

    if req.sport == "NFL":
        return safe_filename(req.player_name)   # current names convert to safe file

    if req.sport == "NHL":
        return safe_filename(req.player_name)

    return safe_filename(req.player_name)


# =========================================================
# SCRAPING
# =========================================================

def scrape_roster_page(session: requests.Session, sport: str, url: str) -> list[PlayerIndexRow]:
    print(f"Scraping {sport}: {url}")
    resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    team_code, team_slug = parse_team_parts(url)

    rows: list[PlayerIndexRow] = []
    seen_ids: set[str] = set()

    anchors = soup.find_all("a", href=True)

    for a in anchors:
        href = a.get("href", "")
        player_name = a.get_text(" ", strip=True)

        if not player_name:
            continue

        if not any(marker in href for marker in ["/player/_/id/", "/athlete/_/id/"]):
            continue

        athlete_id = extract_athlete_id(href)
        if not athlete_id:
            continue

        if athlete_id in seen_ids:
            continue
        seen_ids.add(athlete_id)

        player_url = href if href.startswith("http") else f"https://www.espn.com{href}"

        headshot_url = ""

        search_nodes = []
        parent = a.parent
        if parent:
            search_nodes.append(parent)
        if parent and parent.parent:
            search_nodes.append(parent.parent)

        for node in search_nodes:
            imgs = node.find_all("img")
            for img in imgs:
                src = img.get("src") or img.get("data-src") or ""
                if athlete_id in src and "headshots" in src:
                    headshot_url = src
                    break
            if headshot_url:
                break

        if not headshot_url:
            headshot_url = f"https://a.espncdn.com/i/headshots/{sport.lower()}/players/full/{athlete_id}.png"

        rows.append(
            PlayerIndexRow(
                sport=sport,
                team_code=team_code.upper(),
                team_slug=team_slug,
                player_name=player_name,
                player_name_norm=normalize_name(player_name),
                espn_athlete_id=athlete_id,
                headshot_url=headshot_url,
                player_url=player_url,
            )
        )

    return rows


def build_player_index() -> list[PlayerIndexRow]:
    session = requests.Session()
    session.headers.update(HEADERS)

    all_rows: list[PlayerIndexRow] = []

    for sport, urls in SPORT_TEAM_URLS.items():
        for url in urls:
            try:
                rows = scrape_roster_page(session, sport, url)
                all_rows.extend(rows)
            except Exception as exc:
                print(f"[WARN] Failed scraping {sport} {url}: {exc}")
            time.sleep(REQUEST_SLEEP_SECONDS)

    deduped: dict[tuple[str, str], PlayerIndexRow] = {}
    for row in all_rows:
        deduped[(row.sport, row.espn_athlete_id)] = row

    return sorted(
        deduped.values(),
        key=lambda r: (r.sport, r.player_name_norm, r.team_code, r.espn_athlete_id)
    )


def save_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        print(f"[INFO] No rows to save for {path}")
        return
    ensure_parent(path)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def load_requests(path: Path) -> list[RequestRow]:
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        rows: list[RequestRow] = []
        for r in reader:
            sport = (r.get("sport") or "").strip().upper()
            player_name = (r.get("player_name") or "").strip()
            player_key = (r.get("player_key") or "").strip()

            if not sport or not player_name:
                continue

            rows.append(
                RequestRow(
                    sport=sport,
                    player_name=player_name,
                    player_key=player_key,
                )
            )
        return rows


# =========================================================
# MATCHING
# =========================================================

def best_match(req: RequestRow, index_rows: list[PlayerIndexRow]) -> tuple[Optional[PlayerIndexRow], float]:
    candidates = [r for r in index_rows if r.sport == req.sport]
    if not candidates:
        return None, 0.0

    target = normalize_name(req.player_name)
    scored = [(row, similarity(target, row.player_name_norm)) for row in candidates]
    scored.sort(key=lambda x: x[1], reverse=True)
    return scored[0]


def get_relative_output_path(req: RequestRow, match_row: PlayerIndexRow) -> Path:
    output_dir = SPORT_OUTPUT_DIRS[req.sport]
    filename_base = resolve_output_name(req, match_row)
    return output_dir / f"{filename_base}.png"


def download_file(session: requests.Session, url: str, dest: Path) -> None:
    ensure_parent(dest)
    resp = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
    resp.raise_for_status()
    with dest.open("wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)


def sync_headshots(index_rows: list[PlayerIndexRow], requests_to_process: list[RequestRow]) -> list[MatchResult]:
    session = requests.Session()
    session.headers.update(HEADERS)

    results: list[MatchResult] = []

    for req in requests_to_process:
        match_row, score = best_match(req, index_rows)

        if not match_row or score < MIN_MATCH_SCORE:
            results.append(
                MatchResult(
                    sport=req.sport,
                    requested_name=req.player_name,
                    player_key=req.player_key,
                    matched_name="",
                    match_score=round(score, 4),
                    espn_athlete_id="",
                    headshot_url="",
                    local_repo_path="",
                    github_raw_url="",
                    status="NO_MATCH",
                )
            )
            continue

        relative_path = get_relative_output_path(req, match_row)
        full_path = ASSETS_REPO_PATH / relative_path
        github_url = build_github_raw_url(relative_path)

        try:
            download_file(session, match_row.headshot_url, full_path)
            results.append(
                MatchResult(
                    sport=req.sport,
                    requested_name=req.player_name,
                    player_key=req.player_key,
                    matched_name=match_row.player_name,
                    match_score=round(score, 4),
                    espn_athlete_id=match_row.espn_athlete_id,
                    headshot_url=match_row.headshot_url,
                    local_repo_path=str(full_path),
                    github_raw_url=github_url,
                    status="DOWNLOADED",
                )
            )
        except Exception as exc:
            results.append(
                MatchResult(
                    sport=req.sport,
                    requested_name=req.player_name,
                    player_key=req.player_key,
                    matched_name=match_row.player_name,
                    match_score=round(score, 4),
                    espn_athlete_id=match_row.espn_athlete_id,
                    headshot_url=match_row.headshot_url,
                    local_repo_path=str(full_path),
                    github_raw_url=github_url,
                    status=f"DOWNLOAD_FAILED: {exc}",
                )
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    return results


# =========================================================
# OPTIONAL GIT
# =========================================================

def git_commit_and_push(repo_path: Path, message: str) -> None:
    subprocess.run(["git", "-C", str(repo_path), "add", "."], check=True)
    subprocess.run(["git", "-C", str(repo_path), "commit", "-m", message], check=False)
    subprocess.run(["git", "-C", str(repo_path), "push"], check=True)


# =========================================================
# MAIN
# =========================================================

def run_build_index() -> None:
    print("Building ESPN player index...")
    index_rows = build_player_index()
    if not index_rows:
        raise RuntimeError("No player index rows scraped.")

    save_csv([asdict(r) for r in index_rows], INDEX_CSV)
    print(f"Saved {len(index_rows)} index rows to {INDEX_CSV}")


def run_downloads() -> None:
    if not INDEX_CSV.exists():
        raise FileNotFoundError(
            f"Missing index file: {INDEX_CSV}. Run build-index first."
        )

    with INDEX_CSV.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        index_rows = [
            PlayerIndexRow(
                sport=r["sport"],
                team_code=r["team_code"],
                team_slug=r["team_slug"],
                player_name=r["player_name"],
                player_name_norm=r["player_name_norm"],
                espn_athlete_id=r["espn_athlete_id"],
                headshot_url=r["headshot_url"],
                player_url=r["player_url"],
            )
            for r in reader
        ]

    requests_to_process = load_requests(INPUT_CSV)
    if not requests_to_process:
        raise RuntimeError(f"No valid rows found in {INPUT_CSV}")

    print("Matching and downloading headshots...")
    results = sync_headshots(index_rows, requests_to_process)
    save_csv([asdict(r) for r in results], RESULTS_CSV)

    downloaded = sum(1 for r in results if r.status == "DOWNLOADED")
    no_match = sum(1 for r in results if r.status == "NO_MATCH")

    print(f"Downloaded: {downloaded}")
    print(f"No match:   {no_match}")
    print(f"Saved results to {RESULTS_CSV}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=["build-index", "download"],
        help="build-index = scrape ESPN rosters; download = match and download headshots",
    )
    parser.add_argument(
        "--push",
        action="store_true",
        help="Commit and push updated assets after download",
    )
    args = parser.parse_args()

    if args.mode == "build-index":
        run_build_index()
    elif args.mode == "download":
        run_downloads()
        if args.push:
            git_commit_and_push(
                ASSETS_REPO_PATH,
                "Update ESPN player headshots"
            )


if __name__ == "__main__":
    main()