#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import csv
import time
import json
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Optional, Set

import requests
from bs4 import BeautifulSoup
from plexapi.server import PlexServer
from rapidfuzz import fuzz, process
import yaml

# ---------------------------
# Helpers
# ---------------------------

def slug(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '-', s.lower()).strip('-')

def ensure_dir(p: Path):
    p.mkdir(parents=True, exist_ok=True)

def read_yaml(fp: str) -> dict:
    with open(fp, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def log(msg: str):
    print(f"[+] {msg}")

# ---------------------------
# IMDb Scraping (Top 250)
# ---------------------------

IMDB_TOP250_MOVIES_URL = "https://www.imdb.com/chart/top/"
IMDB_TOP250_TV_URL     = "https://www.imdb.com/chart/toptv/"

def scrape_imdb_top250(url: str, kind: str) -> List[dict]:
    """Return list of dicts with keys: title, year, imdb_id, kind (movie|show)."""
    headers = {"Accept-Language": "en-US,en;q=0.9", "User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers, timeout=20)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    items = []
    # IMDb often puts entries in <li> under <ul data-testid="chart-layout-main"> (2025), but HTML can change.
    # We'll support both new and older layouts:
    li_nodes = soup.select('ul[data-testid="chart-layout-main"] li')
    if not li_nodes:
        # fallback older table layout
        rows = soup.select("tbody tr")
        for tr in rows:
            a = tr.select_one("a[href*='/title/tt']")
            if not a:
                continue
            title = a.get_text(strip=True)
            href = a.get("href", "")
            m = re.search(r'/title/(tt\d+)', href)
            imdb_id = m.group(1) if m else None
            year = None
            year_span = tr.select_one(".secondaryInfo")
            if year_span and year_span.text:
                year = re.sub(r'[^\d]', '', year_span.text)
            items.append({"title": title, "year": year, "imdb_id": imdb_id, "kind": kind})
        return items

    for li in li_nodes:
        a = li.select_one("a[href*='/title/tt']")
        if not a:
            continue
        title = a.get_text(strip=True)
        href = a.get("href", "")
        m = re.search(r'/title/(tt\d+)', href)
        imdb_id = m.group(1) if m else None

        year = None
        year_node = li.find(attrs={"data-testid": "chart-year"})
        if year_node:
            year = re.sub(r'[^\d]', '', year_node.get_text(strip=True))
        else:
            # try alternative year markers
            txt = li.get_text(" ", strip=True)
            yrm = re.search(r'\((\d{4})\)', txt)
            year = yrm.group(1) if yrm else None

        items.append({"title": title, "year": year, "imdb_id": imdb_id, "kind": kind})
    return items

def get_imdb_top250_movies() -> List[dict]:
    return scrape_imdb_top250(IMDB_TOP250_MOVIES_URL, "movie")

def get_imdb_top250_tv() -> List[dict]:
    return scrape_imdb_top250(IMDB_TOP250_TV_URL, "show")

# ---------------------------
# Trakt API (optional lists)
# ---------------------------

def trakt_get(url: str, client_id: str, page: int = 1) -> requests.Response:
    headers = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "User-Agent": "PlexTopListsAudit/1.0"
    }
    return requests.get(url, headers=headers, params={"page": page, "limit": 100}, timeout=20)

def fetch_trakt_list(user: str, slug: str, list_type: str, client_id: str) -> List[dict]:
    """
    list_type: movies | shows | mixed
    Returns dicts with title, year, imdb_id, tmdb_id, tvdb_id, kind
    """
    if list_type == "movies":
        base = f"https://api.trakt.tv/users/{user}/lists/{slug}/items/movies"
        kind = "movie"
    elif list_type == "shows":
        base = f"https://api.trakt.tv/users/{user}/lists/{slug}/items/shows"
        kind = "show"
    else:
        base = f"https://api.trakt.tv/users/{user}/lists/{slug}/items"
        kind = "mixed"

    out = []
    page = 1
    while True:
        resp = trakt_get(base, client_id, page=page)
        if resp.status_code == 404:
            break
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for it in batch:
            # mixed returns "type" and "movie"/"show" subobj
            tkind = kind
            core = None
            if "type" in it and it["type"] in ("movie", "show"):
                tkind = "movie" if it["type"] == "movie" else "show"
                core = it[it["type"]]
            elif kind in ("movie", "show"):
                core = it.get(kind)
            if not core:
                continue
            title = core.get("title")
            year = core.get("year")
            ids = core.get("ids", {})
            out.append({
                "title": title,
                "year": str(year) if year else None,
                "imdb_id": ids.get("imdb"),
                "tmdb_id": ids.get("tmdb"),
                "tvdb_id": ids.get("tvdb"),
                "kind": tkind
            })
        # handle pagination
        if len(batch) < 100:
            break
        page += 1
        time.sleep(0.2)
    return out

# ---------------------------
# Plex Library
# ---------------------------

def gather_plex(plex_url: str, plex_token: str,
                movie_sections: List[str],
                show_sections: List[str]) -> Tuple[List[dict], List[dict]]:
    plex = PlexServer(plex_url, plex_token)
    movies = []
    shows = []

    def extract_ids_from_guids(guids) -> dict:
        ids = {"imdb_id": None, "tmdb_id": None, "tvdb_id": None}
        for g in guids or []:
            uri = getattr(g, "id", "") or str(g)
            # Examples:
            # com.plexapp.agents.imdb://tt0111161?lang=en
            # com.plexapp.agents.themoviedb://278?lang=en
            # com.plexapp.agents.thetvdb://80348?lang=en
            if "imdb://" in uri or "/title/tt" in uri or "tt" in uri:
                m = re.search(r'(tt\d+)', uri)
                if m: ids["imdb_id"] = m.group(1)
            if "themoviedb://" in uri:
                m = re.search(r'themoviedb://(\d+)', uri)
                if m: ids["tmdb_id"] = m.group(1)
            if "thetvdb://" in uri:
                m = re.search(r'thetvdb://(\d+)', uri)
                if m: ids["tvdb_id"] = m.group(1)
        return ids

    for sec_name in movie_sections:
        sec = plex.library.section(sec_name)
        for m in sec.all():
            ids = extract_ids_from_guids(getattr(m, "guids", None))
            movies.append({
                "title": m.title,
                "year": str(getattr(m, "year", "") or ""),
                "imdb_id": ids["imdb_id"],
                "tmdb_id": ids["tmdb_id"],
                "tvdb_id": ids["tvdb_id"],
                "ratingKey": m.ratingKey,
                "kind": "movie",
            })

    for sec_name in show_sections:
        sec = plex.library.section(sec_name)
        for s in sec.all():
            ids = extract_ids_from_guids(getattr(s, "guids", None))
            shows.append({
                "title": s.title,
                "year": str(getattr(s, "year", "") or ""),
                "imdb_id": ids["imdb_id"],
                "tmdb_id": ids["tmdb_id"],
                "tvdb_id": ids["tvdb_id"],
                "ratingKey": s.ratingKey,
                "kind": "show",
            })

    return movies, shows

# ---------------------------
# Matching
# ---------------------------

def normalize_title(t: str) -> str:
    t = t.lower().strip()
    t = re.sub(r"[’'`]", "'", t)
    t = re.sub(r"[:!?.,&/()\-]+", " ", t)
    t = re.sub(r"\s+", " ", t)
    return t

def index_by_ids(items: List[dict]) -> Dict[str, dict]:
    idx = {}
    for it in items:
        for key in ("imdb_id", "tmdb_id", "tvdb_id"):
            val = it.get(key)
            if val:
                idx[f"{key}:{val}"] = it
    return idx

def build_title_year_index(items: List[dict]) -> Dict[Tuple[str, str], dict]:
    d = {}
    for it in items:
        t = normalize_title(it["title"])
        y = it.get("year") or ""
        d[(t, y)] = it
    return d

def match_present(source: List[dict], plex_items: List[dict], fuzzy_threshold: int, prefer_ids: bool) -> Tuple[List[dict], List[dict]]:
    """Return (present, missing) from source relative to plex_items."""
    present = []
    missing = []

    id_index = index_by_ids(plex_items) if prefer_ids else {}
    ty_index = build_title_year_index(plex_items)

    # For fuzzy fallback precompute title list
    plex_titles = [normalize_title(p["title"]) for p in plex_items]

    for s in source:
        found = None

        # ID-first matching
        if prefer_ids:
            for key in ("imdb_id", "tmdb_id", "tvdb_id"):
                sid = s.get(key)
                if sid and f"{key}:{sid}" in id_index:
                    found = id_index[f"{key}:{sid}"]
                    break

        # Exact title+year
        if not found:
            ny = (normalize_title(s["title"]), s.get("year") or "")
            found = ty_index.get(ny)

        # Fuzzy title+year
        if not found:
            st = normalize_title(s["title"])
            match = process.extractOne(st, plex_titles, scorer=fuzz.WRatio)
            if match and match[1] >= fuzzy_threshold:
                # grab first plex item whose normalized title equals match[0] and same year if possible
                for p in plex_items:
                    if normalize_title(p["title"]) == match[0] and ((s.get("year") and p.get("year") == s.get("year")) or not s.get("year")):
                        found = p
                        break

        if found:
            present.append({**s, "_matched_ratingKey": found.get("ratingKey")})
        else:
            missing.append(s)

    return present, missing

# ---------------------------
# Radarr / Sonarr (optional)
# ---------------------------

def radarr_add_missing(radarr_cfg: dict, movies: List[dict]) -> List[dict]:
    if not movies:
        return []
    url = radarr_cfg["url"].rstrip("/")
    api = radarr_cfg["api_key"]
    headers = {"X-Api-Key": api, "Content-Type": "application/json"}
    added = []

    for m in movies:
        imdb = m.get("imdb_id")
        tmdb = m.get("tmdb_id")

        # Look up by IMDb first
        look_term = f"imdb:{imdb}" if imdb else (f"tmdb:{tmdb}" if tmdb else f"{m['title']} ({m.get('year','')})")
        lr = requests.get(f"{url}/api/v3/movie/lookup", params={"term": look_term}, headers=headers, timeout=20)
        if lr.status_code != 200:
            continue
        candidates = lr.json()
        if not candidates:
            # fallback plain search
            lr = requests.get(f"{url}/api/v3/movie/lookup", params={"term": m['title']}, headers=headers, timeout=20)
            candidates = lr.json()

        if not candidates:
            continue

        cand = candidates[0]
        payload = {
            "title": cand["title"],
            "tmdbId": cand.get("tmdbId"),
            "qualityProfileId": radarr_cfg["quality_profile_id"],
            "rootFolderPath": radarr_cfg["root_folder_path"],
            "monitored": bool(radarr_cfg.get("monitored", True)),
            "addOptions": {
                "searchForMovie": bool(radarr_cfg.get("search_for_movie", True))
            },
            "year": cand.get("year"),
            "titleSlug": cand.get("titleSlug"),
            "images": cand.get("images", []),
            "movieFile": {},
            "tmdbId": cand.get("tmdbId"),
            "path": os.path.join(radarr_cfg["root_folder_path"], cand.get("title") or m["title"])
        }

        ar = requests.post(f"{url}/api/v3/movie", headers=headers, data=json.dumps(payload), timeout=20)
        if ar.status_code in (200, 201):
            added.append({"title": m["title"], "year": m.get("year"), "imdb_id": imdb, "tmdb_id": m.get("tmdb_id")})
        time.sleep(0.2)
    return added

def sonarr_add_missing(sonarr_cfg: dict, shows: List[dict]) -> List[dict]:
    if not shows:
        return []
    url = sonarr_cfg["url"].rstrip("/")
    api = sonarr_cfg["api_key"]
    headers = {"X-Api-Key": api, "Content-Type": "application/json"}
    added = []

    for s in shows:
        # Prefer TVDB for Sonarr
        term = f"tvdb:{s.get('tvdb_id')}" if s.get('tvdb_id') else (f"imdb:{s.get('imdb_id')}" if s.get('imdb_id') else s['title'])
        lr = requests.get(f"{url}/api/v3/series/lookup", params={"term": term}, headers=headers, timeout=20)
        if lr.status_code != 200:
            continue
        candidates = lr.json()
        if not candidates:
            # fallback title search
            lr = requests.get(f"{url}/api/v3/series/lookup", params={"term": s["title"]}, headers=headers, timeout=20)
            candidates = lr.json()
        if not candidates:
            continue

        cand = candidates[0]
        payload = {
            "title": cand["title"],
            "qualityProfileId": sonarr_cfg["quality_profile_id"],
            "languageProfileId": sonarr_cfg.get("language_profile_id"),   # v4
            "titleSlug": cand.get("titleSlug"),
            "images": cand.get("images", []),
            "seasons": cand.get("seasons", []),
            "rootFolderPath": sonarr_cfg["root_folder_path"],
            "monitored": bool(sonarr_cfg.get("monitored", True)),
            "addOptions": {
                "searchForMissingEpisodes": bool(sonarr_cfg.get("search_for_missing_episodes", True))
            },
            "path": os.path.join(sonarr_cfg["root_folder_path"], cand.get("title") or s["title"]),
            "seriesType": sonarr_cfg.get("series_type", "standard")
        }
        # Sonarr v3 ignores languageProfileId; it’s okay if present.

        ar = requests.post(f"{url}/api/v3/series", headers=headers, data=json.dumps(payload), timeout=20)
        if ar.status_code in (200, 201):
            added.append({"title": s["title"], "year": s.get("year"), "tvdb_id": s.get("tvdb_id"), "imdb_id": s.get("imdb_id")})
        time.sleep(0.2)
    return added

# ---------------------------
# Reporting
# ---------------------------

def write_csv(path: Path, rows: List[dict], fieldnames: List[str]):
    if not rows:
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fieldnames})

def write_markdown_report(path: Path, sections: List[Tuple[str, List[dict]]]):
    with open(path, "w", encoding="utf-8") as f:
        f.write(f"# Plex Top Lists Audit\n\n")
        f.write(f"_Generated: {time.strftime('%Y-%m-%d %H:%M:%S')}_\n\n")
        for title, rows in sections:
            f.write(f"## {title}\n\n")
            if not rows:
                f.write("All caught up! ✅\n\n")
                continue
            f.write("| Title | Year | IMDb | TMDb | TVDB |\n")
            f.write("|---|---:|---|---|---|\n")
            for r in rows:
                imdb = f"[{r.get('imdb_id')}]({'https://www.imdb.com/title/'+r['imdb_id']+'/'})" if r.get('imdb_id') else ""
                tmdb = f"[{r.get('tmdb_id')}]({'https://www.themoviedb.org/'+('movie' if r.get('kind')=='movie' else 'tv')+'/'+str(r['tmdb_id'])})" if r.get('tmdb_id') else ""
                tvdb = f"[{r.get('tvdb_id')}]({'https://thetvdb.com/?id='+str(r['tvdb_id'])})" if r.get('tvdb_id') else ""
                f.write(f"| {r.get('title','')} | {r.get('year','')} | {imdb} | {tmdb} | {tvdb} |\n")
            f.write("\n")

# ---------------------------
# Main
# ---------------------------

def main():
    ap = argparse.ArgumentParser(description="Audit Plex against IMDb/Trakt top lists and optionally add missing to Radarr/Sonarr.")
    ap.add_argument("-c", "--config", default="config.media.yaml", help="Path to YAML config.")
    args = ap.parse_args()

    cfg = read_yaml(args.config)
    outdir = Path(cfg.get("output", {}).get("dir", "./out"))
    ensure_dir(outdir)

    # Gather Plex library
    log("Connecting to Plex and pulling library…")
    movies, shows = gather_lex = gather_plex(
        cfg["plex"]["url"],
        cfg["plex"]["token"],
        cfg["plex"].get("movie_sections", ["Movies"]),
        cfg["plex"].get("show_sections", ["TV Shows"])
    )

    matching_cfg = cfg.get("matching", {})
    fuzzy_threshold = int(matching_cfg.get("fuzzy_threshold", 90))
    prefer_ids = bool(matching_cfg.get("prefer_ids", True))

    # Build source lists
    sources_cfg = cfg.get("sources", {})
    src_bundles = []  # (name, items)
    if sources_cfg.get("imdb_top250_movies", False):
        log("Fetching IMDb Top 250 Movies…")
        src_bundles.append(("IMDb Top 250 Movies", get_imdb_top250_movies()))
    if sources_cfg.get("imdb_top250_tv", False):
        log("Fetching IMDb Top 250 TV…")
        src_bundles.append(("IMDb Top 250 TV", get_imdb_top250_tv()))

    trakt_cfg = sources_cfg.get("trakt")
    if trakt_cfg and trakt_cfg.get("client_id") and trakt_cfg.get("user_lists"):
        for li in trakt_cfg["user_lists"]:
            log(f"Fetching Trakt list: {li['user']}/{li['slug']} ({li['type']})…")
            items = fetch_trakt_list(li["user"], li["slug"], li["type"], trakt_cfg["client_id"])
            src_bundles.append((f"Trakt: {li['user']}/{li['slug']}", items))

    # Compare & Collect results
    all_sections_md = []
    radarr_missing_movies = []
    sonarr_missing_shows = []

    for name, items in src_bundles:
        # split by kind for matching
        src_movies = [x for x in items if x.get("kind") == "movie"]
        src_shows  = [x for x in items if x.get("kind") == "show"]

        present_movies, missing_movies = match_present(src_movies, movies, fuzzy_threshold, prefer_ids)
        present_shows,  missing_shows  = match_present(src_shows,  shows,  fuzzy_threshold, prefer_ids)

        log(f"{name}: Movies present {len(present_movies)}, missing {len(missing_movies)}; "
            f"Shows present {len(present_shows)}, missing {len(missing_shows)}")

        # Write CSVs
        cols = ["title","year","imdb_id","tmdb_id","tvdb_id","kind"]
        if cfg.get("output", {}).get("write_csv", True):
            write_csv(outdir / f"missing_{slug(name)}_movies.csv", missing_movies, cols)
            write_csv(outdir / f"missing_{slug(name)}_shows.csv",  missing_shows,  cols)

        # For MD
        all_sections_md.append((f"{name} — Missing Movies ({len(missing_movies)})", missing_movies))
        all_sections_md.append((f"{name} — Missing TV ({len(missing_shows)})", missing_shows))

        # Aggregate for optional add
        radarr_missing_movies += missing_movies
        sonarr_missing_shows  += missing_shows

    # Write MD report
    if cfg.get("output", {}).get("write_markdown", True):
        write_markdown_report(outdir / "report.md", all_sections_md)

    # Optional: Add to Radarr/Sonarr
    if cfg.get("radarr", {}).get("enabled", False):
        log(f"Adding {len(radarr_missing_movies)} movies to Radarr…")
        added = radarr_add_missing(cfg["radarr"], radarr_missing_movies)
        write_csv(outdir / "radarr_added.csv", added, ["title","year","imdb_id","tmdb_id"])

    if cfg.get("sonarr", {}).get("enabled", False):
        log(f"Adding {len(sonarr_missing_shows)} series to Sonarr…")
        added = sonarr_add_missing(cfg["sonarr"], sonarr_missing_shows)
        write_csv(outdir / "sonarr_added.csv", added, ["title","year","imdb_id","tvdb_id"])

    log("Done. See the 'out' folder for results.")

if __name__ == "__main__":
    main()
