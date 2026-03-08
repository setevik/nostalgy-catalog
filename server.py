#!/usr/bin/env python3
"""OG Recall — old-games.ru memory triage server."""

import hashlib
import json
import os
import random
import re
import statistics
import threading
import time
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

import requests
from bs4 import BeautifulSoup


# --- Source Architecture ---

class GameSource:
    """Base class for game catalog sources."""
    SOURCE_ID = ""
    SOURCE_NAME = ""
    BASE_URL = ""
    PROXY_DOMAINS = set()

    def game_url(self, local_id):
        raise NotImplementedError

    def catalog_page_url(self, page):
        raise NotImplementedError

    def get_max_page(self, html):
        raise NotImplementedError

    def parse_catalog_page(self, html):
        """Parse a catalog page. Returns list of dicts with keys:
        local_id, name, genre, year, platform, publisher, rating"""
        raise NotImplementedError

    def parse_game_detail(self, local_id):
        """Fetch and parse game detail + screenshots.
        Returns {description, screenshots}"""
        raise NotImplementedError


class OldGamesRuSource(GameSource):
    SOURCE_ID = "og"
    SOURCE_NAME = "old-games.ru"
    BASE_URL = "https://www.old-games.ru"
    PROXY_DOMAINS = {"old-games.ru", "www.old-games.ru", "static.old-games.ru"}

    def game_url(self, local_id):
        return f"{self.BASE_URL}/game/{local_id}.html"

    def catalog_page_url(self, page):
        return f"{self.BASE_URL}/catalog/?sort=name&page={page}"

    def get_max_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        max_page = 1
        for a in soup.find_all("a", href=re.compile(r"page=\d+")):
            m = re.search(r"page=(\d+)", a.get("href", ""))
            if m:
                max_page = max(max_page, int(m.group(1)))
        return max_page

    def parse_catalog_page(self, html):
        soup = BeautifulSoup(html, "html.parser")
        games = []
        seen_ids = set()

        all_links = soup.find_all("a", href=True)
        game_entries = []
        current_entry = None

        for a in all_links:
            href = a.get("href", "")
            text = a.get_text(strip=True)

            game_match = re.match(r"^/game/(\d+)\.html$", href)
            if game_match:
                gid = int(game_match.group(1))
                if text and len(text) >= 2:
                    if current_entry and current_entry["local_id"] not in seen_ids:
                        game_entries.append(current_entry)
                        seen_ids.add(current_entry["local_id"])
                    current_entry = {
                        "local_id": gid,
                        "name": text,
                        "altNames": [],
                        "genre": "",
                        "year": 0,
                        "platform": "",
                        "publisher": "",
                        "rating": 0,
                        "_has_meta": False,
                    }
                continue

            if current_entry:
                if "genre=" in href and not current_entry["genre"]:
                    current_entry["genre"] = text
                    current_entry["_has_meta"] = True
                elif "year=" in href and not current_entry["year"]:
                    try:
                        current_entry["year"] = int(re.search(r"\d{4}", text).group())
                        current_entry["_has_meta"] = True
                    except:
                        pass
                elif "platform=" in href and not current_entry["platform"]:
                    current_entry["platform"] = text
                    current_entry["_has_meta"] = True
                elif "publisherCompany=" in href and not current_entry["publisher"]:
                    current_entry["publisher"] = text
                    current_entry["_has_meta"] = True

        if current_entry and current_entry["local_id"] not in seen_ids:
            game_entries.append(current_entry)

        full_text = str(soup)

        for entry in game_entries:
            game_link_pattern = f'/game/{entry["local_id"]}.html'
            idx = full_text.find(game_link_pattern)
            if idx >= 0:
                window = full_text[idx:idx + 2000]
                m = re.search(r'Оценка рецензента\s*-\s*(\d+)\s*из\s*10', window)
                if m:
                    entry["rating"] = int(m.group(1))

        result = []
        for entry in game_entries:
            del entry["_has_meta"]
            if entry["genre"] or entry["year"] or entry["platform"]:
                result.append(entry)

        return result

    def parse_game_detail(self, local_id):
        result = {"description": "", "screenshots": []}

        # Screenshots page
        try:
            url = f"{self.BASE_URL}/game/screenshots/{local_id}.html"
            html = fetch_cached(url)
            soup = BeautifulSoup(html, "html.parser")

            for a in soup.find_all("a", href=re.compile(r"screenshots/")):
                href = a.get("href", "")
                if not href or "thumbs" in href:
                    continue
                img = a.find("img")
                if img:
                    thumb = img.get("src", "")
                    full = href
                    if thumb.startswith("/"):
                        thumb = self.BASE_URL + thumb
                    elif not thumb.startswith("http"):
                        thumb = self.BASE_URL + "/" + thumb
                    if full.startswith("/"):
                        full = self.BASE_URL + full
                    elif not full.startswith("http"):
                        full = self.BASE_URL + "/" + full
                    result["screenshots"].append({"thumb": thumb, "full": full})
        except Exception as e:
            print(f"  Screenshots fetch failed for {local_id}: {e}")

        # Game page for description
        try:
            url = f"{self.BASE_URL}/game/{local_id}.html"
            html = fetch_cached(url)
            soup = BeautifulSoup(html, "html.parser")

            full_text = soup.get_text(separator="\n")

            start_idx = 0
            for marker in ["Играть в браузере", "Обложки (", "Обложки(", "Видео (", "Видео("]:
                idx = full_text.find(marker)
                if idx >= 0:
                    nl = full_text.find("\n", idx)
                    if nl >= 0:
                        start_idx = max(start_idx, nl + 1)

            screenshot_zone_end = start_idx
            for i in range(start_idx, min(start_idx + 500, len(full_text))):
                chunk = full_text[start_idx:i]
                if "Скриншот:" in chunk or "скриншот:" in chunk:
                    nl = full_text.find("\n", i)
                    if nl >= 0:
                        screenshot_zone_end = nl + 1
            start_idx = max(start_idx, screenshot_zone_end)

            end_idx = len(full_text)
            for marker in [
                "Автор обзора:",
                "Развернуть описание",
                "Время и место:",
                "Особенность геймплея:",
                "Перспектива:",
                "Страна или регион",
                "Тематика:",
                "Технические детали:",
                "Элемент жанра:",
                "Язык:",
                "Рекомендуемые",
                "Незарегистрированные",
                "Комментарии к игре",
            ]:
                idx = full_text.find(marker, start_idx)
                if start_idx < idx < end_idx:
                    end_idx = idx
                    break

            desc = ""
            if start_idx < end_idx:
                raw = full_text[start_idx:end_idx].strip()
                lines = raw.split("\n")
                clean = []
                for line in lines:
                    line = line.strip()
                    if not line:
                        if clean and clean[-1] != "":
                            clean.append("")
                        continue
                    if len(line) < 5:
                        continue
                    clean.append(line)

                desc = "\n".join(clean).strip()
                desc = desc.strip()

            result["description"] = desc[:2000] if desc else ""
        except Exception as e:
            print(f"  Detail fetch failed for {local_id}: {e}")

        return result


# --- Source Registry + ID Utilities ---

SOURCE_PRIORITY = ["og"]
SOURCES = {"og": OldGamesRuSource()}
ALLOWED_PROXY_DOMAINS = set()
for _src in SOURCES.values():
    ALLOWED_PROXY_DOMAINS.update(_src.PROXY_DOMAINS)


def make_game_id(source_id, local_id):
    return f"{source_id}:{local_id}"


def parse_game_id(game_id):
    s = str(game_id)
    if ":" in s:
        src, lid = s.split(":", 1)
        return src, lid
    return "og", s


# --- Config ---
PORT = 8765
RATE_LIMIT_SEC = 1.0
CACHE_DIR = Path("cache")
CATALOG_INDEX_FILE = Path("catalog_index.json")
RATINGS_FILE = Path("ratings.json")
QUEUE_SIZE = 20
QUEUE_REFILL_THRESHOLD = 10
PROFILE_MIN_RATINGS = 10
PROFILE_MATCH_RATIO = 0.30
PREFETCH_AHEAD = 2
AUTO_UPDATE_SKIP_SEC = 24 * 3600       # skip update if scanned < 24h ago
QUICK_SCAN_PAGES = 20                  # pages to check in incremental scan

# --- Global State ---
catalog_index = {"scannedAt": None, "totalGames": 0, "games": []}
ratings_data = {"version": 1, "games": {}, "profile": {}, "stats": {}, "history": []}
game_queue = []
unrated_pool = []
scan_progress = {"total": 0, "scanned": 0, "done": False, "scanning": False}
prefetch_cache = {}
last_request_time = 0
lock = threading.Lock()
index_html_cache = None


def rate_limited_get(url, timeout=15):
    """Fetch URL with rate limiting."""
    global last_request_time
    with lock:
        elapsed = time.time() - last_request_time
        if elapsed < RATE_LIMIT_SEC:
            time.sleep(RATE_LIMIT_SEC - elapsed)
        last_request_time = time.time()
    headers = {
        "User-Agent": "OGRecall/1.0 (personal game catalog tool)",
        "Accept-Language": "ru,en;q=0.5",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp


def cache_path(url):
    h = hashlib.md5(url.encode()).hexdigest()
    return CACHE_DIR / f"{h}.html"


def fetch_cached(url, max_age=7 * 86400):
    cp = cache_path(url)
    if cp.exists():
        age = time.time() - cp.stat().st_mtime
        if age < max_age:
            return cp.read_text(encoding="utf-8")
    resp = rate_limited_get(url)
    text = resp.text
    cp.write_text(text, encoding="utf-8")
    return text



# --- Merge Logic ---

def match_key(game):
    """Normalized (name, year, platform) for cross-source dedup."""
    name = (game.get("name") or "").strip().lower()
    year = game.get("year", 0)
    platform = (game.get("platform") or "").strip().lower()
    return (name, year, platform)


def merge_catalogs(source_games_by_id):
    """Merge per-source game lists into unified catalog entries.
    Fields filled from highest-priority source first.
    With a single source this is essentially a passthrough that prefixes IDs."""
    merged = {}  # match_key -> game dict

    for src_id in SOURCE_PRIORITY:
        games = source_games_by_id.get(src_id, [])
        for g in games:
            local_id = g["local_id"]
            game_id = make_game_id(src_id, local_id)
            key = match_key(g)

            if key in merged:
                # Game already exists from higher-priority source — add this source reference
                existing = merged[key]
                existing["sources"][src_id] = local_id
                # Fill empty fields from this source
                for field in ("genre", "year", "platform", "publisher"):
                    if not existing.get(field) and g.get(field):
                        existing[field] = g[field]
                # Store source-specific rating
                if g.get("rating"):
                    existing[f"rating_{src_id}"] = g["rating"]
            else:
                merged[key] = {
                    "id": game_id,
                    "name": g.get("name", ""),
                    "genre": g.get("genre", ""),
                    "year": g.get("year", 0),
                    "platform": g.get("platform", ""),
                    "publisher": g.get("publisher", ""),
                    f"rating_{src_id}": g.get("rating", 0),
                    "sources": {src_id: local_id},
                }

    return list(merged.values())


# --- Catalog Scanning ---

def scan_catalog(force_fresh=True):
    """Full scan of all catalog pages to build index."""
    global catalog_index, scan_progress
    scan_progress = {"total": 0, "scanned": 0, "done": False, "scanning": True, "mode": "full"}

    try:
        all_source_games = {}

        for src_id in SOURCE_PRIORITY:
            source = SOURCES[src_id]
            url = source.catalog_page_url(1)
            html = fetch_cached(url, max_age=0 if force_fresh else 365 * 86400)
            max_page = source.get_max_page(html)
            scan_progress["total"] = max_page

            games = source.parse_catalog_page(html)
            scan_progress["scanned"] = 1

            seen_ids = {g["local_id"] for g in games}

            for page in range(2, max_page + 1):
                url = source.catalog_page_url(page)
                try:
                    html = fetch_cached(url, max_age=0 if force_fresh else 365 * 86400)
                    page_games = source.parse_catalog_page(html)
                    for g in page_games:
                        if g["local_id"] not in seen_ids:
                            games.append(g)
                            seen_ids.add(g["local_id"])
                except Exception as e:
                    print(f"  Warning: {src_id} page {page} failed: {e}")
                scan_progress["scanned"] = page

            all_source_games[src_id] = games

        merged = merge_catalogs(all_source_games)

        catalog_index = {
            "scannedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "totalGames": len(merged),
            "games": merged,
        }
        CATALOG_INDEX_FILE.write_text(json.dumps(catalog_index, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"Full scan complete: {len(merged)} games indexed.")
    except Exception as e:
        print(f"Catalog scan error: {e}")
    finally:
        scan_progress["done"] = True
        scan_progress["scanning"] = False
        rebuild_pool_and_queue()


def incremental_scan():
    """Quick scan: check first/last pages + random sample for new games."""
    global catalog_index, scan_progress

    old_games = {g["id"]: g for g in catalog_index.get("games", [])}
    old_count = len(old_games)

    scan_progress = {"total": 0, "scanned": 0, "done": False, "scanning": True, "mode": "incremental"}

    try:
        for src_id in SOURCE_PRIORITY:
            source = SOURCES[src_id]

            # Fetch page 1 fresh to get current max_page
            url = source.catalog_page_url(1)
            html = fetch_cached(url, max_age=0)
            current_max_page = source.get_max_page(html)

            # Build list of pages to check:
            # first 3 + last 3 + random middle sample
            pages_to_check = set()
            for p in range(1, min(4, current_max_page + 1)):
                pages_to_check.add(p)
            for p in range(max(1, current_max_page - 2), current_max_page + 1):
                pages_to_check.add(p)
            middle = list(range(4, max(4, current_max_page - 2)))
            if middle:
                sample_size = min(len(middle), QUICK_SCAN_PAGES - len(pages_to_check))
                pages_to_check.update(random.sample(middle, max(0, sample_size)))

            pages_sorted = sorted(pages_to_check)
            scan_progress["total"] = len(pages_sorted)

            new_count = 0
            updated_count = 0

            for i, page in enumerate(pages_sorted):
                url = source.catalog_page_url(page)
                try:
                    page_html = fetch_cached(url, max_age=0) if page != 1 else html
                    games = source.parse_catalog_page(page_html)
                    for g in games:
                        game_id = make_game_id(src_id, g["local_id"])
                        if game_id not in old_games:
                            old_games[game_id] = {
                                "id": game_id,
                                "name": g.get("name", ""),
                                "genre": g.get("genre", ""),
                                "year": g.get("year", 0),
                                "platform": g.get("platform", ""),
                                "publisher": g.get("publisher", ""),
                                f"rating_{src_id}": g.get("rating", 0),
                                "sources": {src_id: g["local_id"]},
                            }
                            new_count += 1
                        else:
                            existing = old_games[game_id]
                            changed = False
                            for field in ("genre", "year", "platform", "publisher"):
                                if g.get(field) and not existing.get(field):
                                    existing[field] = g[field]
                                    changed = True
                            rating_key = f"rating_{src_id}"
                            if g.get("rating") and not existing.get(rating_key):
                                existing[rating_key] = g["rating"]
                                changed = True
                            if changed:
                                updated_count += 1
                except Exception as e:
                    print(f"  Warning: {src_id} incremental page {page} failed: {e}")
                scan_progress["scanned"] = i + 1

        # Rebuild catalog_index
        catalog_index = {
            "scannedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "totalGames": len(old_games),
            "games": list(old_games.values()),
        }
        CATALOG_INDEX_FILE.write_text(json.dumps(catalog_index, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"Incremental scan complete: checked {len(pages_sorted)} pages, "
              f"{new_count} new games, {updated_count} updated. Total: {len(old_games)} games.")
    except Exception as e:
        print(f"Incremental scan error: {e}")
    finally:
        scan_progress["done"] = True
        scan_progress["scanning"] = False
        rebuild_pool_and_queue()


def get_catalog_age_seconds():
    """Return seconds since last catalog scan, or infinity if never scanned."""
    scanned_at = catalog_index.get("scannedAt", "")
    if not scanned_at:
        return float("inf")
    try:
        t = time.strptime(scanned_at, "%Y-%m-%dT%H:%M:%SZ")
        return time.time() - time.mktime(t)
    except:
        return float("inf")


# --- Profile & Queue ---

def compute_profile():
    """Recompute user profile from ratings.

    Only 'good', 'exceptional', and wishlisted games are positive signals.
    'meh' and 'skip' do NOT boost genre/platform/year affinity.
    """
    games = ratings_data.get("games", {})
    wishlist = ratings_data.get("wishlist", {})
    liked = {}  # good/exceptional/wishlisted
    seen_genres = {}
    seen_platforms = {}

    # Collect all positive signal game IDs (rated high or wishlisted)
    liked_ids = set()
    for gid, info in games.items():
        if info.get("rating") in ("good", "exceptional"):
            liked_ids.add(gid)
    for gid in wishlist:
        liked_ids.add(gid)

    # Build genre/platform weights from rated games
    for gid, info in games.items():
        genre = info.get("genre", "")
        platform = info.get("platform", "")
        is_liked = gid in liked_ids

        if genre:
            seen_genres.setdefault(genre, {"liked": 0, "total": 0})
            seen_genres[genre]["total"] += 1
            if is_liked:
                seen_genres[genre]["liked"] += 1

        if platform:
            seen_platforms.setdefault(platform, {"liked": 0, "total": 0})
            seen_platforms[platform]["total"] += 1
            if is_liked:
                seen_platforms[platform]["liked"] += 1

        if is_liked:
            liked[gid] = info

    # Also count wishlist-only entries (not yet rated) toward genre/platform
    for gid, info in wishlist.items():
        if gid in games:
            continue  # already counted above
        genre = info.get("genre", "")
        platform = info.get("platform", "")
        if genre:
            seen_genres.setdefault(genre, {"liked": 0, "total": 0})
            seen_genres[genre]["total"] += 1
            seen_genres[genre]["liked"] += 1
        if platform:
            seen_platforms.setdefault(platform, {"liked": 0, "total": 0})
            seen_platforms[platform]["total"] += 1
            seen_platforms[platform]["liked"] += 1
        liked[gid] = info

    genre_weights = {}
    for g, counts in seen_genres.items():
        if counts["total"] > 0:
            genre_weights[g] = round(counts["liked"] / counts["total"], 2)

    platform_weights = {}
    for p, counts in seen_platforms.items():
        if counts["total"] > 0:
            platform_weights[p] = round(counts["liked"] / counts["total"], 2)

    # Year range from liked games only
    years = [info.get("year", 0) for info in liked.values() if info.get("year", 0) > 0]
    if len(years) >= 3:
        years_sorted = sorted(years)
        p10 = years_sorted[max(0, len(years_sorted) // 10)]
        p90 = years_sorted[min(len(years_sorted) - 1, 9 * len(years_sorted) // 10)]
        year_range = [p10, p90]
    elif years:
        year_range = [min(years), max(years)]
    else:
        year_range = [1980, 2010]

    # Stats
    stats = {"total": len(games), "skip": 0, "meh": 0, "good": 0, "exceptional": 0}
    for info in games.values():
        r = info.get("rating", "skip")
        if r in stats:
            stats[r] += 1

    ratings_data["profile"] = {
        "genre_weights": genre_weights,
        "year_range": year_range,
        "platform_weights": platform_weights,
        "total_played": len(liked),
        "total_rated": len(games),
    }
    ratings_data["stats"] = stats


def score_game_for_profile(game, profile):
    """Score an unrated game against user profile. Higher = more relevant."""
    score = 0.0
    gw = profile.get("genre_weights", {})
    pw = profile.get("platform_weights", {})
    yr = profile.get("year_range", [1980, 2010])

    if game.get("genre") in gw:
        score += gw[game["genre"]]
    if game.get("platform") in pw:
        score += pw[game["platform"]]
    if yr[0] <= game.get("year", 0) <= yr[1]:
        score += 0.3
    if game.get("rating_og", 0) >= 8:
        score += 0.2

    return score


def pick_profile_match(pool, profile):
    """Pick a game from pool weighted by profile relevance."""
    if not pool:
        return None
    scored = [(g, score_game_for_profile(g, profile)) for g in pool]
    scored.sort(key=lambda x: -x[1])
    top_n = max(1, len(scored) // 5)  # top 20%
    candidates = scored[:top_n]
    weights = [max(0.01, s) for _, s in candidates]
    chosen = random.choices([g for g, _ in candidates], weights=weights, k=1)[0]
    return chosen


def rebuild_pool_and_queue():
    """Rebuild unrated pool and refill queue."""
    global unrated_pool, game_queue
    rated_ids = set(ratings_data.get("games", {}).keys())
    unrated_pool = [g for g in catalog_index.get("games", []) if g["id"] not in rated_ids]
    random.shuffle(unrated_pool)
    refill_queue()


def refill_queue():
    """Top up the queue to QUEUE_SIZE."""
    global game_queue
    profile = ratings_data.get("profile", {})
    has_profile = profile.get("total_rated", 0) >= PROFILE_MIN_RATINGS

    while len(game_queue) < QUEUE_SIZE and unrated_pool:
        roll = random.random()
        if has_profile and roll < PROFILE_MATCH_RATIO:
            game = pick_profile_match(unrated_pool, profile)
            if game:
                source = "profile"
            else:
                game = unrated_pool[0]
                source = "random"
        else:
            game = random.choice(unrated_pool)
            source = "random"

        # Remove from pool, add to queue
        if game in unrated_pool:
            unrated_pool.remove(game)
        game_entry = {**game, "_source": source}
        # Avoid duplicates in queue
        if not any(q["id"] == game["id"] for q in game_queue):
            game_queue.append(game_entry)


def pop_next_game():
    """Pop the next game from queue, refill if needed."""
    if not game_queue:
        refill_queue()
    if not game_queue:
        return None
    game = game_queue.pop(0)
    if len(game_queue) < QUEUE_REFILL_THRESHOLD:
        refill_queue()
    return game


# --- Prefetch ---

def fetch_game_detail(game_id):
    """Fetch game detail using the appropriate source."""
    src_id, local_id = parse_game_id(game_id)
    source = SOURCES.get(src_id)
    if not source:
        return {"description": "", "screenshots": []}
    return source.parse_game_detail(local_id)


def prefetch_games():
    """Prefetch detail for next games in queue in background."""
    for i in range(min(PREFETCH_AHEAD, len(game_queue))):
        gid = game_queue[i]["id"]
        if gid not in prefetch_cache:
            try:
                detail = fetch_game_detail(gid)
                prefetch_cache[gid] = detail
            except Exception:
                pass


# --- Ratings Persistence ---

def load_ratings():
    global ratings_data
    if RATINGS_FILE.exists():
        try:
            ratings_data = json.loads(RATINGS_FILE.read_text(encoding="utf-8"))
        except:
            pass
    ratings_data.setdefault("wishlist", {})

    # Migrate legacy bare IDs to og: prefixed IDs
    if ratings_data.get("version", 1) < 2:
        migrated = False

        # Migrate games dict keys
        old_games = ratings_data.get("games", {})
        new_games = {}
        for gid, info in old_games.items():
            if ":" not in str(gid):
                new_games[make_game_id("og", gid)] = info
                migrated = True
            else:
                new_games[gid] = info
        if migrated:
            ratings_data["games"] = new_games

        # Migrate wishlist dict keys + backfill sourceUrl
        old_wl = ratings_data.get("wishlist", {})
        new_wl = {}
        for gid, info in old_wl.items():
            if ":" not in str(gid):
                new_gid = make_game_id("og", gid)
                new_wl[new_gid] = info
                migrated = True
            else:
                new_gid = gid
                new_wl[new_gid] = info
            # Backfill sourceUrl if missing
            if "sourceUrl" not in new_wl[new_gid]:
                sid, lid = parse_game_id(new_gid)
                src = SOURCES.get(sid)
                if src:
                    new_wl[new_gid]["sourceUrl"] = src.game_url(lid)
                    migrated = True
        if migrated or new_wl != old_wl:
            ratings_data["wishlist"] = new_wl

        # Migrate history entries
        old_hist = ratings_data.get("history", [])
        new_hist = []
        for h in old_hist:
            s = str(h)
            if ":" not in s:
                new_hist.append(make_game_id("og", s))
                migrated = True
            else:
                new_hist.append(s)
        if migrated:
            ratings_data["history"] = new_hist

        ratings_data["version"] = 2
        if migrated:
            save_ratings()
            print("Migrated ratings to source-prefixed IDs (version 2).")


def save_ratings():
    RATINGS_FILE.write_text(json.dumps(ratings_data, ensure_ascii=False, indent=1), encoding="utf-8")


def load_catalog_index():
    global catalog_index
    if CATALOG_INDEX_FILE.exists():
        try:
            catalog_index = json.loads(CATALOG_INDEX_FILE.read_text(encoding="utf-8"))
            print(f"Loaded catalog index: {catalog_index['totalGames']} games")
        except:
            pass

    # Migrate legacy IDs (bare integers) to og: prefixed IDs
    games = catalog_index.get("games", [])
    migrated = False
    for g in games:
        gid = str(g.get("id", ""))
        if ":" not in gid:
            g["id"] = make_game_id("og", gid)
            g.setdefault("sources", {"og": int(gid) if gid.isdigit() else gid})
            migrated = True
        # Rename ratingOG -> rating_og
        if "ratingOG" in g:
            g["rating_og"] = g.pop("ratingOG")
            migrated = True
        # Ensure sources field exists
        if "sources" not in g:
            _, lid = parse_game_id(g["id"])
            g["sources"] = {"og": int(lid) if lid.isdigit() else lid}
            migrated = True

    # Remove legacy maxPages field
    if "maxPages" in catalog_index:
        del catalog_index["maxPages"]
        migrated = True

    if migrated and games:
        catalog_index["games"] = games
        CATALOG_INDEX_FILE.write_text(json.dumps(catalog_index, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"Migrated catalog index to source-prefixed IDs.")


# --- HTTP Handler ---

class Handler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        # Suppress default logging except errors
        if args and "404" not in str(args[0]):
            return

    def send_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", len(body))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        params = dict(urllib.parse.parse_qsl(parsed.query))

        if path == "/" or path == "/index.html":
            self.serve_index()
        elif path == "/api/catalog/status":
            age = get_catalog_age_seconds()
            status = {
                **scan_progress,
                "scannedAt": catalog_index.get("scannedAt", ""),
                "totalGames": catalog_index.get("totalGames", 0),
                "ageDays": round(age / 86400, 1) if age != float("inf") else None,
                "stale": age > AUTO_UPDATE_SKIP_SEC,
            }
            self.send_json(status)
        elif path == "/api/next":
            self.handle_next()
        elif path.startswith("/api/game/"):
            m = re.match(r"/api/game/(.+)", path)
            if m:
                self.handle_game(m.group(1))
            else:
                self.send_json({"error": "bad id"}, 400)
        elif path == "/api/proxy":
            self.handle_proxy(params.get("url", ""))
        elif path == "/api/ratings":
            self.send_json(ratings_data)
        elif path == "/api/export":
            self.handle_export()
        else:
            self.send_json({"error": "not found"}, 404)

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length) if length else b"{}"

        if path == "/api/catalog/scan":
            self.handle_scan("full")
        elif path == "/api/catalog/update":
            self.handle_scan("incremental")
        elif path == "/api/ratings":
            self.handle_rate(body)
        else:
            self.send_json({"error": "not found"}, 404)

    def serve_index(self):
        global index_html_cache
        html_path = Path(__file__).parent / "index.html"
        if not html_path.exists():
            self.send_json({"error": "index.html not found"}, 500)
            return
        if index_html_cache is None:
            index_html_cache = html_path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", len(index_html_cache))
        self.end_headers()
        self.wfile.write(index_html_cache)

    def handle_scan(self, mode="full"):
        if scan_progress.get("scanning"):
            self.send_json({"status": "already scanning"})
            return
        # Mark scanning before starting thread to prevent poll race
        scan_progress.update({"scanning": True, "done": False, "scanned": 0, "total": 0})
        if mode == "incremental" and catalog_index.get("totalGames", 0) > 0:
            scan_progress["mode"] = "incremental"
            t = threading.Thread(target=incremental_scan, daemon=True)
            t.start()
            self.send_json({"status": "incremental update started"})
        else:
            scan_progress["mode"] = "full"
            t = threading.Thread(target=scan_catalog, daemon=True)
            t.start()
            self.send_json({"status": "full scan started"})

    def handle_next(self):
        game = pop_next_game()
        if not game:
            self.send_json({"error": "no games left", "done": True})
            return

        gid = game["id"]
        # Check prefetch cache
        if gid in prefetch_cache:
            detail = prefetch_cache.pop(gid)
        else:
            detail = fetch_game_detail(gid)

        result = {**game, **detail}

        # Add source URL and name
        src_id, local_id = parse_game_id(gid)
        source = SOURCES.get(src_id)
        if source:
            result["sourceUrl"] = source.game_url(local_id)
            result["sourceName"] = source.SOURCE_NAME

        self.send_json(result)

        # Trigger background prefetch
        threading.Thread(target=prefetch_games, daemon=True).start()

    def handle_game(self, game_id):
        # Normalize legacy bare IDs
        if ":" not in str(game_id):
            game_id = make_game_id("og", game_id)

        if game_id in prefetch_cache:
            detail = prefetch_cache.pop(game_id)
        else:
            detail = fetch_game_detail(game_id)
        # Merge with catalog info
        cat_entry = next((g for g in catalog_index.get("games", []) if g["id"] == game_id), {})
        result = {**cat_entry, **detail}

        # Add source URL and name
        src_id, local_id = parse_game_id(game_id)
        source = SOURCES.get(src_id)
        if source:
            result["sourceUrl"] = source.game_url(local_id)
            result["sourceName"] = source.SOURCE_NAME

        self.send_json(result)

    def handle_proxy(self, url):
        if not url:
            self.send_json({"error": "no url"}, 400)
            return
        parsed = urllib.parse.urlparse(url)
        domain = parsed.hostname or ""
        if domain not in ALLOWED_PROXY_DOMAINS:
            self.send_json({"error": f"domain not allowed: {domain}"}, 403)
            return
        try:
            resp = rate_limited_get(url)
            self.send_response(200)
            ct = resp.headers.get("Content-Type", "application/octet-stream")
            self.send_header("Content-Type", ct)
            self.send_header("Cache-Control", "public, max-age=86400")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(resp.content)
        except Exception as e:
            self.send_json({"error": str(e)}, 502)

    def handle_rate(self, body):
        try:
            data = json.loads(body)
        except:
            self.send_json({"error": "bad json"}, 400)
            return

        game_id = str(data.get("gameId", ""))
        rating = data.get("rating")  # skip/meh/good/exceptional or null for undo

        if not game_id:
            self.send_json({"error": "no gameId"}, 400)
            return

        # Normalize legacy bare IDs
        if ":" not in game_id:
            game_id = make_game_id("og", game_id)

        # Derive sourceUrl for wishlist entries
        src_id, local_id = parse_game_id(game_id)
        source = SOURCES.get(src_id)
        source_url = source.game_url(local_id) if source else ""

        # Wishlist toggle
        if data.get("wishlist") is not None:
            wl = ratings_data.setdefault("wishlist", {})
            if data["wishlist"]:
                cat_entry = next((g for g in catalog_index.get("games", []) if g["id"] == game_id), {})
                wl[game_id] = {
                    "name": cat_entry.get("name", data.get("name", "")),
                    "genre": cat_entry.get("genre", ""),
                    "year": cat_entry.get("year", 0),
                    "platform": cat_entry.get("platform", ""),
                    "sourceUrl": source_url,
                    "ts": int(time.time()),
                }
            else:
                wl.pop(game_id, None)
            compute_profile()  # Wishlist affects profile
            save_ratings()
            self.send_json({"ok": True, "wishlist": ratings_data.get("wishlist", {}), "profile": ratings_data.get("profile")})
            return

        if rating is None:
            # Undo
            if game_id in ratings_data["games"]:
                removed = ratings_data["games"].pop(game_id)
                # Add back to pool and front of queue
                cat_entry = next((g for g in catalog_index.get("games", []) if g["id"] == game_id), None)
                if cat_entry:
                    unrated_pool.insert(0, cat_entry)
                    game_queue.insert(0, {**cat_entry, "_source": "undo"})
                hist = ratings_data.get("history", [])
                for i in range(len(hist) - 1, -1, -1):
                    if str(hist[i]) == game_id:
                        hist.pop(i)
                        break
        else:
            # Find game info from catalog
            cat_entry = next((g for g in catalog_index.get("games", []) if g["id"] == game_id), {})
            ratings_data["games"][game_id] = {
                "rating": rating,
                "name": cat_entry.get("name", data.get("name", "")),
                "genre": cat_entry.get("genre", ""),
                "year": cat_entry.get("year", 0),
                "platform": cat_entry.get("platform", ""),
                "ts": int(time.time()),
            }
            ratings_data.setdefault("history", []).append(game_id)

        compute_profile()
        save_ratings()
        self.send_json({"ok": True, "stats": ratings_data.get("stats"), "profile": ratings_data.get("profile")})

    def handle_export(self):
        wl = ratings_data.get("wishlist", {})
        lines = ["id,name,genre,year,platform,rating,wishlisted,timestamp"]
        for gid, info in ratings_data.get("games", {}).items():
            name = info.get("name", "").replace('"', '""')
            wishlisted = "yes" if gid in wl else ""
            lines.append(f'{gid},"{name}",{info.get("genre","")},{info.get("year","")},{info.get("platform","")},{info.get("rating","")},{wishlisted},{info.get("ts","")}')
        # Add wishlist-only entries (not yet rated)
        for gid, info in wl.items():
            if gid not in ratings_data.get("games", {}):
                name = info.get("name", "").replace('"', '""')
                lines.append(f'{gid},"{name}",{info.get("genre","")},{info.get("year","")},{info.get("platform","")},,yes,{info.get("ts","")}')
        body = "\n".join(lines).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/csv; charset=utf-8")
        self.send_header("Content-Disposition", "attachment; filename=og-recall-export.csv")
        self.end_headers()
        self.wfile.write(body)


def main():
    CACHE_DIR.mkdir(exist_ok=True)
    load_ratings()
    load_catalog_index()

    if catalog_index.get("totalGames", 0) > 0:
        compute_profile()
        rebuild_pool_and_queue()
        scan_progress["done"] = True
        scan_progress["total"] = 1
        scan_progress["scanned"] = 1

        # Auto-update check
        age = get_catalog_age_seconds()
        if age < AUTO_UPDATE_SKIP_SEC:
            print(f"Catalog is fresh ({age/3600:.0f}h old), skipping update.")
        else:
            print(f"Catalog is {age/86400:.1f} days old, running background incremental update...")
            threading.Thread(target=incremental_scan, daemon=True).start()

    # No catalog at all — auto-start full scan in background
    if catalog_index.get("totalGames", 0) == 0:
        print("No catalog index found. Starting full scan in background...")
        scan_progress.update({"scanning": True, "done": False, "scanned": 0, "total": 0, "mode": "full"})
        threading.Thread(target=scan_catalog, daemon=True).start()

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"OG Recall server running at http://localhost:{PORT}")
    if not catalog_index.get("totalGames"):
        print("Scan in progress — open browser to see progress.")
    else:
        print(f"Catalog loaded: {catalog_index['totalGames']} games, {len(ratings_data.get('games', {}))} rated")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        save_ratings()


if __name__ == "__main__":
    main()
