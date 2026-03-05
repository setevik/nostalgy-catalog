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

# --- Config ---
PORT = 8765
OG_BASE = "https://www.old-games.ru"
STATIC_BASE = "https://static.old-games.ru"
ALLOWED_PROXY_DOMAINS = {"old-games.ru", "www.old-games.ru", "static.old-games.ru"}
RATE_LIMIT_SEC = 1.0
CACHE_DIR = Path("cache")
CATALOG_INDEX_FILE = Path("catalog_index.json")
RATINGS_FILE = Path("ratings.json")
QUEUE_SIZE = 20
QUEUE_REFILL_THRESHOLD = 10
PROFILE_MIN_RATINGS = 10
PROFILE_MATCH_RATIO = 0.30
PREFETCH_AHEAD = 2

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


def fetch_cached_permanent(url):
    return fetch_cached(url, max_age=365 * 86400)


# --- Catalog Parsing ---

def parse_catalog_page(html):
    """Parse a catalog page, return list of game dicts."""
    soup = BeautifulSoup(html, "html.parser")
    games = []
    # Find game links matching /game/{id}.html
    for link in soup.find_all("a", href=re.compile(r"^/game/(\d+)\.html$")):
        m = re.match(r"^/game/(\d+)\.html$", link.get("href", ""))
        if not m:
            continue
        gid = int(m.group(1))
        name = link.get_text(strip=True)
        if not name or len(name) < 2:
            continue

        # Walk up to find the table row
        row = link.find_parent("tr")
        if not row:
            # Try parent table structure
            row = link.find_parent("td")
            if row:
                row = row.find_parent("tr")
        if not row:
            continue

        # Extract metadata from sibling cells/links
        genre = ""
        year = 0
        platform = ""
        publisher = ""
        rating_og = 0

        for a in row.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if "genre=" in href:
                genre = text
            elif "year=" in href:
                try:
                    year = int(re.search(r"\d{4}", text).group())
                except:
                    pass
            elif "platform=" in href:
                platform = text
            elif "publisherCompany=" in href:
                publisher = text

        # Rating from title or text
        for el in row.find_all(string=re.compile(r"Оценка рецензента")):
            m2 = re.search(r"(\d+)\s*из\s*10", el)
            if m2:
                rating_og = int(m2.group(1))
        if not rating_og:
            for img in row.find_all("img", alt=re.compile(r"Оценка")):
                m2 = re.search(r"(\d+)\s*из\s*10", img.get("alt", ""))
                if m2:
                    rating_og = int(m2.group(1))

        # Alt names
        alt_names = []
        td = link.find_parent("td")
        if td:
            for br in td.find_all("br"):
                sib = br.next_sibling
                if sib and isinstance(sib, str) and sib.strip():
                    alt_names.append(sib.strip())

        # Avoid duplicates
        if any(g["id"] == gid for g in games):
            continue

        games.append({
            "id": gid,
            "name": name,
            "altNames": alt_names,
            "genre": genre,
            "year": year,
            "platform": platform,
            "publisher": publisher,
            "ratingOG": rating_og,
        })
    return games


def get_max_page(html):
    """Get max page number from catalog pagination."""
    soup = BeautifulSoup(html, "html.parser")
    max_page = 1
    for a in soup.find_all("a", href=re.compile(r"page=\d+")):
        m = re.search(r"page=(\d+)", a.get("href", ""))
        if m:
            max_page = max(max_page, int(m.group(1)))
    return max_page


def scan_catalog():
    """Scan all catalog pages to build full index."""
    global catalog_index, scan_progress
    scan_progress = {"total": 0, "scanned": 0, "done": False, "scanning": True}

    try:
        # First page to get total pages
        url = f"{OG_BASE}/catalog/?sort=name&page=1"
        html = fetch_cached_permanent(url)
        max_page = get_max_page(html)
        scan_progress["total"] = max_page

        all_games = parse_catalog_page(html)
        scan_progress["scanned"] = 1

        seen_ids = {g["id"] for g in all_games}

        for page in range(2, max_page + 1):
            url = f"{OG_BASE}/catalog/?sort=name&page={page}"
            try:
                html = fetch_cached_permanent(url)
                games = parse_catalog_page(html)
                for g in games:
                    if g["id"] not in seen_ids:
                        all_games.append(g)
                        seen_ids.add(g["id"])
            except Exception as e:
                print(f"  Warning: page {page} failed: {e}")
            scan_progress["scanned"] = page

        catalog_index = {
            "scannedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "totalGames": len(all_games),
            "games": all_games,
        }
        CATALOG_INDEX_FILE.write_text(json.dumps(catalog_index, ensure_ascii=False, indent=1), encoding="utf-8")
        print(f"Catalog scan complete: {len(all_games)} games indexed.")
    except Exception as e:
        print(f"Catalog scan error: {e}")
    finally:
        scan_progress["done"] = True
        scan_progress["scanning"] = False
        rebuild_pool_and_queue()


# --- Game Detail Parsing ---

def parse_game_detail(game_id):
    """Fetch and parse game detail + screenshots."""
    result = {"id": game_id, "description": "", "screenshots": []}

    # Screenshots page
    try:
        url = f"{OG_BASE}/game/screenshots/{game_id}.html"
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
                # Make absolute
                if thumb.startswith("/"):
                    thumb = OG_BASE + thumb
                elif not thumb.startswith("http"):
                    thumb = OG_BASE + "/" + thumb
                if full.startswith("/"):
                    full = OG_BASE + full
                elif not full.startswith("http"):
                    full = OG_BASE + "/" + full
                result["screenshots"].append({"thumb": thumb, "full": full})
    except Exception as e:
        print(f"  Screenshots fetch failed for {game_id}: {e}")

    # Game page for description
    try:
        url = f"{OG_BASE}/game/{game_id}.html"
        html = fetch_cached(url)
        soup = BeautifulSoup(html, "html.parser")

        desc = ""
        # Strategy 1: div with review/description/text class
        for div in soup.find_all("div", class_=re.compile(r"review|description|text|content")):
            text = div.get_text(separator="\n", strip=True)
            if len(text) > 50 and len(text) > len(desc):
                desc = text

        # Strategy 2: look for the main content area after the game info table
        if not desc:
            for h1 in soup.find_all("h1"):
                parent = h1.find_parent("div")
                if parent:
                    # Get all paragraph-like text blocks after the header area
                    for p in parent.find_all(["p", "div"]):
                        text = p.get_text(strip=True)
                        if len(text) > 100 and not any(skip in text.lower() for skip in ["каталог", "регистрация", "форум", "скачать"]):
                            if len(text) > len(desc):
                                desc = text

        # Strategy 3: longest text block on page that looks like a description
        if not desc:
            for el in soup.find_all(["p", "div", "td"]):
                text = el.get_text(strip=True)
                if 100 < len(text) < 5000 and not any(skip in text.lower() for skip in ["каталог", "регистрация", "cookie", "вход"]):
                    if len(text) > len(desc):
                        desc = text

        result["description"] = desc[:2000] if desc else ""
    except Exception as e:
        print(f"  Detail fetch failed for {game_id}: {e}")

    return result


# --- Profile & Queue ---

def compute_profile():
    """Recompute user profile from ratings."""
    games = ratings_data.get("games", {})
    played = {}  # non-skip ratings
    seen_genres = {}
    seen_platforms = {}

    for gid, info in games.items():
        genre = info.get("genre", "")
        platform = info.get("platform", "")
        is_played = info.get("rating") != "skip"

        if genre:
            seen_genres.setdefault(genre, {"played": 0, "total": 0})
            seen_genres[genre]["total"] += 1
            if is_played:
                seen_genres[genre]["played"] += 1

        if platform:
            seen_platforms.setdefault(platform, {"played": 0, "total": 0})
            seen_platforms[platform]["total"] += 1
            if is_played:
                seen_platforms[platform]["played"] += 1

        if is_played:
            played[gid] = info

    genre_weights = {}
    for g, counts in seen_genres.items():
        if counts["total"] > 0:
            genre_weights[g] = round(counts["played"] / counts["total"], 2)

    platform_weights = {}
    for p, counts in seen_platforms.items():
        if counts["total"] > 0:
            platform_weights[p] = round(counts["played"] / counts["total"], 2)

    years = [info.get("year", 0) for info in played.values() if info.get("year", 0) > 0]
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
        "total_played": len(played),
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
    if game.get("ratingOG", 0) >= 8:
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
    unrated_pool = [g for g in catalog_index.get("games", []) if str(g["id"]) not in rated_ids]
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

def prefetch_games():
    """Prefetch detail for next games in queue in background."""
    for i in range(min(PREFETCH_AHEAD, len(game_queue))):
        gid = game_queue[i]["id"]
        if gid not in prefetch_cache:
            try:
                detail = parse_game_detail(gid)
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
            self.send_json(scan_progress)
        elif path == "/api/next":
            self.handle_next()
        elif path.startswith("/api/game/"):
            m = re.match(r"/api/game/(\d+)", path)
            if m:
                self.handle_game(int(m.group(1)))
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
            self.handle_scan()
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

    def handle_scan(self):
        if scan_progress.get("scanning"):
            self.send_json({"status": "already scanning"})
            return
        t = threading.Thread(target=scan_catalog, daemon=True)
        t.start()
        self.send_json({"status": "scan started"})

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
            detail = parse_game_detail(gid)

        result = {**game, **detail}
        self.send_json(result)

        # Trigger background prefetch
        threading.Thread(target=prefetch_games, daemon=True).start()

    def handle_game(self, game_id):
        if game_id in prefetch_cache:
            detail = prefetch_cache.pop(game_id)
        else:
            detail = parse_game_detail(game_id)
        # Merge with catalog info
        cat_entry = next((g for g in catalog_index.get("games", []) if g["id"] == game_id), {})
        result = {**cat_entry, **detail}
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

        # Wishlist toggle
        if data.get("wishlist") is not None:
            wl = ratings_data.setdefault("wishlist", {})
            if data["wishlist"]:
                cat_entry = next((g for g in catalog_index.get("games", []) if str(g["id"]) == game_id), {})
                wl[game_id] = {
                    "name": cat_entry.get("name", data.get("name", "")),
                    "genre": cat_entry.get("genre", ""),
                    "year": cat_entry.get("year", 0),
                    "platform": cat_entry.get("platform", ""),
                    "ts": int(time.time()),
                }
            else:
                wl.pop(game_id, None)
            save_ratings()
            self.send_json({"ok": True, "wishlist": ratings_data.get("wishlist", {})})
            return

        if rating is None:
            # Undo
            if game_id in ratings_data["games"]:
                removed = ratings_data["games"].pop(game_id)
                # Add back to pool and front of queue
                cat_entry = next((g for g in catalog_index.get("games", []) if str(g["id"]) == game_id), None)
                if cat_entry:
                    unrated_pool.insert(0, cat_entry)
                    game_queue.insert(0, {**cat_entry, "_source": "undo"})
                if game_id in [str(h) for h in ratings_data.get("history", [])]:
                    hist = ratings_data.get("history", [])
                    # Remove last occurrence
                    for i in range(len(hist) - 1, -1, -1):
                        if str(hist[i]) == game_id:
                            hist.pop(i)
                            break
        else:
            # Find game info from catalog
            cat_entry = next((g for g in catalog_index.get("games", []) if str(g["id"]) == game_id), {})
            ratings_data["games"][game_id] = {
                "rating": rating,
                "name": cat_entry.get("name", data.get("name", "")),
                "genre": cat_entry.get("genre", ""),
                "year": cat_entry.get("year", 0),
                "platform": cat_entry.get("platform", ""),
                "ts": int(time.time()),
            }
            ratings_data.setdefault("history", []).append(int(game_id))

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

    server = HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"OG Recall server running at http://localhost:{PORT}")
    if not catalog_index.get("totalGames"):
        print("No catalog index found. Open browser and click 'Start Scan' or POST /api/catalog/scan")
    else:
        print(f"Catalog loaded: {catalog_index['totalGames']} games, {len(ratings_data.get('games', {}))} rated")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down.")
        save_ratings()


if __name__ == "__main__":
    main()
