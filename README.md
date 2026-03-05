# OG Recall

Tinder-style memory triage for the [old-games.ru](https://www.old-games.ru) catalog.
Rapidly scroll through ~5000+ retro games, see screenshots to jog your memory,
and tag each one: **Skip** / **Meh** / **Good** / **Exceptional**.

## Quick Start

```bash
pip install -r requirements.txt
python server.py
```

Open **http://localhost:8765** in your browser.

On first launch, click **Start Scan** to index the catalog (~3-5 minutes, one-time).
After that, games appear in shuffled order with screenshots — rate and move on.

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `S` | Skip (never played / don't recognize) |
| `1` | Meh |
| `2` | Good |
| `3` | Exceptional |
| `←` / `→` | Browse screenshots |
| `U` | Undo last rating |
| `T` | Toggle stats panel |
| `Space` | Open game on old-games.ru |
| `E` | Export ratings as CSV |

## How the Queue Works

- **70% random** — any unrated game from the catalog
- **30% profile-matched** — biased toward genres, years, and platforms you tend to play (kicks in after 10 ratings)
- A **🎯 For You** badge appears when the current game was profile-selected

## Files

| File | Purpose |
|------|---------|
| `server.py` | Local HTTP server, scraper, queue engine |
| `index.html` | Full UI (single file) |
| `ratings.json` | Your progress + taste profile (auto-created) |
| `catalog_index.json` | Full game index (auto-created on first scan) |
| `cache/` | Cached HTML pages from old-games.ru |

## Notes

- Polite scraping: 1 request/sec rate limit to old-games.ru
- All data stays local — no external services, no accounts
- The server prefetches the next 2 games in the background for instant loading
