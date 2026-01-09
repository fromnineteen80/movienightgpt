import os
import re
import json
import time
import random
import requests
from datetime import datetime, timezone

YOUTUBE_API_KEY = os.environ.get("YOUTUBE_API_KEY", "").strip()
TMDB_API_KEY = os.environ.get("TMDB_API_KEY", "").strip()

if not YOUTUBE_API_KEY:
    raise SystemExit("Missing YOUTUBE_API_KEY secret")
if not TMDB_API_KEY:
    raise SystemExit("Missing TMDB_API_KEY secret")

OUT_PATH = "data/today.json"

ROW_TITLES = ["Recently Uploaded", "Popular", "Political", "War", "History"]
ROW_QUERIES = {
    "Recently Uploaded": ["political thriller full movie", "war full movie", "historical drama full movie", "history war full movie"],
    "Popular": ["political thriller full movie", "war full movie", "historical drama full movie", "history war full movie"],
    "Political": ["political thriller full movie", "political drama full movie", "conspiracy thriller full movie", "spy political thriller full movie"],
    "War": ["war full movie", "ww2 full movie", "vietnam war full movie", "military thriller full movie"],
    "History": ["historical drama full movie", "based on true events full movie", "period drama full movie", "history war drama full movie"],
}

MIN_MINUTES = 75
EXCLUDE_TITLE_RE = re.compile(r"\bdocumentary\b", re.IGNORECASE)
YT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")

# Wikidata award Q-ids for "major" Oscar / Globe wins (film-level awards only).
# These are intentionally conservative. If present, we show a star badge.
WIKIDATA_AWARD_QIDS = {
    # Academy Awards
    "Q103360": "Oscar Best Picture",
    "Q106291": "Oscar Best Director",
    "Q1033603": "Oscar Best Original Screenplay",
    "Q1033604": "Oscar Best Adapted Screenplay",
    # Golden Globes (film awards)
    "Q106301": "Golden Globe Best Motion Picture (Drama)",
    "Q106295": "Golden Globe Best Motion Picture (Musical/Comedy)",
    "Q106296": "Golden Globe Best Director",
    "Q106297": "Golden Globe Best Screenplay",
}

def iso8601_duration_to_minutes(d: str) -> int:
    # PT2H10M, PT95M, PT1H
    h = m = s = 0
    mobj = re.match(r"^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$", d or "")
    if not mobj:
        return 0
    if mobj.group(1): h = int(mobj.group(1))
    if mobj.group(2): m = int(mobj.group(2))
    if mobj.group(3): s = int(mobj.group(3))
    return h * 60 + m + (1 if s >= 30 else 0)

def yt_search(query: str, order: str, page_token: str | None = None, max_results: int = 25) -> dict:
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet",
        "q": query,
        "type": "video",
        "maxResults": max_results,
        "order": order,                 # date, viewCount, relevance
        "videoDuration": "long",        # 20+ minutes, still must verify duration
        "safeSearch": "none",
    }
    if page_token:
        params["pageToken"] = page_token
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def yt_videos(video_ids: list[str]) -> list[dict]:
    url = "https://www.googleapis.com/youtube/v3/videos"
    params = {
        "key": YOUTUBE_API_KEY,
        "part": "snippet,contentDetails,statistics",
        "id": ",".join(video_ids),
        "maxResults": 50,
    }
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    return r.json().get("items", [])

def pick_best_thumbnail(snippet: dict) -> str:
    thumbs = (snippet.get("thumbnails") or {})
    for k in ["maxres", "standard", "high", "medium", "default"]:
        if k in thumbs and thumbs[k].get("url"):
            return thumbs[k]["url"]
    return ""

def clean_title_for_tmdb(title: str) -> str:
    t = title or ""
    # Remove common junk
    t = re.sub(r"\b(full movie|full-film|full film|hd|4k|1080p|720p|english)\b", "", t, flags=re.I)
    t = re.sub(r"[\[\]\(\)\|].*?[\[\]\(\)\|]", " ", t)  # remove bracketed phrases
    t = re.sub(r"\s+", " ", t).strip()
    return t[:120]

def tmdb_search_movie(query: str) -> dict | None:
    url = "https://api.themoviedb.org/3/search/movie"
    params = {"api_key": TMDB_API_KEY, "query": query, "include_adult": "false"}
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    results = r.json().get("results", [])
    return results[0] if results else None

def tmdb_movie_details(movie_id: int) -> dict:
    url = f"https://api.themoviedb.org/3/movie/{movie_id}"
    params = {"api_key": TMDB_API_KEY, "append_to_response": "credits,external_ids"}
    r = requests.get(url, params=params, timeout=25)
    r.raise_for_status()
    return r.json()

def wikidata_awards_for_film(imdb_id: str) -> list[str]:
    if not imdb_id:
        return []
    # Query awards (P166) for film identified by IMDb ID (P345)
    endpoint = "https://query.wikidata.org/sparql"
    q = f"""
    SELECT ?award WHERE {{
      ?film wdt:P345 "{imdb_id}" .
      ?film wdt:P166 ?award .
    }}
    """
    headers = {"User-Agent": "MovieNightGPT/1.0 (GitHub Actions)"}
    r = requests.get(endpoint, params={"format": "json", "query": q}, headers=headers, timeout=25)
    if r.status_code != 200:
        return []
    data = r.json()
    out = []
    for b in data.get("results", {}).get("bindings", []):
        uri = b.get("award", {}).get("value", "")
        qid = uri.rsplit("/", 1)[-1]
        if qid in WIKIDATA_AWARD_QIDS:
            out.append(WIKIDATA_AWARD_QIDS[qid])
    # Dedup
    return sorted(list(dict.fromkeys(out)))

def build_item_from_video(v: dict) -> dict | None:
    vid = v.get("id", "")
    if not YT_ID_RE.match(vid):
        return None

    snippet = v.get("snippet") or {}
    title = (snippet.get("title") or "").strip()
    if not title or EXCLUDE_TITLE_RE.search(title):
        return None

    minutes = iso8601_duration_to_minutes((v.get("contentDetails") or {}).get("duration", ""))
    if minutes < MIN_MINUTES:
        return None

    # TMDB metadata
    tmdb_title = clean_title_for_tmdb(title)
    year = ""
    director = ""
    leads = []
    poster_url = ""

    awards = []

    tmdb_hit = None
    if tmdb_title:
        tmdb_hit = tmdb_search_movie(tmdb_title)

    if tmdb_hit and tmdb_hit.get("id"):
        details = tmdb_movie_details(int(tmdb_hit["id"]))
        release_date = details.get("release_date") or ""
        if release_date[:4].isdigit():
            year = release_date[:4]

        # Director
        crew = ((details.get("credits") or {}).get("crew") or [])
        directors = [c for c in crew if (c.get("job") == "Director" and c.get("name"))]
        if directors:
            director = directors[0]["name"]

        # Leads: top 2 billed cast names
        cast = ((details.get("credits") or {}).get("cast") or [])
        leads = [c.get("name") for c in cast[:2] if c.get("name")]
        leads = leads[:2]

        # Poster
        poster_path = details.get("poster_path")
        if poster_path:
            poster_url = f"https://image.tmdb.org/t/p/w500{poster_path}"

        # Awards via Wikidata using IMDb ID
        imdb_id = ((details.get("external_ids") or {}).get("imdb_id") or "").strip()
        awards = wikidata_awards_for_film(imdb_id)

        # Better title if TMDB has it
        if details.get("title"):
            title = details["title"]

    if not poster_url:
        poster_url = pick_best_thumbnail(snippet)

    return {
        "title": title,
        "year": year,
        "director": director,
        "leads": leads,
        "posterUrl": poster_url,
        "youtubeId": vid,
        "awards": awards,
    }

def fill_row(row_name: str, order: str, seen_ids: set[str]) -> list[dict]:
    queries = ROW_QUERIES[row_name]
    items: list[dict] = []
    random.shuffle(queries)

    for q in queries:
        page_token = None
        for _ in range(4):  # paginate a bit if needed
            data = yt_search(q, order=order, page_token=page_token, max_results=25)
            ids = []
            for it in data.get("items", []):
                vid = ((it.get("id") or {}).get("videoId") or "").strip()
                if vid and vid not in seen_ids:
                    ids.append(vid)

            ids = ids[:50]
            if not ids:
                page_token = data.get("nextPageToken")
                if not page_token:
                    break
                continue

            vids = yt_videos(ids)
            for v in vids:
                vid = v.get("id", "")
                if vid in seen_ids:
                    continue
                item = build_item_from_video(v)
                if not item:
                    continue
                seen_ids.add(vid)
                items.append(item)
                if len(items) >= 4:
                    return items

            page_token = data.get("nextPageToken")
            if not page_token:
                break

            time.sleep(0.2)

        if len(items) >= 4:
            break

    return items

def main():
    os.makedirs("data", exist_ok=True)

    seen_ids: set[str] = set()
    all_items: list[dict] = []

    for row in ROW_TITLES:
        if row == "Recently Uploaded":
            order = "date"
        elif row == "Popular":
            order = "viewCount"
        else:
            order = "relevance"

        row_items = fill_row(row, order=order, seen_ids=seen_ids)

        # Hard requirement: 4 per row. If short, try again with generic fallback queries.
        if len(row_items) < 4:
            fallback_pool = ["full movie", "political thriller full movie", "war full movie", "historical drama full movie"]
            for fb in fallback_pool:
                if len(row_items) >= 4:
                    break
                data = yt_search(fb, order=order, max_results=25)
                ids = []
                for it in data.get("items", []):
                    vid = ((it.get("id") or {}).get("videoId") or "").strip()
                    if vid and vid not in seen_ids:
                        ids.append(vid)
                vids = yt_videos(ids[:50])
                for v in vids:
                    if len(row_items) >= 4:
                        break
                    vid = v.get("id", "")
                    if vid in seen_ids:
                        continue
                    item = build_item_from_video(v)
                    if not item:
                        continue
                    seen_ids.add(vid)
                    row_items.append(item)

        # If still short, fail loudly so you know instead of committing empty.
        if len(row_items) < 4:
            raise SystemExit(f"Could not fill row '{row}' with 4 valid movies. Found {len(row_items)}.")

        all_items.extend(row_items)

    payload = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "criteria": "Public daily mix: drama, history, war, political thrillers. Secondary action thrillers.",
        "row_titles": ROW_TITLES,
        "items": all_items,
    }

    with open(OUT_PATH, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    print(f"Wrote {OUT_PATH} with {len(all_items)} items.")

if __name__ == "__main__":
    main()
