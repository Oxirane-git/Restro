"""
tools/google_maps_search.py

Searches Google Maps Places API (New) for a business niche in a given city.
Paginates up to 3 pages (max 60 results). Writes results to .tmp/.

Usage:
    python tools/google_maps_search.py --niche "cafes" --city "New York City, USA"

Output:
    .tmp/raw_places_cafes_new-york-city-usa.json
"""

import argparse
import json
import os
import re
import time

import requests
from dotenv import load_dotenv

PLACES_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
MAX_PAGES = 3
PAGE_SIZE = 20
INTER_PAGE_DELAY = 2.0  # seconds — Google requires token settle time between pages
FIELD_MASK = (
    "places.id,"
    "places.displayName,"
    "places.formattedAddress,"
    "places.nationalPhoneNumber,"
    "places.websiteUri,"
    "places.types"
)


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def search_places(api_key: str, niche: str, city: str) -> list[dict]:
    query = f"{niche} in {city}"
    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": FIELD_MASK,
    }

    all_places = []
    page_token = None

    for page_num in range(1, MAX_PAGES + 1):
        body = {"textQuery": query, "pageSize": PAGE_SIZE}
        if page_token:
            body["pageToken"] = page_token

        try:
            resp = requests.post(PLACES_SEARCH_URL, headers=headers, json=body, timeout=15)
            resp.raise_for_status()
            data = resp.json()
        except requests.exceptions.Timeout:
            print(f"  [WARN] Timeout on page {page_num} for {city}. Stopping pagination.")
            break
        except requests.exceptions.HTTPError:
            status = resp.status_code
            print(f"  [ERROR] HTTP {status} on page {page_num}")
            if status == 429:
                print("  [WARN] Rate limit hit. Waiting 60s before retry...")
                time.sleep(60)
                try:
                    resp = requests.post(PLACES_SEARCH_URL, headers=headers, json=body, timeout=15)
                    resp.raise_for_status()
                    data = resp.json()
                except Exception:
                    print("  [ERROR] Retry failed. Skipping remaining pages for this city.")
                    break
            else:
                break

        places_raw = data.get("places", [])
        if not places_raw:
            print(f"  [INFO] No results on page {page_num}. Stopping.")
            break

        for p in places_raw:
            all_places.append({
                "place_id": p.get("id", ""),
                "business_name": p.get("displayName", {}).get("text", ""),
                "address": p.get("formattedAddress", ""),
                "phone": p.get("nationalPhoneNumber", ""),
                "website": p.get("websiteUri", ""),
                "types": p.get("types", []),
                "city": city,
                "email": "",
                "owner_name": "",
            })

        print(f"  [INFO] Page {page_num}: {len(places_raw)} results (total: {len(all_places)})")

        page_token = data.get("nextPageToken")
        if not page_token:
            break

        if page_num < MAX_PAGES:
            time.sleep(INTER_PAGE_DELAY)

    return all_places


def main():
    load_dotenv()
    api_key = os.getenv("GOOGLE_MAPS_API_KEY")
    if not api_key:
        raise SystemExit("[ERROR] GOOGLE_MAPS_API_KEY not set in .env")

    parser = argparse.ArgumentParser()
    parser.add_argument("--niche", required=True, help="Business type, e.g. 'cafes'")
    parser.add_argument("--city", required=True, help="City name, e.g. 'New York City, USA'")
    args = parser.parse_args()

    niche_slug = slugify(args.niche)
    city_slug = slugify(args.city)
    output_path = f".tmp/raw_places_{niche_slug}_{city_slug}.json"

    if os.path.exists(output_path) and os.path.getsize(output_path) > 10:
        print(f"[SKIP] {output_path} already exists. Delete it to re-run.")
        return

    print(f"[INFO] Searching: '{args.niche}' in '{args.city}'")
    places = search_places(api_key, args.niche, args.city)

    os.makedirs(".tmp", exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(places, f, indent=2, ensure_ascii=False)

    print(f"[DONE] Wrote {len(places)} records to {output_path}")


if __name__ == "__main__":
    main()
