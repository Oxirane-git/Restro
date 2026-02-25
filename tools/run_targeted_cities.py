"""
tools/run_targeted_cities.py

Runs google_maps_search + scrape_website_emails for a list of niches
across a fixed set of new cities. Skips already-processed city/niche combos.

Usage:
    python3 tools/run_targeted_cities.py --category cafes
    python3 tools/run_targeted_cities.py --category restaurants
"""

import argparse
import os
import re
import subprocess
import sys
import time

NEW_CITIES = [
    "San Francisco, USA",
    "Boston, USA",
    "Seattle, USA",
    "Birmingham, UK",
    "Edinburgh, UK",
    "Glasgow, UK",
    "Montreal, Canada",
    "Calgary, Canada",
    "Brisbane, Australia",
    "Perth, Australia",
    "Cork, Ireland",
    "Wellington, New Zealand",
    "Delhi, India",
    "Bangalore, India",
    "Hyderabad, India",
    "Chennai, India",
    "Johannesburg, South Africa",
    "Durban, South Africa",
    "Abu Dhabi, UAE",
    "Abuja, Nigeria",
    "Manila, Philippines",
    "Cebu City, Philippines",
    "Valletta, Malta",
    "Hong Kong",
    "Nairobi, Kenya",
    "Accra, Ghana",
    "Karachi, Pakistan",
    "Islamabad, Pakistan",
    "Harare, Zimbabwe",
    "Kingston, Jamaica",
    "Port of Spain, Trinidad and Tobago",
    "Nassau, Bahamas",
]

CAFE_NICHES    = ["cafes", "coffee shops", "espresso bars"]
RESTAURANT_NICHES = ["restaurants", "bistro", "diner"]

INTER_CITY_DELAY = 1.5


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def run(cmd: list) -> int:
    return subprocess.run(cmd, text=True).returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--category", required=True, choices=["cafes", "restaurants"])
    args = parser.parse_args()

    niches = CAFE_NICHES if args.category == "cafes" else RESTAURANT_NICHES
    cities = NEW_CITIES
    total  = len(cities) * len(niches)
    done   = 0

    print(f"\n{'='*60}")
    print(f" TARGET RUN: {args.category} — {len(niches)} niches × {len(cities)} cities = {total} jobs")
    print(f"{'='*60}\n")

    for city in cities:
        city_slug = slugify(city)
        for niche in niches:
            niche_slug = slugify(niche)
            done += 1
            raw_path = f".tmp/raw_places_{niche_slug}_{city_slug}.json"

            print(f"\n[{done}/{total}] '{niche}' in '{city}'")

            # Step 1: Maps search
            if os.path.exists(raw_path) and os.path.getsize(raw_path) > 10:
                print(f"  [SKIP] Raw file exists.")
            else:
                rc = run(["python3", "tools/google_maps_search.py", "--niche", niche, "--city", city])
                if rc != 0:
                    print(f"  [WARN] Maps search failed. Skipping.")
                    time.sleep(INTER_CITY_DELAY)
                    continue

            # Step 2: Scrape emails
            enriched_path = f".tmp/enriched_{niche_slug}_{city_slug}.json"
            if os.path.exists(enriched_path) and os.path.getsize(enriched_path) > 10:
                print(f"  [SKIP] Enriched file exists.")
            else:
                rc = run(["python3", "tools/scrape_website_emails.py", "--input", raw_path])
                if rc != 0:
                    print(f"  [WARN] Scraping failed.")

            time.sleep(INTER_CITY_DELAY)

    print(f"\n{'='*60}")
    print(f" DONE — {args.category} across {len(cities)} new cities.")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
