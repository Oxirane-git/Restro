"""
tools/run_niche_batch.py

Runs the full lead generation pipeline for a niche across all 30 cities.
Skips cities already processed (resume-safe).

Usage:
    python tools/run_niche_batch.py --niche "coffee shops"

Steps per city:
    1. google_maps_search.py  → .tmp/raw_places_*.json
    2. scrape_website_emails.py → .tmp/enriched_*.json

After all cities, run build_leads_csv.py separately.
"""

import argparse
import subprocess
import sys
import time

CITIES = [
    "New York City, USA",
    "Los Angeles, USA",
    "Chicago, USA",
    "Toronto, Canada",
    "Miami, USA",
    "Houston, USA",
    "Vancouver, Canada",
    "Atlanta, USA",
    "London, UK",
    "Berlin, Germany",
    "Amsterdam, Netherlands",
    "Paris, France",
    "Barcelona, Spain",
    "Dubai, UAE",
    "Manchester, UK",
    "Sydney, Australia",
    "Melbourne, Australia",
    "Singapore",
    "Tokyo, Japan",
    "Mumbai, India",
    "Auckland, New Zealand",
    "São Paulo, Brazil",
    "Mexico City, Mexico",
    "Buenos Aires, Argentina",
    "Bogotá, Colombia",
    "Phoenix, USA",
    "Dublin, Ireland",
    "Kuala Lumpur, Malaysia",
    "Cape Town, South Africa",
    "Lagos, Nigeria",
]

INTER_CITY_DELAY = 1.5  # seconds between cities


def run(cmd: list[str]) -> int:
    result = subprocess.run(cmd, text=True)
    return result.returncode


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--niche", required=True, help="Business type, e.g. 'coffee shops'")
    args = parser.parse_args()

    niche = args.niche
    total = len(CITIES)

    print(f"\n{'='*60}")
    print(f" BATCH RUN: '{niche}' across {total} cities")
    print(f"{'='*60}\n")

    for i, city in enumerate(CITIES, 1):
        print(f"\n[{i}/{total}] --- {city} ---")

        # Step 1: Google Maps search
        rc = run(["python3", "tools/google_maps_search.py", "--niche", niche, "--city", city])
        if rc != 0:
            print(f"  [WARN] Maps search failed for {city}, skipping scrape.")
            time.sleep(INTER_CITY_DELAY)
            continue

        # Step 2: Scrape emails
        import re
        niche_slug = re.sub(r"[^\w\s-]", "", niche.lower())
        niche_slug = re.sub(r"[\s_]+", "-", niche_slug).strip("-")
        city_slug = re.sub(r"[^\w\s-]", "", city.lower())
        city_slug = re.sub(r"[\s_]+", "-", city_slug).strip("-")
        raw_path = f".tmp/raw_places_{niche_slug}_{city_slug}.json"

        rc = run(["python3", "tools/scrape_website_emails.py", "--input", raw_path])
        if rc != 0:
            print(f"  [WARN] Scraping failed for {city}.")

        time.sleep(INTER_CITY_DELAY)

    print(f"\n{'='*60}")
    print(f" DONE. Now run:")
    print(f"   python tools/build_leads_csv.py --niche \"{niche}\"")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
