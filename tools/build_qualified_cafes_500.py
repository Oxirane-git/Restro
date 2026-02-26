"""
tools/build_qualified_cafes_500.py

Consolidates all existing cafe-related leads from the Leads/ folder into a
single, deduplicated, qualified CSV with exactly 500 records (all having emails).

If fewer than 500 unique email leads exist, runs the Google Maps + scraping
pipeline for additional cities/niches to fill the gap.

Usage:
    python tools/build_qualified_cafes_500.py

Output:
    Leads/unique_emails_cafes_qualified_500_<timestamp>.csv
"""

import argparse
import csv
import glob
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime

# ── Config ────────────────────────────────────────────────────────────────────

LEADS_DIR = "Leads"
TARGET_COUNT = 500

CAFE_NICHES = ["cafes", "coffee shops", "espresso bars"]

# Cities not yet in the existing leads (expansion pool)
EXPANSION_CITIES = [
    "San Francisco, USA",
    "Boston, USA",
    "Seattle, USA",
    "Montreal, Canada",
    "Calgary, Canada",
    "Brisbane, Australia",
    "Perth, Australia",
    "Edinburgh, UK",
    "Glasgow, UK",
    "Dublin, Ireland",
    "Delhi, India",
    "Bangalore, India",
    "Hong Kong",
    "Manila, Philippines",
    "Nairobi, Kenya",
    "Cape Town, South Africa",
    "Johannesburg, South Africa",
    "Abu Dhabi, UAE",
    "Kuala Lumpur, Malaysia",
    "Wellington, New Zealand",
    "Porto, Portugal",
    "Lisbon, Portugal",
    "Rome, Italy",
    "Milan, Italy",
    "Vienna, Austria",
    "Copenhagen, Denmark",
    "Stockholm, Sweden",
    "Oslo, Norway",
    "Helsinki, Finland",
    "Warsaw, Poland",
]

CSV_FIELDNAMES = [
    "business_name",
    "address",
    "owner_name",
    "email",
    "phone",
    "website",
    "category",
    "city",
    "source",
]

GENERIC_TYPES = {
    "point_of_interest", "establishment", "food", "store", "health",
    "gym", "local_business", "premise", "route", "street_address",
    "locality", "political", "geocode",
}


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def pick_category(types, niche: str) -> str:
    specific = [t for t in types if t not in GENERIC_TYPES]
    if specific:
        niche_words = set(niche.lower().split())
        for t in specific:
            if any(word in t.lower() for word in niche_words):
                return t.replace("_", " ")
        return specific[0].replace("_", " ")
    return niche


def load_all_cafe_leads_from_folder(leads_dir: str) -> dict:
    """
    Reads all cafe-related CSVs from the Leads folder.
    Returns a dict keyed by lowercase email (only records with emails).
    Priority: first occurrence wins (keeps later files from overwriting earlier ones).
    """
    all_records = {}
    cafe_keywords = ("cafe", "coffee", "espresso")
    
    for f in sorted(os.listdir(leads_dir)):
        if not f.endswith(".csv"):
            continue
        if not any(kw in f.lower() for kw in cafe_keywords):
            continue
        filepath = os.path.join(leads_dir, f)
        try:
            with open(filepath, "r", encoding="utf-8") as fp:
                for row in csv.DictReader(fp):
                    email = row.get("email", "").strip().lower()
                    if email and email not in all_records:
                        all_records[email] = {
                            "business_name": row.get("business_name", "").strip(),
                            "address": row.get("address", "").strip(),
                            "owner_name": row.get("owner_name", "").strip(),
                            "email": email,
                            "phone": row.get("phone", "").strip(),
                            "website": row.get("website", "").strip(),
                            "category": row.get("category", "cafe").strip(),
                            "city": row.get("city", "").strip(),
                            "source": row.get("source", "google_maps").strip(),
                        }
        except Exception as e:
            print(f"  [WARN] Failed to read {f}: {e}")

    return all_records


def run_maps_search(niche: str, city: str) -> int:
    """Run google_maps_search.py for a niche/city pair."""
    cmd = ["python", "tools/google_maps_search.py", "--niche", niche, "--city", city]
    result = subprocess.run(cmd, text=True)
    return result.returncode


def run_scrape(raw_path: str) -> int:
    """Run scrape_website_emails.py on a raw JSON file."""
    cmd = ["python", "tools/scrape_website_emails.py", "--input", raw_path]
    result = subprocess.run(cmd, text=True)
    return result.returncode


def load_enriched_json(niche_slug: str) -> list:
    """Load all enriched JSON files for a niche slug."""
    pattern = f".tmp/enriched_{niche_slug}_*.json"
    records = []
    for filepath in sorted(glob.glob(pattern)):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
            records.extend(data)
        except Exception as e:
            print(f"  [WARN] Failed to load {filepath}: {e}")
    return records


def scrape_more_cafes(existing_emails: set, target: int) -> list:
    """
    Runs Maps search + email scraping for expansion cities until we
    have enough unique cafe emails to reach `target`.
    Returns list of new qualifying records.
    """
    new_records = {}
    os.makedirs(".tmp", exist_ok=True)

    for city in EXPANSION_CITIES:
        if len(existing_emails) + len(new_records) >= target:
            print(f"\n[INFO] Target of {target} reached. Stopping search.")
            break

        city_slug = slugify(city)

        for niche in CAFE_NICHES:
            if len(existing_emails) + len(new_records) >= target:
                break

            niche_slug = slugify(niche)
            raw_path = f".tmp/raw_places_{niche_slug}_{city_slug}.json"
            enriched_path = f".tmp/enriched_{niche_slug}_{city_slug}.json"

            print(f"\n[SEARCH] '{niche}' in '{city}' …")

            # Step 1: Maps search (skip if already done)
            if os.path.exists(raw_path) and os.path.getsize(raw_path) > 10:
                print(f"  [SKIP] Raw file exists.")
            else:
                rc = run_maps_search(niche, city)
                if rc != 0:
                    print(f"  [WARN] Maps search failed. Skipping.")
                    time.sleep(1.5)
                    continue
                time.sleep(1.5)

            # Step 2: Scrape emails (skip if already done)
            if os.path.exists(enriched_path) and os.path.getsize(enriched_path) > 10:
                print(f"  [SKIP] Enriched file exists.")
            else:
                rc = run_scrape(raw_path)
                if rc != 0:
                    print(f"  [WARN] Scraping failed.")

            # Step 3: Pull new unique emails
            if os.path.exists(enriched_path):
                try:
                    with open(enriched_path, "r", encoding="utf-8") as f:
                        records = json.load(f)
                    before = len(new_records)
                    for r in records:
                        email = r.get("email", "").strip().lower()
                        if (
                            email
                            and email not in existing_emails
                            and email not in new_records
                        ):
                            new_records[email] = {
                                "business_name": r.get("business_name", "").strip(),
                                "address": r.get("address", "").strip(),
                                "owner_name": r.get("owner_name", "").strip(),
                                "email": email,
                                "phone": r.get("phone", "").strip(),
                                "website": r.get("website", "").strip(),
                                "category": pick_category(r.get("types", []), niche),
                                "city": r.get("city", city).strip(),
                                "source": "google_maps",
                            }
                    added = len(new_records) - before
                    print(f"  [INFO] +{added} new emails (total new: {len(new_records)}, existing: {len(existing_emails)})")
                except Exception as e:
                    print(f"  [WARN] Could not parse {enriched_path}: {e}")

            time.sleep(1.5)

    return list(new_records.values())


def main():
    print("\n" + "=" * 60)
    print(f" CAFE LEADS CONSOLIDATION -> Target: {TARGET_COUNT} qualified leads")
    print("=" * 60)

    # ── Step 1: Load existing qualified cafe leads ────────────────────────────
    print(f"\n[STEP 1] Loading existing cafe leads from '{LEADS_DIR}/'…")
    existing = load_all_cafe_leads_from_folder(LEADS_DIR)
    print(f"  Found {len(existing)} unique cafe leads with email.")

    # ── Step 2: Check if we need more ────────────────────────────────────────
    needed = max(0, TARGET_COUNT - len(existing))
    print(f"\n[STEP 2] Need {needed} more leads to reach {TARGET_COUNT}.")

    new_leads = []
    if needed > 0:
        print(f"  Running additional city scraping…")
        new_leads = scrape_more_cafes(set(existing.keys()), TARGET_COUNT)
        print(f"\n  [INFO] Scraped {len(new_leads)} additional unique leads.")
    else:
        print(f"  Already have enough! Selecting best {TARGET_COUNT} from {len(existing)}.")

    # ── Step 3: Merge and take top 500 ───────────────────────────────────────
    all_records = list(existing.values()) + new_leads

    # Sort priority: has phone > has owner_name > just email
    def quality_score(r):
        score = 0
        if r.get("phone"): score += 2
        if r.get("owner_name"): score += 3
        if r.get("website"): score += 1
        return -score  # negative so higher is better

    all_records.sort(key=quality_score)
    final = all_records[:TARGET_COUNT]

    # ── Step 4: Write output ──────────────────────────────────────────────────
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(LEADS_DIR, f"unique_emails_cafes_qualified_500_{timestamp}.csv")

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for record in final:
            writer.writerow(record)

    # ── Step 5: Stats ─────────────────────────────────────────────────────────
    total = len(final)
    with_phone  = sum(1 for r in final if r.get("phone"))
    with_owner  = sum(1 for r in final if r.get("owner_name"))
    with_website= sum(1 for r in final if r.get("website"))
    pct = lambda n: f"{100 * n // total}%" if total else "0%"

    print(f"\n{'=' * 60}")
    print(f" OUTPUT: {output_path}")
    print(f"{'=' * 60}")
    print(f"  Total leads  : {total}")
    print(f"  All with email: 100% (qualification filter)")
    print(f"  With phone   : {with_phone} ({pct(with_phone)})")
    print(f"  With owner   : {with_owner} ({pct(with_owner)})")
    print(f"  With website : {with_website} ({pct(with_website)})")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
