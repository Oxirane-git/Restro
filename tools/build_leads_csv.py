"""
tools/build_leads_csv.py

Merges all enriched JSON files for a niche, deduplicates, and writes a final CSV.

Usage:
    python tools/build_leads_csv.py --niche "cafes"

Reads:  .tmp/enriched_cafes_*.json
Output: .tmp/leads_cafes_<YYYYMMDD_HHMMSS>.csv
"""

import argparse
import csv
import glob
import json
import os
import re
from datetime import datetime

GENERIC_TYPES = {
    "point_of_interest", "establishment", "food", "store", "health",
    "gym", "local_business", "premise", "route", "street_address",
    "locality", "political", "geocode",
}

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


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def pick_category(types: list, niche: str) -> str:
    specific = [t for t in types if t not in GENERIC_TYPES]
    if specific:
        niche_words = set(niche.lower().split())
        for t in specific:
            if any(word in t.lower() for word in niche_words):
                return t.replace("_", " ")
        return specific[0].replace("_", " ")
    return niche


def load_enriched_files(niche_slug: str) -> list[dict]:
    pattern = f".tmp/enriched_{niche_slug}_*.json"
    files = sorted(glob.glob(pattern))
    if not files:
        raise SystemExit(
            f"[ERROR] No enriched files found matching: {pattern}\n"
            "Run scrape_website_emails.py for each city first."
        )

    all_records = []
    for filepath in files:
        with open(filepath, "r", encoding="utf-8") as f:
            records = json.load(f)
        print(f"  [INFO] Loaded {len(records)} records from {filepath}")
        all_records.extend(records)

    return all_records


def deduplicate(records: list[dict]) -> list[dict]:
    seen_place_ids: set[str] = set()
    seen_emails: set[str] = set()
    unique = []

    for record in records:
        place_id = record.get("place_id", "")
        email = record.get("email", "").strip().lower()

        if place_id and place_id in seen_place_ids:
            continue
        if email and email in seen_emails:
            continue

        if place_id:
            seen_place_ids.add(place_id)
        if email:
            seen_emails.add(email)

        unique.append(record)

    return unique


def normalize(record: dict, niche: str) -> dict:
    return {
        "business_name": record.get("business_name", "").strip(),
        "address": record.get("address", "").strip(),
        "owner_name": record.get("owner_name", "").strip(),
        "email": record.get("email", "").strip().lower(),
        "phone": record.get("phone", "").strip(),
        "website": record.get("website", "").strip(),
        "category": pick_category(record.get("types", []), niche),
        "city": record.get("city", "").strip(),
        "source": "google_maps",
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--niche", required=True, help="Business type used in the search")
    args = parser.parse_args()

    niche_slug = slugify(args.niche)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f".tmp/leads_{niche_slug}_{timestamp}.csv"

    print(f"[INFO] Loading enriched files for niche: '{args.niche}'")
    records = load_enriched_files(niche_slug)
    print(f"[INFO] Total before dedup: {len(records)}")

    records = deduplicate(records)
    print(f"[INFO] Total after dedup:  {len(records)}")

    if len(records) < 1000:
        print(
            f"[WARN] Only {len(records)} unique leads. "
            "Consider adding more cities or niche synonyms (see workflow edge cases)."
        )

    os.makedirs(".tmp", exist_ok=True)
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_FIELDNAMES)
        writer.writeheader()
        for record in records:
            writer.writerow(normalize(record, args.niche))

    total = len(records)
    email_n = sum(1 for r in records if r.get("email"))
    owner_n = sum(1 for r in records if r.get("owner_name"))
    phone_n = sum(1 for r in records if r.get("phone"))
    web_n   = sum(1 for r in records if r.get("website"))
    pct     = lambda n: f"{100 * n // total}%" if total else "0%"

    print(f"\n[DONE] CSV written to: {output_path}")
    print(f"  Total leads  : {total}")
    print(f"  With email   : {email_n} ({pct(email_n)})")
    print(f"  With owner   : {owner_n} ({pct(owner_n)})")
    print(f"  With phone   : {phone_n} ({pct(phone_n)})")
    print(f"  With website : {web_n} ({pct(web_n)})")


if __name__ == "__main__":
    main()
