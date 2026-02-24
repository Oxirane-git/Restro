"""
tools/scrape_website_emails.py

Enriches a raw_places JSON file by scraping each business website for
email addresses and owner/contact names.

Usage:
    python tools/scrape_website_emails.py --input .tmp/raw_places_cafes_new-york-city-usa.json

Output:
    .tmp/enriched_cafes_new-york-city-usa.json
"""

import argparse
import json
import os
import re
import time
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

REQUEST_TIMEOUT = 10
INTER_RECORD_DELAY = 1.2  # seconds between records
INTER_PAGE_DELAY = 0.5    # seconds between subpages within a record

SUBPAGES_TO_TRY = ["/contact", "/contact-us", "/about", "/about-us"]

GENERIC_EMAIL_DOMAINS = {"gmail.com", "yahoo.com", "hotmail.com",
                         "outlook.com", "icloud.com", "mail.com", "aol.com"}

DISCARD_EMAIL_PATTERN = re.compile(
    r"example|domain|youremail|noreply|no-reply|donotreply|"
    r"@wixpress|@squarespace|@wordpress|@cloudflare|"
    r"test@|admin@example|user@",
    re.IGNORECASE
)

DISCARD_EXTENSION_PATTERN = re.compile(
    r"\.(png|jpg|jpeg|gif|svg|webp|ico|bmp|tiff)$", re.IGNORECASE
)

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")

OWNER_PATTERNS = [
    re.compile(
        r"(?:founded|owned|started|run|managed)\s+by\s+"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})",
        re.IGNORECASE,
    ),
    re.compile(r"Owner[:\s\-\u2013]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})"),
    re.compile(r"Proprietor[:\s\-\u2013]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})"),
    re.compile(r"Director[:\s\-\u2013]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})"),
    re.compile(
        r"(?:I'm|I am|My name is)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})",
        re.IGNORECASE,
    ),
    re.compile(r"Meet\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})"),
]

FALSE_POSITIVE_NAMES = {
    "about us", "contact us", "our team", "the owner", "the founder",
    "the director", "learn more", "read more", "find out", "get in touch",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}


def fetch_page(url: str) -> BeautifulSoup | None:
    try:
        resp = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT,
                            allow_redirects=True)
        if resp.status_code == 200:
            return BeautifulSoup(resp.text, "lxml")
        return None
    except Exception:
        return None


def extract_emails(soup: BeautifulSoup, site_domain: str) -> list[str]:
    text = soup.get_text(separator=" ")
    raw = EMAIL_REGEX.findall(text)

    valid = []
    for email in raw:
        email = email.lower().strip()
        if DISCARD_EMAIL_PATTERN.search(email):
            continue
        if DISCARD_EXTENSION_PATTERN.search(email):
            continue
        valid.append(email)

    # Deduplicate
    seen = set()
    deduped = []
    for e in valid:
        if e not in seen:
            seen.add(e)
            deduped.append(e)

    # Sort: exact business domain > other non-generic > generic providers
    def priority(e: str) -> int:
        domain = e.split("@")[-1]
        if domain == site_domain:
            return 0
        if domain not in GENERIC_EMAIL_DOMAINS:
            return 1
        return 2

    deduped.sort(key=priority)
    return deduped


def validate_name(name: str) -> bool:
    if not name:
        return False
    words = name.strip().split()
    if not (2 <= len(words) <= 4):
        return False
    if any(len(w) > 20 for w in words):
        return False
    if any(ch.isdigit() for ch in name):
        return False
    if name.lower() in FALSE_POSITIVE_NAMES:
        return False
    if not all(w[0].isupper() for w in words if w):
        return False
    return True


def extract_owner(soup: BeautifulSoup) -> str:
    # 1. Schema.org Person microdata
    persons = soup.find_all(attrs={"itemtype": re.compile(r"schema\.org/Person", re.IGNORECASE)})
    for person in persons:
        name_tag = person.find(attrs={"itemprop": "name"})
        if name_tag:
            candidate = name_tag.get_text(strip=True)
            if validate_name(candidate):
                return candidate

    # 2. Regex patterns on visible text
    text = re.sub(r"\s+", " ", soup.get_text(separator=" "))
    for pattern in OWNER_PATTERNS:
        match = pattern.search(text)
        if match:
            candidate = match.group(1).strip()
            if validate_name(candidate):
                return candidate

    return ""


def enrich_record(record: dict) -> dict:
    website = record.get("website", "").strip()
    if not website:
        return record

    if not website.startswith(("http://", "https://")):
        website = "https://" + website

    parsed = urlparse(website)
    site_domain = parsed.netloc.lower().replace("www.", "")
    base_url = f"{parsed.scheme}://{parsed.netloc}"

    pages = [website] + [urljoin(base_url, sp) for sp in SUBPAGES_TO_TRY]

    all_emails: list[str] = []
    owner_name = ""

    for i, url in enumerate(pages):
        if i > 0:
            time.sleep(INTER_PAGE_DELAY)

        soup = fetch_page(url)
        if soup is None:
            continue

        for e in extract_emails(soup, site_domain):
            if e not in all_emails:
                all_emails.append(e)

        if not owner_name:
            owner_name = extract_owner(soup)

        # Early exit if we have a business-domain email and a name
        if owner_name and all_emails and all_emails[0].endswith(site_domain):
            break

    record["email"] = all_emails[0] if all_emails else ""
    record["owner_name"] = owner_name
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to raw_places JSON file")
    args = parser.parse_args()

    if not os.path.exists(args.input):
        raise SystemExit(f"[ERROR] Input file not found: {args.input}")

    output_path = args.input.replace("raw_places_", "enriched_")

    if os.path.exists(output_path) and os.path.getsize(output_path) > 10:
        print(f"[SKIP] {output_path} already exists. Delete it to re-run.")
        return

    with open(args.input, "r", encoding="utf-8") as f:
        records = json.load(f)

    print(f"[INFO] Enriching {len(records)} records from {args.input}")
    enriched = []

    for i, record in enumerate(records):
        print(f"  [{i + 1}/{len(records)}] {record.get('business_name', 'unknown')}"
              f" -- {record.get('website', '(no website)')}")
        enriched.append(enrich_record(record))
        if i < len(records) - 1:
            time.sleep(INTER_RECORD_DELAY)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enriched, f, indent=2, ensure_ascii=False)

    email_count = sum(1 for r in enriched if r.get("email"))
    pct = (100 * email_count // len(enriched)) if enriched else 0
    print(f"[DONE] Wrote {len(enriched)} records to {output_path}")
    print(f"[INFO] Email hit rate: {email_count}/{len(enriched)} ({pct}%)")


if __name__ == "__main__":
    main()
