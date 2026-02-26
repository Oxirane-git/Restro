"""
Batch runner: bakeries across global cities.
Runs Maps search + email scrape for each city/niche combo.
"""
import os, re, subprocess, time, sys

NICHE = "bakeries"

CITIES = [
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
    "Sao Paulo, Brazil",
    "Mexico City, Mexico",
    "Buenos Aires, Argentina",
    "Bogota, Colombia",
    "San Francisco, USA",
    "Seattle, USA",
    "Boston, USA",
    "Dublin, Ireland",
    "Edinburgh, UK",
    "Montreal, Canada",
    "Cape Town, South Africa",
    "Nairobi, Kenya",
    "Hong Kong",
    "Manila, Philippines",
    "Delhi, India",
    "Kuala Lumpur, Malaysia",
    "Vienna, Austria",
    "Rome, Italy",
    "Lisbon, Portugal",
]

def slugify(t):
    t = t.lower()
    t = re.sub(r"[^\w\s-]", "", t)
    t = re.sub(r"[\s_]+", "-", t)
    return re.sub(r"-+", "-", t).strip("-")

def run(cmd):
    return subprocess.run(cmd, text=True).returncode

total = len(CITIES)
for i, city in enumerate(CITIES, 1):
    city_slug  = slugify(city)
    niche_slug = slugify(NICHE)
    raw_path     = f".tmp/raw_places_{niche_slug}_{city_slug}.json"
    enriched_path= f".tmp/enriched_{niche_slug}_{city_slug}.json"

    print(f"\n[{i}/{total}] {city}")

    if os.path.exists(raw_path) and os.path.getsize(raw_path) > 10:
        print(f"  [SKIP] Maps already done.")
    else:
        rc = run(["python", "tools/google_maps_search.py", "--niche", NICHE, "--city", city])
        if rc != 0:
            print(f"  [WARN] Maps search failed, skipping.")
            time.sleep(1.5)
            continue
        time.sleep(1.5)

    if os.path.exists(enriched_path) and os.path.getsize(enriched_path) > 10:
        print(f"  [SKIP] Scrape already done.")
    else:
        run(["python", "tools/scrape_website_emails.py", "--input", raw_path])

    time.sleep(1.0)

print("\n[DONE] All cities processed. Now building CSV...")
subprocess.run(["python", "tools/build_leads_csv.py", "--niche", NICHE, "--exclude-leads-dir", "Leads"])
