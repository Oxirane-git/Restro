# Lead Generation Workflow

## Objective
Generate a CSV of 1000+ business leads for a given niche/business type, globally
distributed across high-ROI cities, enriched with emails and contact names scraped
from business websites.

## Required Inputs
- `NICHE`: Business type string, e.g. `cafes`, `gyms`, `dentists`, `yoga studios`
- `GOOGLE_MAPS_API_KEY`: Must be set in `.env`
- Optional: Run only for a single city when testing with `--city` flag

## Tools Used (in order)
1. `tools/google_maps_search.py`
2. `tools/scrape_website_emails.py`
3. `tools/build_leads_csv.py`

---

## Step-by-Step Process

### Step 1 — Validate Inputs
- Confirm NICHE is a non-empty string
- Confirm `.env` exists and `GOOGLE_MAPS_API_KEY` is set and non-empty
- If either is missing, stop and ask the user before proceeding

### Step 2 — Run Google Maps Search Per City
For each city in the HIGH_ROI_CITIES list below, run:
```
python tools/google_maps_search.py --niche "<NICHE>" --city "<CITY>"
```

- Output: `.tmp/raw_places_<niche_slug>_<city_slug>.json`
- If the output file already exists and is non-empty, **skip that city** (resume support)
- Wait 1.5 seconds between city runs to avoid quota bursts
- If a city returns 0 results, log it and continue — do not abort
- After all cities complete, count total unique records collected

### Step 3 — Enrich With Website Scraping
For each `.tmp/raw_places_<niche_slug>_*.json` file, run:
```
python tools/scrape_website_emails.py --input .tmp/raw_places_<niche_slug>_<city_slug>.json
```

- Output: `.tmp/enriched_<niche_slug>_<city_slug>.json`
- If the enriched file already exists and is non-empty, **skip** (resume support)
- Expect ~40–60% of records to yield an email — this is normal
- Do not abort the entire batch for one failed URL

### Step 4 — Build Final CSV
```
python tools/build_leads_csv.py --niche "<NICHE>"
```

- Reads all `.tmp/enriched_<niche_slug>_*.json` files
- Deduplicates and writes `.tmp/leads_<niche_slug>_<timestamp>.csv`

### Step 5 — Verify and Report
- Count rows in the CSV
- Report: total leads, email hit rate (%), cities covered, cities with 0 results
- If total leads < 100, see Edge Cases below

---

## HIGH_ROI_CITIES (25 Cities)

### North America (8)
- New York City, USA
- Los Angeles, USA
- Chicago, USA
- Toronto, Canada
- Miami, USA
- Houston, USA
- Vancouver, Canada
- Atlanta, USA

### Europe / MENA (7)
- London, UK
- Berlin, Germany
- Amsterdam, Netherlands
- Paris, France
- Barcelona, Spain
- Dubai, UAE
- Manchester, UK

### Asia-Pacific (6)
- Sydney, Australia
- Melbourne, Australia
- Singapore
- Tokyo, Japan
- Mumbai, India
- Auckland, New Zealand

### Latin America (4)
- São Paulo, Brazil
- Mexico City, Mexico
- Buenos Aires, Argentina
- Bogotá, Colombia

**Expected yield:** 25 cities × ~40 avg results = ~1,000 raw records. With ~10% dedup overlap → ~900–950 unique leads.

---

## Edge Cases

### Fewer Than 1000 Leads After Full Run
Run niche synonyms (e.g., for "cafes" also run "coffee shops", "espresso bars"), then
re-run `build_leads_csv.py`. Or add cities from this expansion list:
Phoenix USA, Dublin Ireland, Kuala Lumpur Malaysia, Cape Town South Africa, Lagos Nigeria.

### API Quota Exceeded (429 / OVER_QUERY_LIMIT)
The tool waits 60s and retries once automatically. If the retry also fails, that city
is skipped and logged. Total API calls for a full run: 25 cities × 3 pages = 75 calls —
well within the 5,000/day default limit.

### Website Scraping Blocked (403 / 429 / Timeout)
Record is saved with `email: ""` and `owner_name: ""`. This is expected behavior —
do not retry and do not abort. The ~40–60% hit rate assumption accounts for this.

### Business Has No Website in Google Maps
Skip website scraping for that record. The business name, address, phone, and city
from Google Maps are still valid lead data.

### Duplicate Businesses Across Cities
Handled automatically in `build_leads_csv.py`:
- Primary dedup key: `place_id` (exact Google Maps ID match)
- Secondary dedup key: non-empty `email` (if two records share an email, keep first)

---

## Output Format

Final CSV columns:
```
business_name | address | owner_name | email | phone | website | category | city | source
```

- `category` — most specific Google Maps type, or the niche input string as fallback
- `city` — which city this lead was sourced from
- `source` — always `google_maps`

File location: `.tmp/leads_<niche_slug>_<YYYYMMDD_HHMMSS>.csv`
