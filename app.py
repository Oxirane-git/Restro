"""
app.py — Lead Generation Web UI

Wraps the WAT pipeline (google_maps_search → scrape_website_emails → build_leads_csv)
in a simple localhost web interface with real-time log streaming and CSV download.

Usage:
    python app.py
    Open http://localhost:5000
"""

import glob
import os
import queue
import re
import subprocess
import threading
import uuid

from dotenv import load_dotenv
from flask import Flask, Response, jsonify, render_template, request, send_file

load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

app = Flask(__name__)

HIGH_ROI_CITIES = [
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
]

# job_id -> {"queue": Queue, "status": "running"|"done"|"error", "csv": str|None}
jobs: dict[str, dict] = {}


def slugify(text: str) -> str:
    text = text.lower()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_]+", "-", text)
    text = re.sub(r"-+", "-", text)
    return text.strip("-")


def cleanup_intermediates(niche_slug: str) -> tuple[int, int]:
    """Delete raw and enriched JSON files for a niche after CSV is built."""
    patterns = [
        os.path.join(BASE_DIR, ".tmp", f"raw_places_{niche_slug}_*.json"),
        os.path.join(BASE_DIR, ".tmp", f"enriched_{niche_slug}_*.json"),
    ]
    files = [f for pattern in patterns for f in glob.glob(pattern)]
    total_bytes = sum(os.path.getsize(f) for f in files)
    for f in files:
        os.remove(f)
    return len(files), total_bytes


def run_streamed(cmd: list[str], log_fn) -> int:
    """Run a subprocess and feed its output line-by-line to log_fn in real time."""
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,  # merge stderr so nothing is silently dropped
        text=True,
        cwd=BASE_DIR,
        bufsize=1,
    )
    for line in proc.stdout:  # type: ignore[union-attr]
        line = line.rstrip()
        if line:
            log_fn(f"  {line}")
    proc.wait()
    return proc.returncode


def run_pipeline(job_id: str, niche: str, cities: list[str]) -> None:
    import time

    q = jobs[job_id]["queue"]

    def log(msg: str) -> None:
        q.put(msg)

    niche_slug = slugify(niche)
    os.makedirs(os.path.join(BASE_DIR, ".tmp"), exist_ok=True)

    log(f"[START] Niche: {niche} | Cities: {len(cities)}")

    # ── Step 1: Google Maps search ────────────────────────────────────────────
    log("\n── Step 1 / 3 — Google Maps Search ──")
    for city in cities:
        city_slug = slugify(city)
        output_path = os.path.join(BASE_DIR, ".tmp", f"raw_places_{niche_slug}_{city_slug}.json")

        if os.path.exists(output_path) and os.path.getsize(output_path) > 10:
            log(f"[SKIP] {city} (already cached)")
            continue

        log(f"[SEARCH] {city} …")
        run_streamed(
            ["python", os.path.join(BASE_DIR, "tools", "google_maps_search.py"),
             "--niche", niche, "--city", city],
            log,
        )

        time.sleep(1.5)

    # ── Step 2: Scrape emails ─────────────────────────────────────────────────
    raw_files = sorted(glob.glob(os.path.join(BASE_DIR, ".tmp", f"raw_places_{niche_slug}_*.json")))
    log(f"\n── Step 2 / 3 — Website Scraping ({len(raw_files)} files) ──")

    for raw_file in raw_files:
        basename = os.path.basename(raw_file)
        city_slug = basename.replace(f"raw_places_{niche_slug}_", "").replace(".json", "")
        enriched_path = os.path.join(BASE_DIR, ".tmp", f"enriched_{niche_slug}_{city_slug}.json")

        if os.path.exists(enriched_path) and os.path.getsize(enriched_path) > 10:
            log(f"[SKIP] {city_slug} (already enriched)")
            continue

        log(f"[SCRAPE] {city_slug} …")
        run_streamed(
            ["python", os.path.join(BASE_DIR, "tools", "scrape_website_emails.py"),
             "--input", raw_file],
            log,
        )

    # ── Step 3: Build CSV ─────────────────────────────────────────────────────
    log(f"\n── Step 3 / 3 — Building CSV ──")
    run_streamed(
        ["python", os.path.join(BASE_DIR, "tools", "build_leads_csv.py"),
         "--niche", niche],
        log,
    )

    # Find the latest CSV
    csv_files = sorted(
        glob.glob(os.path.join(BASE_DIR, ".tmp", f"leads_{niche_slug}_*.csv")),
        reverse=True,
    )
    if csv_files:
        jobs[job_id]["csv"] = os.path.basename(csv_files[0])
        jobs[job_id]["status"] = "done"
        log(f"\n[DONE] {os.path.basename(csv_files[0])}")
        count, freed = cleanup_intermediates(niche_slug)
        log(f"[CLEAN] Removed {count} intermediate JSON files ({freed // 1024} KB freed)")
    else:
        jobs[job_id]["status"] = "error"
        log("\n[ERROR] No CSV produced — check logs above")

    q.put(None)  # sentinel → end of stream


# ── Routes ────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html", cities=HIGH_ROI_CITIES)


@app.route("/run", methods=["POST"])
def run():
    data = request.get_json(force=True)
    niche = (data.get("niche") or "").strip()
    selected_cities = data.get("cities") or HIGH_ROI_CITIES

    if not niche:
        return jsonify({"error": "Niche is required"}), 400
    if not os.getenv("GOOGLE_MAPS_API_KEY"):
        return jsonify({"error": "GOOGLE_MAPS_API_KEY not set in .env"}), 400

    job_id = str(uuid.uuid4())[:8]
    jobs[job_id] = {"queue": queue.Queue(), "status": "running", "csv": None}

    t = threading.Thread(target=run_pipeline, args=(job_id, niche, selected_cities), daemon=True)
    t.start()

    return jsonify({"job_id": job_id})


@app.route("/stream/<job_id>")
def stream(job_id):
    def generate():
        if job_id not in jobs:
            yield "data: [ERROR] Job not found\n\n"
            return

        q = jobs[job_id]["queue"]
        while True:
            msg = q.get()
            if msg is None:
                job = jobs[job_id]
                if job["status"] == "done":
                    yield f"data: __DONE__:{job['csv']}\n\n"
                else:
                    yield "data: __ERROR__\n\n"
                break
            # Escape any bare newlines inside the message so SSE stays valid
            safe = msg.replace("\n", " | ")
            yield f"data: {safe}\n\n"

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


@app.route("/download/<filename>")
def download(filename):
    # Prevent path traversal
    filename = os.path.basename(filename)
    filepath = os.path.join(BASE_DIR, ".tmp", filename)
    if not os.path.exists(filepath):
        return "File not found", 404
    return send_file(filepath, as_attachment=True, download_name=filename)


@app.route("/csvs")
def list_csvs():
    files = sorted(glob.glob(os.path.join(BASE_DIR, ".tmp", "leads_*.csv")), reverse=True)
    return jsonify([os.path.basename(f) for f in files])


if __name__ == "__main__":
    os.makedirs(os.path.join(BASE_DIR, ".tmp"), exist_ok=True)
    print("Lead Generator running at http://localhost:5000")
    app.run(host="127.0.0.1", port=5000, debug=False)
