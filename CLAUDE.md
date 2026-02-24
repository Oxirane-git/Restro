# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## WAT Framework Architecture

This repo uses the **WAT framework** (Workflows, Agents, Tools) — a 3-layer architecture that separates AI reasoning from deterministic execution:

- **`workflows/`** — Markdown SOPs defining objectives, required inputs, which tools to use, expected outputs, and edge case handling. These are your instructions. Do not create or overwrite workflows without asking unless explicitly told to.
- **`tools/`** — Python scripts for deterministic execution (API calls, data transforms, file ops). Check here before writing new code.
- **`.tmp/`** — Temporary/intermediate files. Disposable and regenerable.
- **`.env`** — All API keys and credentials. Never store secrets anywhere else.
- **`credentials.json`, `token.json`** — Google OAuth tokens (gitignored).

## How to Operate

1. **Check `tools/` first** — Only create new scripts when nothing exists for the task.
2. **Read workflows before acting** — When a task maps to an existing workflow (e.g., `workflows/scrape_website.md`), read it and follow it. Don't attempt direct execution when a tool exists for that purpose.
3. **Final outputs go to cloud services** (Google Sheets, Slides, etc.) — not local files. `.tmp/` is for intermediate processing only.

## Error Handling and Self-Improvement

When a tool fails:
1. Read the full error trace
2. Fix the script and retest (check before re-running if the tool uses paid API calls)
3. Document learnings in the relevant workflow (rate limits, batch endpoints, timing quirks)
4. Update the workflow to prevent recurrence

This loop is how the framework improves — every failure should leave the system more robust.
